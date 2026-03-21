import { prisma } from "../db/prisma.js";
import { DISCOVERY_DEFAULTS, VALIDATION_DEFAULTS } from "../config/tuning.js";

type FeatureBinDef = {
  kind: "quantile" | "fixed";
  labels: string[];
  edges: number[];
};

type FeatureExtractor = (row: GameWithContextAndOdds) => number | null;

type GameWithContextAndOdds = Awaited<ReturnType<typeof loadGamesForFeatures>>[number];

type SplitStats = {
  n: number;
  wins: number;
  rawHitRate: number;
  posteriorHitRate: number;
  edge: number;
  lift: number;
};

type CandidatePattern = {
  outcomeType: string;
  conditions: string[];
  discoverySource: "tree" | "fpgrowth";
};

type TokenizedGame = {
  gameId: string;
  season: number;
  date: Date;
  tokens: Set<string>;
  outcomes: Set<string>;
};

type ScoredPattern = CandidatePattern & {
  train: SplitStats;
  val: SplitStats;
  forward: SplitStats;
  rawHitRate: number;
  posteriorHitRate: number;
  lift: number;
  edge: number;
  score: number;
  n: number;
  trainCoverage: number;
};

type FamilyBucket = "PLAYER" | "TOTAL" | "SPREAD" | "MONEYLINE" | "OTHER";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--") continue; // bare separator, skip
    if (arg.startsWith("--") && i + 1 < args.length) {
      const value = args[i + 1];
      if (!value.startsWith("--")) {
        flags[arg.slice(2)] = value;
        i++;
      }
    }
  }
  return flags;
}

function americanToDecimal(american: number): number {
  if (american > 0) return 1 + american / 100;
  if (american < 0) return 1 + 100 / Math.abs(american);
  return 2;
}

function q(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * p;
  const low = Math.floor(idx);
  const high = Math.ceil(idx);
  if (low === high) return sorted[low];
  const w = idx - low;
  return sorted[low] * (1 - w) + sorted[high] * w;
}

function quantileBinDef(values: number[], qCount: number): FeatureBinDef | null {
  if (values.length < Math.max(20, qCount * 5)) return null;
  const edges: number[] = [];
  for (let i = 1; i < qCount; i++) {
    edges.push(q(values, i / qCount));
  }
  const deduped = [...new Set(edges.map((v) => Number(v.toFixed(6))))].sort((a, b) => a - b);
  if (deduped.length < 1) return null;
  const labels = Array.from({ length: deduped.length + 1 }, (_, i) => `Q${i + 1}`);
  return { kind: "quantile", labels, edges: deduped };
}

function fixedBinDef(labels: string[], edges: number[]): FeatureBinDef {
  return { kind: "fixed", labels, edges };
}

function bucketValue(value: number, def: FeatureBinDef): string {
  for (let i = 0; i < def.edges.length; i++) {
    if (value <= def.edges[i]) {
      return def.labels[i] ?? `B${i + 1}`;
    }
  }
  return def.labels[def.labels.length - 1] ?? `B${def.labels.length}`;
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function toPgTextArrayLiteral(values: string[]): string {
  const escaped = values.map((v) => `"${v.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`);
  return `{${escaped.join(",")}}`;
}

function getCurrentSeason(): number {
  const now = new Date();
  return now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
}

async function loadGamesForFeatures(fromSeason: number, toSeason: number) {
  return prisma.game.findMany({
    where: {
      season: { gte: fromSeason, lte: toSeason },
    },
    include: {
      context: true,
      odds: true,
      playerContexts: {
        select: {
          teamId: true,
          ppg: true,
          apg: true,
        },
      },
    },
  });
}

function getConsensusOdds(row: GameWithContextAndOdds) {
  return row.odds.find((o) => o.source === "consensus") ?? row.odds[0] ?? null;
}

function featureExtractors(): Record<string, FeatureExtractor> {
  const top3Average = (
    r: GameWithContextAndOdds,
    teamId: number,
    key: "ppg" | "apg",
  ): number | null => {
    const vals = r.playerContexts
      .filter((pc) => pc.teamId === teamId)
      .map((pc) => pc[key])
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => b - a)
      .slice(0, 3);
    if (vals.length === 0) return null;
    return vals.reduce((sum, v) => sum + v, 0) / vals.length;
  };

  return {
    home_ppg: (r) => r.context?.homePpg ?? null,
    away_ppg: (r) => r.context?.awayPpg ?? null,
    home_oppg: (r) => r.context?.homeOppg ?? null,
    away_oppg: (r) => r.context?.awayOppg ?? null,
    home_rank_off: (r) => r.context?.homeRankOff ?? null,
    away_rank_off: (r) => r.context?.awayRankOff ?? null,
    home_rank_def: (r) => r.context?.homeRankDef ?? null,
    away_rank_def: (r) => r.context?.awayRankDef ?? null,
    home_rest_days: (r) => r.context?.homeRestDays ?? null,
    away_rest_days: (r) => r.context?.awayRestDays ?? null,
    home_streak: (r) => r.context?.homeStreak ?? null,
    away_streak: (r) => r.context?.awayStreak ?? null,
    home_injury_out: (r) => r.context?.homeInjuryOutCount ?? null,
    away_injury_out: (r) => r.context?.awayInjuryOutCount ?? null,
    injury_out_delta: (r) =>
      r.context ? (r.context.homeInjuryOutCount ?? 0) - (r.context.awayInjuryOutCount ?? 0) : null,
    home_injury_questionable: (r) => r.context?.homeInjuryQuestionableCount ?? null,
    away_injury_questionable: (r) => r.context?.awayInjuryQuestionableCount ?? null,
    injury_questionable_delta: (r) =>
      r.context
        ? (r.context.homeInjuryQuestionableCount ?? 0) - (r.context.awayInjuryQuestionableCount ?? 0)
        : null,
    home_lineup_certainty: (r) => r.context?.homeLineupCertainty ?? null,
    away_lineup_certainty: (r) => r.context?.awayLineupCertainty ?? null,
    lineup_certainty_delta: (r) =>
      r.context ? (r.context.homeLineupCertainty ?? 0) - (r.context.awayLineupCertainty ?? 0) : null,
    home_late_scratch_risk: (r) => r.context?.homeLateScratchRisk ?? null,
    away_late_scratch_risk: (r) => r.context?.awayLateScratchRisk ?? null,
    late_scratch_risk_delta: (r) =>
      r.context ? (r.context.homeLateScratchRisk ?? 0) - (r.context.awayLateScratchRisk ?? 0) : null,
    home_net_rating: (r) => (r.context ? r.context.homePpg - r.context.homeOppg : null),
    away_net_rating: (r) => (r.context ? r.context.awayPpg - r.context.awayOppg : null),
    spread_home: (r) => getConsensusOdds(r)?.spreadHome ?? null,
    spread_abs: (r) => {
      const s = getConsensusOdds(r)?.spreadHome;
      return s == null ? null : Math.abs(s);
    },
    total_line: (r) => getConsensusOdds(r)?.totalOver ?? null,
    ml_home: (r) => getConsensusOdds(r)?.mlHome ?? null,
    ml_away: (r) => getConsensusOdds(r)?.mlAway ?? null,
    home_creation_burden: (r) => {
      const topScorer = top3Average(r, r.homeTeamId, "ppg");
      const ast = r.context?.homeAstPg ?? top3Average(r, r.homeTeamId, "apg");
      if (topScorer == null || ast == null || ast <= 0) return null;
      return topScorer / ast;
    },
    away_creation_burden: (r) => {
      const topScorer = top3Average(r, r.awayTeamId, "ppg");
      const ast = r.context?.awayAstPg ?? top3Average(r, r.awayTeamId, "apg");
      if (topScorer == null || ast == null || ast <= 0) return null;
      return topScorer / ast;
    },
    home_playmaking_resilience: (r) => {
      const ast = r.context?.homeAstPg ?? top3Average(r, r.homeTeamId, "apg");
      const oppDefRank = r.context?.awayRankDef;
      if (ast == null || ast <= 0) return null;
      const pressure = oppDefRank != null ? (31 - oppDefRank) / 30 : 0.5;
      return ast / (1 + pressure * 0.6);
    },
    away_playmaking_resilience: (r) => {
      const ast = r.context?.awayAstPg ?? top3Average(r, r.awayTeamId, "apg");
      const oppDefRank = r.context?.homeRankDef;
      if (ast == null || ast <= 0) return null;
      const pressure = oppDefRank != null ? (31 - oppDefRank) / 30 : 0.5;
      return ast / (1 + pressure * 0.6);
    },
    home_role_dependency: (r) => {
      const players = r.playerContexts
        .filter((pc) => pc.teamId === r.homeTeamId && Number.isFinite(pc.ppg))
        .map((pc) => pc.ppg)
        .sort((a, b) => b - a)
        .slice(0, 3);
      if (players.length < 2) return null;
      const denom = players.reduce((sum, v) => sum + v, 0);
      if (denom <= 0) return null;
      return players[0] / denom;
    },
    away_role_dependency: (r) => {
      const players = r.playerContexts
        .filter((pc) => pc.teamId === r.awayTeamId && Number.isFinite(pc.ppg))
        .map((pc) => pc.ppg)
        .sort((a, b) => b - a)
        .slice(0, 3);
      if (players.length < 2) return null;
      const denom = players.reduce((sum, v) => sum + v, 0);
      if (denom <= 0) return null;
      return players[0] / denom;
    },
  };
}

export async function buildFeatureBins(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const quantiles = Number(flags.quantiles ?? 5);
  const fromSeason = Number(flags["from-season"] ?? 2023);
  const toSeason = Number(flags["to-season"] ?? getCurrentSeason());
  const seasonRange = `${fromSeason}-${toSeason}`;

  console.log(`\n=== Build Feature Bins (${seasonRange}) ===\n`);
  const rows = await loadGamesForFeatures(fromSeason, toSeason);
  if (rows.length === 0) {
    console.log("No games found for requested season range.");
    return;
  }

  const extractors = featureExtractors();
  const fixedDefs: Record<string, FeatureBinDef> = {
    spread_abs: fixedBinDef(["LT_3", "RANGE_3_7", "RANGE_7_10", "GT_10"], [3, 7, 10]),
    spread_home: fixedBinDef(["LE_-10", "-10_TO_-3", "-3_TO_0", "0_TO_3", "3_TO_10", "GE_10"], [-10, -3, 0, 3, 10]),
    total_line: fixedBinDef(["LT_210", "RANGE_210_220", "RANGE_220_230", "RANGE_230_240", "GE_240"], [210, 220, 230, 240]),
    home_role_dependency: fixedBinDef(["LOW", "MODERATE", "HIGH", "EXTREME"], [0.33, 0.4, 0.5]),
    away_role_dependency: fixedBinDef(["LOW", "MODERATE", "HIGH", "EXTREME"], [0.33, 0.4, 0.5]),
  };

  const bins: Array<{ featureName: string; def: FeatureBinDef; method: string }> = [];
  for (const [featureName, extractor] of Object.entries(extractors)) {
    const values = rows
      .map(extractor)
      .filter((v): v is number => v != null && Number.isFinite(v));
    if (values.length === 0) continue;

    if (fixedDefs[featureName]) {
      bins.push({ featureName, def: fixedDefs[featureName], method: "fixed" });
      continue;
    }

    const qDef = quantileBinDef(values, quantiles);
    if (qDef) {
      bins.push({ featureName, def: qDef, method: "quantile" });
    }
  }

  await prisma.$executeRawUnsafe(`DELETE FROM "FeatureBin"`);
  for (const row of bins) {
    const binJson = sqlEsc(JSON.stringify(row.def));
    await prisma.$executeRawUnsafe(
      `INSERT INTO "FeatureBin" ("id","featureName","binEdges","method","seasonRange","createdAt")
       VALUES ('${crypto.randomUUID()}','${sqlEsc(row.featureName)}','${binJson}'::jsonb,'${row.method}','${seasonRange}',NOW())`,
    );
  }

  console.log(`Stored ${bins.length} feature bin definitions.`);
}

type FeatureBinRow = {
  featureName: string;
  binEdges: FeatureBinDef;
};

async function loadLatestFeatureBins(): Promise<Map<string, FeatureBinDef>> {
  const rows = await prisma.$queryRawUnsafe<Array<{ featureName: string; binEdges: unknown }>>(
    `SELECT DISTINCT ON ("featureName") "featureName", "binEdges"
     FROM "FeatureBin"
     ORDER BY "featureName", "createdAt" DESC`,
  );
  const out = new Map<string, FeatureBinDef>();
  for (const row of rows) {
    const parsed = row.binEdges as FeatureBinDef;
    if (!parsed || !Array.isArray(parsed.labels) || !Array.isArray(parsed.edges)) continue;
    out.set(row.featureName, parsed);
  }
  return out;
}

export async function buildQuantizedGameFeatures(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const fromSeason = Number(flags["from-season"] ?? 2023);
  const toSeason = Number(flags["to-season"] ?? getCurrentSeason() + 1);

  console.log(`\n=== Build Quantized Game Features (${fromSeason}-${toSeason}) ===\n`);
  const bins = await loadLatestFeatureBins();
  if (bins.size === 0) {
    throw new Error("No feature bins found. Run build:feature-bins first.");
  }

  const rows = await loadGamesForFeatures(fromSeason, toSeason);
  const extractors = featureExtractors();

  const BATCH_SIZE = 500;
  let upserts = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const values = batch
      .map((row) => {
        const tokens: string[] = [];
        for (const [featureName, def] of bins.entries()) {
          const extractor = extractors[featureName];
          if (!extractor) continue;
          const value = extractor(row);
          if (value == null || !Number.isFinite(value)) continue;
          const bucket = bucketValue(value, def);
          tokens.push(`${featureName}:${bucket}`);
        }
        tokens.push(`season:${row.season}`);
        if (row.context?.homeIsB2b) tokens.push("home_is_b2b:true");
        if (row.context?.awayIsB2b) tokens.push("away_is_b2b:true");
        const arrayLiteral = toPgTextArrayLiteral([...new Set(tokens)].sort());
        return `('${crypto.randomUUID()}','${sqlEsc(row.id)}',${row.season},'${row.date.toISOString().slice(0, 10)}','${sqlEsc(arrayLiteral)}',NOW())`;
      })
      .filter((v): v is string => v != null);
    if (values.length === 0) continue;
    await prisma.$executeRawUnsafe(
      `INSERT INTO "GameFeatureToken" ("id","gameId","season","date","tokens","createdAt") VALUES ${values.join(",")}
       ON CONFLICT ("gameId") DO UPDATE SET "season" = EXCLUDED."season", "date" = EXCLUDED."date", "tokens" = EXCLUDED."tokens"`,
    );
    upserts += values.length;
  }

  console.log(`Upserted ${upserts} game token rows.`);
}

function matchesConditions(tokens: Set<string>, conditions: string[]): boolean {
  for (const c of conditions) {
    if (c.startsWith("!")) {
      if (tokens.has(c.slice(1))) return false;
    } else if (!tokens.has(c)) {
      return false;
    }
  }
  return true;
}

function isLowSpecificityConditionToken(token: string): boolean {
  return (
    token.startsWith("home_rest_days:") ||
    token.startsWith("away_rest_days:") ||
    token.startsWith("season:") ||
    token === "home_is_b2b:true" ||
    token === "away_is_b2b:true"
  );
}

function conditionOverlap(a: string[], b: string[]): number {
  const aSet = new Set(a);
  const bSet = new Set(b);
  if (aSet.size === 0 || bSet.size === 0) return 0;
  let intersection = 0;
  for (const v of aSet) {
    if (bSet.has(v)) intersection++;
  }
  // Overlap coefficient (intersection over smaller set) catches near-superset duplicates.
  return intersection / Math.max(1, Math.min(aSet.size, bSet.size));
}

function conditionFeatureKey(token: string): string {
  const base = token.startsWith("!") ? token.slice(1) : token;
  const idx = base.indexOf(":");
  return idx >= 0 ? base.slice(0, idx) : base;
}

function distinctConditionFeatureCount(conditions: string[]): number {
  const keys = new Set<string>();
  for (const c of conditions) {
    const key = conditionFeatureKey(c);
    if (key) keys.add(key);
  }
  return keys.size;
}

function outcomeFamily(outcomeType: string): string {
  const base = outcomeType.replace(/:.*$/, "");
  if (base.startsWith("PLAYER_")) return "PLAYER";
  if (base.endsWith("_WIN") || base === "HOME_WIN" || base === "AWAY_WIN") return "MONEYLINE";
  if (base.includes("COVERED")) return "SPREAD";
  if (base.startsWith("TOTAL_") || base.startsWith("OVER_") || base.startsWith("UNDER_")) return "TOTAL";
  return base.split("_").slice(0, 2).join("_");
}

function outcomeFamilyBucket(outcomeType: string): FamilyBucket {
  const family = outcomeFamily(outcomeType);
  if (
    family === "PLAYER" ||
    family === "TOTAL" ||
    family === "SPREAD" ||
    family === "MONEYLINE"
  ) {
    return family;
  }
  return "OTHER";
}

function portabilitySignals(outcomeType: string, conditions: string[]): {
  portabilityScore: number;
  dependencyRisk: number;
} {
  const family = outcomeFamily(outcomeType);
  let portabilityScore =
    family === "PLAYER" ? 0.45 :
    family === "TOTAL" ? 0.6 :
    family === "SPREAD" ? 0.62 :
    family === "MONEYLINE" ? 0.65 :
    0.55;
  let dependencyRisk =
    family === "PLAYER" ? 0.65 :
    family === "TOTAL" ? 0.35 :
    family === "SPREAD" ? 0.3 :
    family === "MONEYLINE" ? 0.25 :
    0.4;
  for (const c of conditions) {
    if (c.startsWith("home_role_dependency:") || c.startsWith("away_role_dependency:")) {
      if (c.endsWith(":EXTREME") || c.endsWith(":Q5")) dependencyRisk += 0.18;
      if (c.endsWith(":HIGH") || c.endsWith(":Q4")) dependencyRisk += 0.1;
    }
    if (c.startsWith("home_playmaking_resilience:") || c.startsWith("away_playmaking_resilience:")) {
      if (c.endsWith(":Q5") || c.endsWith(":GE_")) portabilityScore += 0.08;
      if (c.endsWith(":Q1") || c.endsWith(":LE_")) portabilityScore -= 0.08;
    }
  }
  portabilityScore = Math.max(0.05, Math.min(0.95, portabilityScore - dependencyRisk * 0.2));
  dependencyRisk = Math.max(0.05, Math.min(0.95, dependencyRisk));
  return { portabilityScore, dependencyRisk };
}

function clampProb(p: number): number {
  if (!Number.isFinite(p)) return 0.5;
  return Math.min(0.99, Math.max(0.01, p));
}

function normalUpperTail(z: number): number {
  // Abramowitz & Stegun style approximation for normal CDF.
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp((-z * z) / 2);
  const poly =
    (((((1.330274429 * t - 1.821255978) * t) + 1.781477937) * t - 0.356563782) * t + 0.319381530) * t;
  const cdf = z >= 0 ? 1 - d * poly : d * poly;
  return 1 - cdf;
}

function oneSidedPValueFromStats(stats: SplitStats): number {
  const n = Math.max(1, stats.n ?? 0);
  const baseline = clampProb((stats.posteriorHitRate ?? 0.5) - (stats.edge ?? 0));
  const expected = n * baseline;
  const variance = Math.max(1e-6, n * baseline * (1 - baseline));
  const z = ((stats.wins ?? 0) - expected) / Math.sqrt(variance);
  return Math.max(0, Math.min(1, normalUpperTail(z)));
}

function makeSeededRng(seed: number): () => number {
  let state = seed >>> 0;
  if (state === 0) state = 0x9e3779b9;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return ((state >>> 0) + 0.5) / 4294967296;
  };
}

function sampleBinomial(n: number, p: number, rng: () => number): number {
  let wins = 0;
  for (let i = 0; i < n; i++) {
    if (rng() < p) wins++;
  }
  return wins;
}

function permutationPValueFromStats(
  stats: SplitStats,
  iterations: number,
  rng: () => number,
): number {
  const n = Math.max(1, Math.floor(stats.n ?? 0));
  const observedWins = Math.max(0, Math.floor(stats.wins ?? 0));
  const baseline = clampProb((stats.posteriorHitRate ?? 0.5) - (stats.edge ?? 0));
  let extreme = 0;
  for (let i = 0; i < iterations; i++) {
    const simWins = sampleBinomial(n, baseline, rng);
    if (simWins >= observedWins) extreme++;
  }
  return (extreme + 1) / (iterations + 1);
}

function bhPassSet(items: Array<{ id: string; pValue: number }>, alpha: number): Set<string> {
  if (items.length === 0 || alpha <= 0) return new Set();
  if (alpha >= 1) return new Set(items.map((i) => i.id));
  const sorted = [...items].sort((a, b) => a.pValue - b.pValue);
  let maxIdx = -1;
  const m = sorted.length;
  for (let i = 0; i < m; i++) {
    const threshold = ((i + 1) / m) * alpha;
    if (sorted[i].pValue <= threshold) maxIdx = i;
  }
  if (maxIdx < 0) return new Set();
  return new Set(sorted.slice(0, maxIdx + 1).map((x) => x.id));
}

/**
 * Builds a season weight function: most recent season = 1.0, oldest = minWeight (e.g. 0.4).
 * Linear interpolation between min and max season.
 */
function makeSeasonWeightFn(
  minSeason: number,
  maxSeason: number,
  minWeight: number,
): (season: number) => number {
  const span = Math.max(1, maxSeason - minSeason);
  return (season: number) => {
    const t = (season - minSeason) / span;
    return minWeight + (1 - minWeight) * Math.max(0, Math.min(1, t));
  };
}

function computeStats(
  rows: TokenizedGame[],
  outcomeType: string,
  conditions: string[],
  baselineRate: number,
  priorStrength: number,
  seasonWeight?: (season: number) => number,
): SplitStats {
  let n = 0;
  let wins = 0;
  for (const r of rows) {
    if (!matchesConditions(r.tokens, conditions)) continue;
    const w = seasonWeight ? seasonWeight(r.season) : 1;
    n += w;
    if (r.outcomes.has(outcomeType)) wins += w;
  }
  const rawHitRate = n > 0 ? wins / n : 0;
  const priorBase = clampProb(baselineRate);
  const alpha = priorBase * priorStrength;
  const beta = (1 - priorBase) * priorStrength;
  const posteriorHitRate = (wins + alpha) / (n + alpha + beta);
  const edge = posteriorHitRate - priorBase;
  const lift = baselineRate > 0 ? rawHitRate / baselineRate : 0;
  return { n, wins, rawHitRate, posteriorHitRate, edge, lift };
}

function computeWeightedStats(
  rows: TokenizedGame[],
  outcomeType: string,
  conditions: string[],
  baselineRate: number,
  priorStrength: number,
  halfLifeDays: number,
  seasonWeight?: (season: number) => number,
): SplitStats {
  if (rows.length === 0) {
    return computeStats(rows, outcomeType, conditions, baselineRate, priorStrength, seasonWeight);
  }
  const anchor = rows.reduce((mx, r) => Math.max(mx, r.date.getTime()), 0);
  const lambda = Math.log(2) / Math.max(1, halfLifeDays);
  let n = 0;
  let wins = 0;
  for (const r of rows) {
    if (!matchesConditions(r.tokens, conditions)) continue;
    const ageDays = Math.max(0, (anchor - r.date.getTime()) / 86_400_000);
    const w = Math.exp(-lambda * ageDays);
    n += w;
    if (r.outcomes.has(outcomeType)) wins += w;
  }
  const rawHitRate = n > 0 ? wins / n : 0;
  const priorBase = clampProb(baselineRate);
  const alpha = priorBase * priorStrength;
  const beta = (1 - priorBase) * priorStrength;
  const posteriorHitRate = (wins + alpha) / (n + alpha + beta);
  const edge = posteriorHitRate - priorBase;
  const lift = baselineRate > 0 ? rawHitRate / baselineRate : 0;
  return { n, wins, rawHitRate, posteriorHitRate, edge, lift };
}

type SeasonSplit =
  | { mode: "default"; train: TokenizedGame[]; val: TokenizedGame[]; forward: TokenizedGame[] }
  | {
      mode: "loso";
      train: TokenizedGame[];
      losoFolds: Array<{ season: number; valRows: TokenizedGame[] }>;
      forward: TokenizedGame[];
    };

function splitRowsBySeason(rows: TokenizedGame[], trainSeason: number, valSeason: number, forwardFromSeason: number): SeasonSplit {
  const train = rows.filter((r) => r.season === trainSeason);
  const val = rows.filter((r) => r.season === valSeason);
  const forward = rows.filter((r) => r.season >= forwardFromSeason);
  return { mode: "default", train, val, forward };
}

function splitRowsBySeasonLOSO(
  rows: TokenizedGame[],
  forwardFromSeason: number,
): SeasonSplit {
  const losoRows = rows.filter((r) => r.season < forwardFromSeason);
  const forward = rows.filter((r) => r.season >= forwardFromSeason);
  const seasons = [...new Set(losoRows.map((r) => r.season))].sort((a, b) => a - b);
  const losoFolds = seasons.map((season) => ({
    season,
    valRows: losoRows.filter((r) => r.season === season),
  }));
  return { mode: "loso", train: losoRows, losoFolds, forward };
}

function treeCandidatesForOutcome(
  trainRows: TokenizedGame[],
  outcomeType: string,
  maxDepth: number,
  minLeaf: number,
  minTokenSupport: number,
): CandidatePattern[] {
  const tokenFreq = new Map<string, number>();
  for (const row of trainRows) {
    for (const t of row.tokens) {
      tokenFreq.set(t, (tokenFreq.get(t) ?? 0) + 1);
    }
  }
  const viableTokens = [...tokenFreq.entries()]
    .filter(([, n]) => n >= minTokenSupport && n <= Math.floor(trainRows.length * 0.95))
    .map(([t]) => t);

  const used = new Set<string>();
  const candidates: CandidatePattern[] = [];

  function recurse(rows: TokenizedGame[], depth: number, path: string[]): void {
    if (rows.length < minLeaf) return;
    if (depth >= maxDepth) {
      if (path.length > 0) {
        candidates.push({
          outcomeType,
          conditions: [...path],
          discoverySource: "tree",
        });
      }
      return;
    }

    let bestToken: string | null = null;
    let bestScore = -Infinity;
    let bestPresent: TokenizedGame[] = [];
    let bestAbsent: TokenizedGame[] = [];

    for (const token of viableTokens) {
      if (used.has(token)) continue;
      const present: TokenizedGame[] = [];
      const absent: TokenizedGame[] = [];
      for (const r of rows) {
        if (r.tokens.has(token)) present.push(r);
        else absent.push(r);
      }
      if (present.length < minLeaf || absent.length < minLeaf) continue;

      const pHit = present.filter((r) => r.outcomes.has(outcomeType)).length / present.length;
      const aHit = absent.filter((r) => r.outcomes.has(outcomeType)).length / absent.length;
      const score = Math.abs(pHit - aHit) * Math.log(rows.length);
      if (score > bestScore) {
        bestScore = score;
        bestToken = token;
        bestPresent = present;
        bestAbsent = absent;
      }
    }

    if (!bestToken) {
      if (path.length > 0) {
        candidates.push({
          outcomeType,
          conditions: [...path],
          discoverySource: "tree",
        });
      }
      return;
    }

    used.add(bestToken);
    recurse(bestPresent, depth + 1, [...path, bestToken]);
    recurse(bestAbsent, depth + 1, [...path, `!${bestToken}`]);
    used.delete(bestToken);
  }

  recurse(trainRows, 0, []);
  return candidates;
}

function aprioriFrequentItemsets(trainRows: TokenizedGame[], maxSize: number, minSupport: number): string[][] {
  const tokenCounts = new Map<string, number>();
  for (const row of trainRows) {
    for (const t of row.tokens) tokenCounts.set(t, (tokenCounts.get(t) ?? 0) + 1);
  }

  let frequent: string[][] = [...tokenCounts.entries()]
    .filter(([, c]) => c >= minSupport)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 48)
    .map(([t]) => [t]);

  const all: string[][] = [...frequent];

  for (let size = 2; size <= maxSize; size++) {
    const candidateSet = new Set<string>();
    for (let i = 0; i < frequent.length; i++) {
      for (let j = i + 1; j < frequent.length; j++) {
        const merged = [...new Set([...frequent[i], ...frequent[j]])].sort();
        if (merged.length === size) candidateSet.add(merged.join("|"));
      }
    }

    const candidates = [...candidateSet].map((s) => s.split("|"));
    const next: string[][] = [];
    for (const combo of candidates) {
      let support = 0;
      for (const row of trainRows) {
        let ok = true;
        for (const token of combo) {
          if (!row.tokens.has(token)) {
            ok = false;
            break;
          }
        }
        if (ok) support++;
      }
      if (support >= minSupport) next.push(combo);
    }

    if (next.length === 0) break;
    all.push(...next);
    frequent = next;
  }

  return all;
}

async function loadTokenizedGames(): Promise<TokenizedGame[]> {
  const tokenRows = await prisma.$queryRawUnsafe<Array<{ gameId: string; season: number; date: Date; tokens: string[] }>>(
    `SELECT "gameId", "season", "date", "tokens" FROM "GameFeatureToken"`,
  );
  const outcomeRows = await prisma.gameEvent.findMany({
    where: { type: "outcome" },
    select: { gameId: true, eventKey: true, side: true },
  });

  const outcomesByGame = new Map<string, Set<string>>();
  for (const row of outcomeRows) {
    const key = `${row.eventKey}:${row.side}`;
    const set = outcomesByGame.get(row.gameId) ?? new Set<string>();
    set.add(key);
    outcomesByGame.set(row.gameId, set);
  }

  const games: TokenizedGame[] = [];
  for (const row of tokenRows) {
    const outcomes = outcomesByGame.get(row.gameId);
    if (!outcomes || outcomes.size === 0) continue;
    games.push({
      gameId: row.gameId,
      season: row.season,
      date: new Date(row.date),
      tokens: new Set(row.tokens ?? []),
      outcomes,
    });
  }
  return games;
}

export async function discoverPatternsV2(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const objective = (flags.objective ?? DISCOVERY_DEFAULTS.objective ?? "edge").toLowerCase();
  const useHitRateObjective = objective === "hit_rate";
  const cvMode = (flags["cv"] ?? "default") as "default" | "loso";
  const trainSeason = Number(flags["train-season"] ?? DISCOVERY_DEFAULTS.trainSeason);
  const valSeason = Number(flags["val-season"] ?? DISCOVERY_DEFAULTS.valSeason);
  const forwardFrom = Number(flags["forward-from"] ?? DISCOVERY_DEFAULTS.forwardFrom);
  const minLosoFoldsPass = Math.max(1, Number(flags["min-loso-folds-pass"] ?? DISCOVERY_DEFAULTS.minLosoFoldsPass));
  const maxDepth = Number(flags["tree-depth"] ?? DISCOVERY_DEFAULTS.maxDepth);
  const minLeaf = Number(flags["min-samples"] ?? DISCOVERY_DEFAULTS.minLeaf);
  const minTokenSupport = Number(flags["min-token-support"] ?? DISCOVERY_DEFAULTS.minTokenSupport);
  const minOutcomeSamples = Number(flags["min-outcome-samples"] ?? DISCOVERY_DEFAULTS.minOutcomeSamples);
  const maxItemset = Number(flags["max-itemset-size"] ?? DISCOVERY_DEFAULTS.maxItemset);
  const minSupport = Number(flags["min-support"] ?? DISCOVERY_DEFAULTS.minSupport);
  const maxPatterns = Number(flags["max-patterns"] ?? DISCOVERY_DEFAULTS.maxPatterns);
  const minConditionCount = Math.max(1, Number(flags["min-condition-count"] ?? DISCOVERY_DEFAULTS.minConditionCount));
  const minDistinctFeatureCount = Math.max(1, Number(flags["min-distinct-features"] ?? DISCOVERY_DEFAULTS.minDistinctFeatureCount));
  const maxPerOutcome = Math.max(1, Number(flags["max-per-outcome"] ?? DISCOVERY_DEFAULTS.maxPerOutcome));
  const maxPerFamily = Math.max(1, Number(flags["max-per-family"] ?? DISCOVERY_DEFAULTS.maxPerFamily));
  const maxPerFamilyByBucket: Record<FamilyBucket, number> = {
    PLAYER: Math.max(
      1,
      Number(flags["max-per-family-player"] ?? DISCOVERY_DEFAULTS.maxPerFamilyByBucket.PLAYER),
    ),
    TOTAL: Math.max(
      1,
      Number(flags["max-per-family-total"] ?? DISCOVERY_DEFAULTS.maxPerFamilyByBucket.TOTAL),
    ),
    SPREAD: Math.max(
      1,
      Number(flags["max-per-family-spread"] ?? DISCOVERY_DEFAULTS.maxPerFamilyByBucket.SPREAD),
    ),
    MONEYLINE: Math.max(
      1,
      Number(flags["max-per-family-moneyline"] ?? DISCOVERY_DEFAULTS.maxPerFamilyByBucket.MONEYLINE),
    ),
    OTHER: Math.max(
      1,
      Number(flags["max-per-family-other"] ?? DISCOVERY_DEFAULTS.maxPerFamilyByBucket.OTHER),
    ),
  };
  const maxTrainCoverage = Math.min(1, Math.max(0.01, Number(flags["max-train-coverage"] ?? DISCOVERY_DEFAULTS.maxTrainCoverage)));
  const minOutcomeTrainSeasons = Math.max(1, Number(flags["min-outcome-train-seasons"] ?? DISCOVERY_DEFAULTS.minOutcomeTrainSeasons));
  const minOutcomeTrainCoverage = Math.max(0, Number(flags["min-outcome-train-coverage"] ?? DISCOVERY_DEFAULTS.minOutcomeTrainCoverage));
  const maxOutcomeTrainCoverage = Math.min(1, Math.max(0.01, Number(flags["max-outcome-train-coverage"] ?? DISCOVERY_DEFAULTS.maxOutcomeTrainCoverage)));
  const minValSamples = Math.max(1, Number(flags["min-val-samples"] ?? DISCOVERY_DEFAULTS.minValSamples));
  const minForwardSamples = Math.max(1, Number(flags["min-forward-samples"] ?? DISCOVERY_DEFAULTS.minForwardSamples));
  const minValPosteriorDiscovery = Number(
    flags["min-val-posterior-discovery"] ?? DISCOVERY_DEFAULTS.minValPosteriorDiscovery,
  );
  const minForwardPosteriorDiscovery = Number(
    flags["min-forward-posterior-discovery"] ?? DISCOVERY_DEFAULTS.minForwardPosteriorDiscovery,
  );
  const minValEdge = Number(flags["min-val-edge"] ?? DISCOVERY_DEFAULTS.minValEdge);
  const minForwardEdgeDiscovery = Number(flags["min-forward-edge-discovery"] ?? DISCOVERY_DEFAULTS.minForwardEdgeDiscovery);
  const maxConditionOverlap = Math.min(0.99, Math.max(0, Number(flags["max-condition-overlap"] ?? DISCOVERY_DEFAULTS.maxConditionOverlap)));
  const priorStrength = Number(flags["prior-strength"] ?? DISCOVERY_DEFAULTS.priorStrength);
  const recencyHalfLifeDays = Math.max(7, Number(flags["recency-half-life-days"] ?? DISCOVERY_DEFAULTS.recencyHalfLifeDays));
  const lowNPenaltyK = Math.max(1, Number(flags["low-n-penalty-k"] ?? DISCOVERY_DEFAULTS.lowNPenaltyK));
  const forwardStabilityWeight = Math.max(
    0,
    Number(flags["forward-stability-weight"] ?? DISCOVERY_DEFAULTS.forwardStabilityWeight),
  );
  const seasonWeightEnabled =
    (flags["season-weight"] ?? String(DISCOVERY_DEFAULTS.seasonWeightEnabled)) !== "false";
  const seasonWeightMin = Math.max(
    0.1,
    Math.min(1, Number(flags["season-weight-min"] ?? DISCOVERY_DEFAULTS.seasonWeightMin)),
  );

  console.log("\n=== Discover Patterns v2 ===\n");
  const games = await loadTokenizedGames();
  const splits: SeasonSplit =
    cvMode === "loso"
      ? splitRowsBySeasonLOSO(games, forwardFrom)
      : splitRowsBySeason(games, trainSeason, valSeason, forwardFrom);

  if (splits.train.length === 0 || splits.forward.length === 0) {
    throw new Error("Insufficient split coverage. Make sure train and forward have quantized games.");
  }
  if (splits.mode === "default" && splits.val.length === 0) {
    throw new Error("Insufficient val coverage. Make sure val season has quantized games.");
  }
  if (splits.mode === "loso" && splits.losoFolds.length < 2) {
    throw new Error("LOSO requires at least 2 seasons in the train range.");
  }
  const effectiveMinLosoFolds =
    splits.mode === "loso"
      ? Math.min(minLosoFoldsPass, splits.losoFolds.length)
      : minLosoFoldsPass;
  if (splits.mode === "loso" && minLosoFoldsPass > splits.losoFolds.length) {
    console.log(
      `Note: min-loso-folds-pass (${minLosoFoldsPass}) > folds (${splits.losoFolds.length}); using ${effectiveMinLosoFolds}.\n`,
    );
  }
  const allSeasons = [...new Set(games.map((r) => r.season))].sort((a, b) => a - b);
  const minSeason = allSeasons[0] ?? 2023;
  const maxSeason = allSeasons[allSeasons.length - 1] ?? 2025;
  const seasonWeight =
    seasonWeightEnabled && allSeasons.length > 1
      ? makeSeasonWeightFn(minSeason, maxSeason, seasonWeightMin)
      : undefined;
  if (seasonWeight != null) {
    console.log(
      `Season weighting: enabled (min=${seasonWeightMin}, ${minSeason}..${maxSeason}), recent seasons upweighted.\n`,
    );
  }
  // Note: Discovery uses GameFeatureToken from build:quantized-game-features (default from-season 2023).
  // To use 2021+ game-odds data, run build:day-bundles + ingest:day-bundles for that range first, then
  // build:game-context, build:game-events, build:feature-bins, build:quantized-game-features --from-season 2021.
  if (splits.mode === "loso") {
    const seasons = splits.losoFolds.map((f) => f.season).join(", ");
    console.log(
      `LOSO seasons: ${seasons} (${splits.losoFolds.length} folds), min ${effectiveMinLosoFolds} folds must pass, forward: ${forwardFrom}+\n`,
    );
  }

  const outcomes = new Map<string, number>();
  const outcomeTrainSeasons = new Map<string, Set<number>>();
  for (const row of splits.train) {
    for (const o of row.outcomes) {
      outcomes.set(o, (outcomes.get(o) ?? 0) + 1);
      const seasons = outcomeTrainSeasons.get(o) ?? new Set<number>();
      seasons.add(row.season);
      outcomeTrainSeasons.set(o, seasons);
    }
  }
  const trainTotal = Math.max(1, splits.train.length);
  const outcomeTypes = [...outcomes.entries()]
    .filter(([o, n]) => {
      if (n < minOutcomeSamples) return false;
      const coverage = n / trainTotal;
      if (coverage < minOutcomeTrainCoverage) return false;
      if (coverage > maxOutcomeTrainCoverage) return false;
      const seasons = outcomeTrainSeasons.get(o)?.size ?? 0;
      if (seasons < minOutcomeTrainSeasons) return false;
      return true;
    })
    .map(([o]) => o)
    .sort();

  const rawCandidates: CandidatePattern[] = [];
  for (const outcomeType of outcomeTypes) {
    rawCandidates.push(
      ...treeCandidatesForOutcome(splits.train, outcomeType, maxDepth, minLeaf, minTokenSupport),
    );
  }

  const itemsets = aprioriFrequentItemsets(splits.train, maxItemset, minSupport);
  for (const outcomeType of outcomeTypes) {
    for (const itemset of itemsets) {
      rawCandidates.push({
        outcomeType,
        conditions: itemset,
        discoverySource: "fpgrowth",
      });
    }
  }

  const deduped = new Map<string, CandidatePattern>();
  for (const c of rawCandidates) {
    const key = `${c.outcomeType}|${c.conditions.slice().sort().join("&")}`;
    if (!deduped.has(key)) deduped.set(key, c);
  }

  const scored: ScoredPattern[] = [];
  for (const c of deduped.values()) {
    if ((c.conditions?.length ?? 0) < minConditionCount) continue;
    if (distinctConditionFeatureCount(c.conditions ?? []) < minDistinctFeatureCount) continue;
    if (
      (c.conditions?.length ?? 0) === 1 &&
      c.conditions[0] &&
      !c.conditions[0].startsWith("!") &&
      isLowSpecificityConditionToken(c.conditions[0])
    ) {
      continue;
    }

    const trainBase =
      splits.train.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.train.length);
    const forwardBase =
      splits.forward.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.forward.length);

    const train = computeStats(
      splits.train,
      c.outcomeType,
      c.conditions,
      trainBase,
      priorStrength,
      seasonWeight,
    );
    if (train.n < minLeaf) continue;
    const trainRecency = computeWeightedStats(
      splits.train,
      c.outcomeType,
      c.conditions,
      trainBase,
      priorStrength,
      recencyHalfLifeDays,
      seasonWeight,
    );
    const forward = computeStats(
      splits.forward,
      c.outcomeType,
      c.conditions,
      forwardBase,
      priorStrength,
      seasonWeight,
    );
    if (forward.n < minForwardSamples) continue;
    if (useHitRateObjective) {
      if (forward.posteriorHitRate < minForwardPosteriorDiscovery) continue;
    } else if (forward.edge < minForwardEdgeDiscovery) {
      continue;
    }

    let val: SplitStats;
    let valOk: boolean;
    if (splits.mode === "default") {
      const valRows = splits.val;
      const valBase =
        valRows.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, valRows.length);
      val = computeStats(
        valRows,
        c.outcomeType,
        c.conditions,
        valBase,
        priorStrength,
        seasonWeight,
      );
      valOk = useHitRateObjective
        ? val.n >= minValSamples && val.posteriorHitRate >= minValPosteriorDiscovery
        : val.n >= minValSamples && val.edge >= minValEdge;
    } else {
      const losoFoldStats: SplitStats[] = [];
      for (const fold of splits.losoFolds) {
        const foldBase =
          fold.valRows.filter((r) => r.outcomes.has(c.outcomeType)).length /
          Math.max(1, fold.valRows.length);
        losoFoldStats.push(
          computeStats(
            fold.valRows,
            c.outcomeType,
            c.conditions,
            foldBase,
            priorStrength,
            seasonWeight,
          ),
        );
      }
      const foldsWithPositiveSignal = losoFoldStats.filter(
        (s) =>
          useHitRateObjective
            ? s.n >= minValSamples && s.posteriorHitRate >= minValPosteriorDiscovery
            : s.n >= minValSamples && s.edge >= minValEdge,
      ).length;
      valOk = foldsWithPositiveSignal >= effectiveMinLosoFolds;
      const totalN = losoFoldStats.reduce((acc, s) => acc + s.n, 0);
      const totalWins = losoFoldStats.reduce((acc, s) => acc + s.wins, 0);
      const meanEdge =
        losoFoldStats.length > 0
          ? losoFoldStats.reduce((acc, s) => acc + s.edge, 0) / losoFoldStats.length
          : 0;
      const avgPosterior =
        losoFoldStats.length > 0
          ? losoFoldStats.reduce((acc, s) => acc + s.posteriorHitRate, 0) / losoFoldStats.length
          : 0.5;
      val = {
        n: totalN,
        wins: totalWins,
        rawHitRate: totalN > 0 ? totalWins / totalN : 0,
        posteriorHitRate: avgPosterior,
        edge: meanEdge,
        lift: meanEdge > 0 ? 1 + meanEdge : 0,
      };
    }
    if (!valOk) continue;

    const trainCoverage = train.n / Math.max(1, splits.train.length);
    if (trainCoverage > maxTrainCoverage) continue;

    const stabilityFactor =
      (val.edge > 0 ? 0.6 : 0.2) +
      (forward.edge > 0 ? 0.5 : 0.1) +
      (Math.sign(train.edge) === Math.sign(val.edge) ? 0.2 : 0);
    const forwardGapVsVal = Math.abs(forward.edge - val.edge);
    const forwardGapVsTrain = Math.abs(forward.edge - trainRecency.edge);
    const forwardStability =
      1 / (1 + forwardStabilityWeight * (forwardGapVsVal + forwardGapVsTrain));
    const forwardDirectionalBonus =
      forward.edge >= val.edge && val.edge >= 0 ? 1.08 : forward.edge >= 0 ? 1.02 : 0.92;

    // Prefer specific patterns over broad "always-on" rules.
    const specificityFactor = Math.sqrt(Math.max(0.01, 1 - trainCoverage));
    const uncertaintyPenalty = Math.sqrt(trainRecency.n / (trainRecency.n + lowNPenaltyK));
    const portability = portabilitySignals(c.outcomeType, c.conditions ?? []);
    const fragilityPenalty = 1 - portability.dependencyRisk * 0.25;
    const portabilityFactor = 0.7 + portability.portabilityScore * 0.6;
    const baseSignal = useHitRateObjective
      ? Math.max(0, trainRecency.posteriorHitRate - 0.5)
      : trainRecency.edge;
    const score =
      baseSignal *
      Math.log(Math.max(2, trainRecency.n)) *
      stabilityFactor *
      specificityFactor *
      uncertaintyPenalty *
      portabilityFactor *
      fragilityPenalty *
      forwardStability *
      forwardDirectionalBonus;
    scored.push({
      ...c,
      train,
      val,
      forward,
      rawHitRate: train.rawHitRate,
      posteriorHitRate: train.posteriorHitRate,
      lift: train.lift,
      edge: train.edge,
      score,
      n: train.n,
      trainCoverage,
    });
  }

  scored.sort((a, b) => b.score - a.score || b.edge - a.edge || b.n - a.n);
  const top: ScoredPattern[] = [];
  const perOutcome = new Map<string, number>();
  const perFamily = new Map<FamilyBucket, number>();
  const selectedByOutcome = new Map<string, ScoredPattern[]>();
  for (const p of scored) {
    const used = perOutcome.get(p.outcomeType) ?? 0;
    if (used >= maxPerOutcome) continue;
    const family = outcomeFamilyBucket(p.outcomeType);
    const familyUsed = perFamily.get(family) ?? 0;
    const familyCap = Math.max(1, Math.min(maxPerFamily, maxPerFamilyByBucket[family] ?? maxPerFamily));
    if (familyUsed >= familyCap) continue;
    const existing = selectedByOutcome.get(p.outcomeType) ?? [];
    const tooSimilar = existing.some(
      (e) => conditionOverlap(e.conditions ?? [], p.conditions ?? []) >= maxConditionOverlap,
    );
    if (tooSimilar) continue;
    top.push(p);
    perOutcome.set(p.outcomeType, used + 1);
    perFamily.set(family, familyUsed + 1);
    selectedByOutcome.set(p.outcomeType, [...existing, p]);
    if (top.length >= maxPatterns) break;
  }

  await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2Hit"`);
  await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2"`);
  for (const p of top) {
    const id = crypto.randomUUID();
    const conditions = toPgTextArrayLiteral(p.conditions);
    await prisma.$executeRawUnsafe(
      `INSERT INTO "PatternV2"
      ("id","outcomeType","conditions","discoverySource","trainStats","valStats","forwardStats","rawHitRate","posteriorHitRate","lift","edge","score","n","status","createdAt","updatedAt")
      VALUES (
        '${id}',
        '${sqlEsc(p.outcomeType)}',
        '${conditions}',
        '${p.discoverySource}',
        '${sqlEsc(JSON.stringify(p.train))}'::jsonb,
        '${sqlEsc(JSON.stringify(p.val))}'::jsonb,
        '${sqlEsc(JSON.stringify(p.forward))}'::jsonb,
        ${p.rawHitRate},
        ${p.posteriorHitRate},
        ${p.lift},
        ${p.edge},
        ${p.score},
        ${p.n},
        'candidate',
        NOW(),
        NOW()
      )`,
    );
  }

  console.log(`Generated ${rawCandidates.length} raw candidates.`);
  const cvNote = cvMode === "loso" ? `CV=loso min-folds-pass=${effectiveMinLosoFolds}; ` : "";
  console.log(
    `Scored ${scored.length} patterns. Stored top ${top.length} into PatternV2 (objective=${objective}; ${cvNote}outcome filters: min samples=${minOutcomeSamples}, seasons>=${minOutcomeTrainSeasons}, coverage ${minOutcomeTrainCoverage}-${maxOutcomeTrainCoverage}; candidate filters: min conditions=${minConditionCount}, min distinct features=${minDistinctFeatureCount}, max/outcome=${maxPerOutcome}, max/family=${maxPerFamily}, max train coverage=${maxTrainCoverage}, min val samples=${minValSamples}, min forward samples=${minForwardSamples}${useHitRateObjective ? `, min val posterior=${minValPosteriorDiscovery}, min forward posterior=${minForwardPosteriorDiscovery}` : cvMode === "default" ? `, min val edge=${minValEdge}` : ""}${useHitRateObjective ? "" : `, min forward edge=${minForwardEdgeDiscovery}`}, recency half-life=${recencyHalfLifeDays}d, low-n penalty k=${lowNPenaltyK}, max overlap=${maxConditionOverlap}, outcomes=${perOutcome.size}, families=${perFamily.size}).`,
  );
}

type StoredPatternRow = {
  id: string;
  outcomeType: string;
  conditions: string[];
  trainStats: SplitStats;
  valStats: SplitStats;
  forwardStats: SplitStats;
  status: string;
};

async function loadStoredPatternsV2(): Promise<StoredPatternRow[]> {
  const rows = await prisma.$queryRawUnsafe<
    Array<{
      id: string;
      outcomeType: string;
      conditions: string[];
      trainStats: unknown;
      valStats: unknown;
      forwardStats: unknown;
      status: string;
    }>
  >(
    `SELECT "id","outcomeType","conditions","trainStats","valStats","forwardStats","status" FROM "PatternV2"`,
  );
  return rows.map((r) => ({
    id: r.id,
    outcomeType: r.outcomeType,
    conditions: r.conditions ?? [],
    trainStats: r.trainStats as SplitStats,
    valStats: r.valStats as SplitStats,
    forwardStats: r.forwardStats as SplitStats,
    status: r.status,
  }));
}

async function rebuildPatternV2HitsForDeployed(options?: {
  progressEveryPattern?: number;
  progressEveryInsert?: number;
}): Promise<void> {
  const progressEveryPattern = Math.max(1, options?.progressEveryPattern ?? 10);
  const progressEveryInsert = Math.max(1, options?.progressEveryInsert ?? 5000);
  const startedAt = Date.now();
  const patterns = await prisma.$queryRawUnsafe<Array<{ id: string; outcomeType: string; conditions: string[] }>>(
    `SELECT "id","outcomeType","conditions" FROM "PatternV2" WHERE "status" = 'deployed'`,
  );
  if (patterns.length === 0) {
    await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2Hit"`);
    console.log("Hit rebuild skipped: no deployed patterns.");
    return;
  }
  const games = await loadTokenizedGames();
  await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2Hit"`);
  console.log(
    `Rebuilding PatternV2Hit for ${patterns.length} deployed patterns across ${games.length} tokenized games...`,
  );

  const BATCH_SIZE = 2000;
  const rows: Array<{ patternId: string; gameId: string; hitBool: boolean; date: string }> = [];
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    let matchedForPattern = 0;
    for (const g of games) {
      if (!matchesConditions(g.tokens, p.conditions ?? [])) continue;
      const hit = g.outcomes.has(p.outcomeType);
      matchedForPattern++;
      rows.push({
        patternId: p.id,
        gameId: g.gameId,
        hitBool: hit,
        date: g.date.toISOString().slice(0, 10),
      });
    }
    if ((i + 1) % progressEveryPattern === 0 || i + 1 === patterns.length) {
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(
        `  Pattern ${i + 1}/${patterns.length} processed (${matchedForPattern} matches, ${rows.length} rows queued, ${elapsedSec}s elapsed)`,
      );
    }
  }

  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const values = batch
      .map(
        (r) =>
          `('${crypto.randomUUID()}','${sqlEsc(r.patternId)}','${sqlEsc(r.gameId)}',${r.hitBool ? "TRUE" : "FALSE"},'${r.date}')`,
      )
      .join(",");
    await prisma.$executeRawUnsafe(
      `INSERT INTO "PatternV2Hit" ("id","patternId","gameId","hitBool","date") VALUES ${values}
       ON CONFLICT ("patternId","gameId") DO NOTHING`,
    );
    inserted += batch.length;
    if (inserted % progressEveryInsert === 0 || i + BATCH_SIZE >= rows.length) {
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(`  Hit rebuild inserts: ${inserted} (${elapsedSec}s elapsed)`);
    }
  }
  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`Rebuilt ${inserted} PatternV2Hit rows in ${elapsedSec}s.`);
}

export async function validatePatternsV2(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const objective = (flags.objective ?? VALIDATION_DEFAULTS.objective ?? "edge").toLowerCase();
  const useHitRateObjective = objective === "hit_rate";
  const minTrainEdge = Number(flags["min-train-edge"] ?? VALIDATION_DEFAULTS.minTrainEdge);
  const minValEdge = Number(flags["min-val-edge"] ?? VALIDATION_DEFAULTS.minValEdge);
  const minForwardEdge = Number(flags["min-forward-edge"] ?? VALIDATION_DEFAULTS.minForwardEdge);
  const minTrainPosterior = Number(flags["min-train-posterior"] ?? VALIDATION_DEFAULTS.minTrainPosterior);
  const minValPosterior = Number(flags["min-val-posterior"] ?? VALIDATION_DEFAULTS.minValPosterior);
  const minForwardPosterior = Number(flags["min-forward-posterior"] ?? VALIDATION_DEFAULTS.minForwardPosterior);
  const minTrainSamples = Math.max(1, Number(flags["min-train-samples"] ?? VALIDATION_DEFAULTS.minTrainSamples));
  const minValSamples = Math.max(1, Number(flags["min-val-samples"] ?? VALIDATION_DEFAULTS.minValSamples));
  const minForwardSamples = Math.max(1, Number(flags["min-forward-samples"] ?? VALIDATION_DEFAULTS.minForwardSamples));
  const uncertaintyPenaltyK = Math.max(1, Number(flags["uncertainty-penalty-k"] ?? VALIDATION_DEFAULTS.uncertaintyPenaltyK));
  const uncertaintyPenaltyScale = Math.max(0, Number(flags["uncertainty-penalty-scale"] ?? VALIDATION_DEFAULTS.uncertaintyPenaltyScale));
  const fdrAlpha = Math.max(0, Math.min(1, Number(flags["fdr-alpha"] ?? VALIDATION_DEFAULTS.fdrAlpha)));
  const fdrMethod = (flags["fdr-method"] ?? VALIDATION_DEFAULTS.fdrMethod).toLowerCase();
  const fdrPermutations = Math.max(100, Number(flags["fdr-permutations"] ?? VALIDATION_DEFAULTS.fdrPermutations));
  const fdrSeed = Number(flags["fdr-seed"] ?? VALIDATION_DEFAULTS.fdrSeed);
  const runDecay = (flags["run-decay"] ?? String(VALIDATION_DEFAULTS.runDecay)) !== "false";
  const decayMinWindowSamples = Math.max(1, Number(flags["decay-min-window-samples"] ?? VALIDATION_DEFAULTS.decayMinWindowSamples));
  const decayCollapseEdge = Number(flags["decay-collapse-edge"] ?? VALIDATION_DEFAULTS.decayCollapseEdge);
  const progressEvery = Math.max(1, Number(flags["progress-every"] ?? VALIDATION_DEFAULTS.progressEvery));
  const startedAt = Date.now();
  if (!["bh", "perm-bh"].includes(fdrMethod)) {
    throw new Error(`Invalid --fdr-method '${fdrMethod}'. Use bh|perm-bh.`);
  }
  const rng = makeSeededRng(Number.isFinite(fdrSeed) ? fdrSeed : 1337);

  console.log("\n=== Validate Patterns v2 ===\n");
  const countResult = await prisma.$queryRawUnsafe<Array<{ count: bigint }>>(
    `SELECT COUNT(*) AS count FROM "PatternV2"`,
  );
  const rawCount = Number(countResult[0]?.count ?? 0);
  console.log(`PatternV2 table has ${rawCount} rows in database.`);
  const patterns = await loadStoredPatternsV2();
  console.log(`Loaded ${patterns.length} patterns for validation.`);
  let deployed = 0;
  let validated = 0;
  let retired = 0;
  let candidates = 0;

  const evalRows: Array<{
    id: string;
    status: string;
    trainOk: boolean;
    valOk: boolean;
    forwardOk: boolean;
    pValue: number;
  }> = [];
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    const trainPenalizedEdge =
      (p.trainStats.edge ?? -1) -
      uncertaintyPenaltyScale / Math.sqrt(Math.max(1, (p.trainStats.n ?? 0) + uncertaintyPenaltyK));
    const valPenalizedEdge =
      (p.valStats.edge ?? -1) -
      uncertaintyPenaltyScale / Math.sqrt(Math.max(1, (p.valStats.n ?? 0) + uncertaintyPenaltyK));
    const forwardPenalizedEdge =
      (p.forwardStats.edge ?? -1) -
      uncertaintyPenaltyScale / Math.sqrt(Math.max(1, (p.forwardStats.n ?? 0) + uncertaintyPenaltyK));
    const trainOk = useHitRateObjective
      ? (p.trainStats.n ?? 0) >= minTrainSamples &&
        (p.trainStats.posteriorHitRate ?? 0) >= minTrainPosterior
      : (p.trainStats.n ?? 0) >= minTrainSamples &&
        trainPenalizedEdge >= minTrainEdge;
    const valOk = useHitRateObjective
      ? (p.valStats.n ?? 0) >= minValSamples &&
        (p.valStats.posteriorHitRate ?? 0) >= minValPosterior
      : (p.valStats.n ?? 0) >= minValSamples &&
        valPenalizedEdge >= minValEdge &&
        (p.valStats.posteriorHitRate ?? 0) >= minValPosterior;
    const forwardOk = useHitRateObjective
      ? (p.forwardStats.n ?? 0) >= minForwardSamples &&
        (p.forwardStats.posteriorHitRate ?? 0) >= minForwardPosterior
      : (p.forwardStats.n ?? 0) >= minForwardSamples &&
        forwardPenalizedEdge >= minForwardEdge &&
        (p.forwardStats.posteriorHitRate ?? 0) >= minForwardPosterior;

    const pVal =
      fdrMethod === "perm-bh"
        ? permutationPValueFromStats(p.valStats, fdrPermutations, rng)
        : oneSidedPValueFromStats(p.valStats);
    const pForward =
      fdrMethod === "perm-bh"
        ? permutationPValueFromStats(p.forwardStats, fdrPermutations, rng)
        : oneSidedPValueFromStats(p.forwardStats);
    evalRows.push({
      id: p.id,
      status: p.status,
      trainOk,
      valOk,
      forwardOk,
      pValue: Math.max(pVal, pForward),
    });
  }

  const fdrEligible = evalRows
    .filter((r) => r.trainOk && r.valOk && r.forwardOk)
    .map((r) => ({ id: r.id, pValue: r.pValue }));
  const fdrPass = bhPassSet(fdrEligible, fdrAlpha);
  const familyEligible = new Map<string, number>();
  const familyPassed = new Map<string, number>();

  for (let i = 0; i < evalRows.length; i++) {
    const row = evalRows[i];
    let nextStatus = "candidate";
    if (row.trainOk && row.valOk && row.forwardOk && fdrPass.has(row.id)) {
      nextStatus = "deployed";
      deployed++;
      const family = outcomeFamily(patterns[i]?.outcomeType ?? "");
      familyPassed.set(family, (familyPassed.get(family) ?? 0) + 1);
    } else if (row.trainOk && row.valOk) {
      nextStatus = "validated";
      validated++;
    } else if (row.status === "deployed" || row.status === "validated") {
      nextStatus = "retired";
      retired++;
    } else {
      candidates++;
    }
    if (row.trainOk && row.valOk && row.forwardOk) {
      const family = outcomeFamily(patterns[i]?.outcomeType ?? "");
      familyEligible.set(family, (familyEligible.get(family) ?? 0) + 1);
    }

    const retiredAt = nextStatus === "retired" ? "NOW()" : "NULL";
    await prisma.$executeRawUnsafe(
      `UPDATE "PatternV2"
       SET "status" = '${nextStatus}', "retiredAt" = ${retiredAt}, "updatedAt" = NOW()
       WHERE "id" = '${sqlEsc(row.id)}'`,
    );
    if ((i + 1) % progressEvery === 0 || i + 1 === evalRows.length) {
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(
        `Validate progress: ${i + 1}/${evalRows.length} (deployed=${deployed}, validated=${validated}, retired=${retired}, candidate=${candidates}, ${elapsedSec}s elapsed)`,
      );
    }
  }

  await rebuildPatternV2HitsForDeployed({
    progressEveryPattern: Math.max(1, Math.floor(progressEvery / 5)),
  });
  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`Status counts: deployed=${deployed}, validated=${validated}, retired=${retired}, candidate=${candidates}`);
  if (useHitRateObjective) {
    console.log(
      `Thresholds: objective=hit_rate | train(posterior>=${minTrainPosterior}, n>=${minTrainSamples}) | val(posterior>=${minValPosterior}, n>=${minValSamples}) | forward(posterior>=${minForwardPosterior}, n>=${minForwardSamples})`,
    );
  } else {
    console.log(
      `Thresholds: objective=edge | train(penalized edge>=${minTrainEdge}, n>=${minTrainSamples}) | val(penalized edge>=${minValEdge}, posterior>=${minValPosterior}, n>=${minValSamples}) | forward(penalized edge>=${minForwardEdge}, posterior>=${minForwardPosterior}, n>=${minForwardSamples}) | uncertainty k=${uncertaintyPenaltyK}`,
    );
    console.log(
      `Uncertainty penalty: edge -= ${uncertaintyPenaltyScale}/sqrt(n + ${uncertaintyPenaltyK})`,
    );
  }
  console.log(
    `FDR gate: method=${fdrMethod}, alpha=${fdrAlpha.toFixed(3)} | eligible=${fdrEligible.length} | passed=${fdrPass.size}` +
      (fdrMethod === "perm-bh" ? ` | permutations=${fdrPermutations}, seed=${fdrSeed}` : ""),
  );
  const familyRows = [...new Set([...familyEligible.keys(), ...familyPassed.keys()])].sort();
  if (familyRows.length > 0) {
    console.log("FDR by family:");
    for (const family of familyRows) {
      const eligible = familyEligible.get(family) ?? 0;
      const passed = familyPassed.get(family) ?? 0;
      console.log(`  ${family}: eligible=${eligible}, passed=${passed}`);
    }
  }
  if (runDecay) {
    await monitorPatternDecay([
      "--min-window-samples",
      String(decayMinWindowSamples),
      "--collapse-edge",
      String(decayCollapseEdge),
    ]);
  } else {
    console.log("Decay monitor skipped (--run-decay false).");
  }
  console.log(`Validation flow complete in ${elapsedSec}s.`);
}

export async function monitorPatternDecay(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const minSamples = Number(flags["min-window-samples"] ?? 20);
  const collapseEdge = Number(flags["collapse-edge"] ?? -0.01);
  const priorStrength = Number(flags["prior-strength"] ?? 25);
  const clvFeedback = (flags["clv-feedback"] ?? "true") !== "false";
  const clvWindowDays = Math.max(7, Number(flags["clv-window-days"] ?? 60));
  const clvMinSamples = Math.max(10, Number(flags["clv-min-samples"] ?? 40));
  const clvMinEdge = Number(flags["clv-min-edge"] ?? -0.005);
  const windowDays = [30, 60, 90];

  console.log("\n=== Monitor Pattern Decay ===\n");
  const deployedPatterns = await prisma.$queryRawUnsafe<
    Array<{ id: string; outcomeType: string }>
  >(
    `SELECT "id","outcomeType" FROM "PatternV2" WHERE "status" = 'deployed'`,
  );
  if (deployedPatterns.length === 0) {
    console.log("No deployed patterns to evaluate.");
    return;
  }

  const rows = await prisma.$queryRawUnsafe<
    Array<{ patternId: string; date: Date; hitBool: boolean }>
  >(
    `SELECT h."patternId", h."date", h."hitBool"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     WHERE p."status" = 'deployed'`,
  );

  const now = new Date();
  const startedAt = Date.now();
  const tokenizedGames = await loadTokenizedGames();
  const outcomeRatesByWindow = new Map<number, Map<string, number>>();
  for (const days of windowDays) {
    const minDate = new Date(now.getTime() - days * 86_400_000);
    const slice = tokenizedGames.filter((g) => g.date >= minDate);
    const total = Math.max(1, slice.length);
    const counts = new Map<string, number>();
    for (const g of slice) {
      for (const outcome of g.outcomes) {
        counts.set(outcome, (counts.get(outcome) ?? 0) + 1);
      }
    }
    const rates = new Map<string, number>();
    for (const [outcome, count] of counts) {
      rates.set(outcome, count / total);
    }
    outcomeRatesByWindow.set(days, rates);
  }

  const byPattern = new Map<string, Array<{ date: Date; hit: boolean }>>();
  for (const r of rows) {
    const arr = byPattern.get(r.patternId) ?? [];
    arr.push({ date: new Date(r.date), hit: r.hitBool });
    byPattern.set(r.patternId, arr);
  }

  let retired = 0;
  let processed = 0;
  const clvBadOutcomes = new Set<string>();
  if (clvFeedback) {
    try {
      const clvRows = await prisma.$queryRawUnsafe<
        Array<{ outcomeType: string; samples: number; avgClv: number }>
      >(
        `SELECT l."outcomeType" as "outcomeType",
                COUNT(*)::int as "samples",
                AVG(COALESCE(l."clvDeltaProb", l."modelEdge", 0))::float8 as "avgClv"
         FROM "SuggestedPlayLedger" l
         WHERE l."isActionable" = TRUE
           AND l."settledResult" IN ('HIT','MISS')
           AND l."date" >= NOW()::date - INTERVAL '${clvWindowDays} days'
         GROUP BY l."outcomeType"`,
      );
      for (const r of clvRows) {
        if ((r.samples ?? 0) >= clvMinSamples && (r.avgClv ?? 0) < clvMinEdge) {
          clvBadOutcomes.add(r.outcomeType);
        }
      }
      if (clvRows.length > 0) {
        console.log(
          `CLV feedback: scanned ${clvRows.length} outcomes, suppressing ${clvBadOutcomes.size} under clv<${clvMinEdge} with n>=${clvMinSamples}`,
        );
      }
    } catch {
      console.log("CLV feedback skipped (SuggestedPlayLedger unavailable).");
    }
  }
  const totalPatterns = deployedPatterns.length;
  for (const { id: patternId, outcomeType } of deployedPatterns) {
    processed++;
    const hits = byPattern.get(patternId) ?? [];
    const windows = [30, 60, 90].map((days) => {
      const minDate = new Date(now.getTime() - days * 86_400_000);
      const slice = hits.filter((h) => h.date >= minDate);
      const n = slice.length;
      const wins = slice.filter((h) => h.hit).length;
      const baselineRate = clampProb(
        outcomeRatesByWindow.get(days)?.get(outcomeType) ?? 0.5,
      );
      const alpha = baselineRate * priorStrength;
      const beta = (1 - baselineRate) * priorStrength;
      const posterior = (wins + alpha) / (n + alpha + beta);
      return { days, n, edge: posterior - baselineRate };
    });

    const collapseByDecay = windows.some((w) => w.n >= minSamples && w.edge <= collapseEdge);
    const collapseByClv = clvBadOutcomes.has(outcomeType);
    const collapse = collapseByDecay || collapseByClv;
    if (collapse) {
      await prisma.$executeRawUnsafe(
        `UPDATE "PatternV2" SET "status" = 'retired', "retiredAt" = NOW(), "updatedAt" = NOW() WHERE "id" = '${sqlEsc(patternId)}'`,
      );
      retired++;
    }
    if (processed % 25 === 0 || processed === totalPatterns) {
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(`Decay progress: ${processed}/${totalPatterns} patterns checked (${retired} retired, ${elapsedSec}s elapsed)`);
    }
  }

  if (retired > 0) {
    await rebuildPatternV2HitsForDeployed();
  }
  console.log(`Auto-retired ${retired} patterns due to decay.`);
}

export async function analyzePatternsV2(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const status = flags.status ?? "deployed";
  const topN = Math.max(3, Number(flags["top-n"] ?? 15));
  const where =
    status === "all" ? "" : `WHERE "status" = '${sqlEsc(status)}'`;

  const rows = await prisma.$queryRawUnsafe<
    Array<{ id: string; outcomeType: string; conditions: string[]; status: string }>
  >(
    `SELECT "id","outcomeType","conditions","status" FROM "PatternV2" ${where}`,
  );

  console.log("\n=== Analyze Patterns v2 ===\n");
  console.log(`Scope: status=${status} | rows=${rows.length}`);
  if (rows.length === 0) return;

  const byOutcome = new Map<string, number>();
  const byFamily = new Map<string, number>();
  const byToken = new Map<string, number>();
  for (const r of rows) {
    byOutcome.set(r.outcomeType, (byOutcome.get(r.outcomeType) ?? 0) + 1);
    const family = outcomeFamily(r.outcomeType);
    byFamily.set(family, (byFamily.get(family) ?? 0) + 1);
    for (const c of r.conditions ?? []) {
      byToken.set(c, (byToken.get(c) ?? 0) + 1);
    }
  }

  const outcomeRanks = [...byOutcome.entries()].sort((a, b) => b[1] - a[1]);
  const familyRanks = [...byFamily.entries()].sort((a, b) => b[1] - a[1]);
  const tokenRanks = [...byToken.entries()].sort((a, b) => b[1] - a[1]);

  const topOutcomeShare = outcomeRanks[0] ? (outcomeRanks[0][1] / rows.length) * 100 : 0;
  const topFamilyShare = familyRanks[0] ? (familyRanks[0][1] / rows.length) * 100 : 0;

  console.log(`Top outcome concentration: ${topOutcomeShare.toFixed(1)}% (${outcomeRanks[0]?.[0] ?? "n/a"})`);
  console.log(`Top family concentration: ${topFamilyShare.toFixed(1)}% (${familyRanks[0]?.[0] ?? "n/a"})`);

  console.log(`\nTop ${Math.min(topN, outcomeRanks.length)} outcomes:`);
  for (const [outcome, count] of outcomeRanks.slice(0, topN)) {
    console.log(`  ${outcome}: ${count}`);
  }

  console.log(`\nTop ${Math.min(topN, familyRanks.length)} families:`);
  for (const [family, count] of familyRanks.slice(0, topN)) {
    console.log(`  ${family}: ${count}`);
  }

  console.log(`\nTop ${Math.min(topN, tokenRanks.length)} condition tokens:`);
  for (const [token, count] of tokenRanks.slice(0, topN)) {
    console.log(`  ${token}: ${count}`);
  }
}

export async function analyzeV2Bankroll(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const status = flags.status ?? "deployed";
  const stake = Number(flags.stake ?? 10);
  const odds = Number(flags.odds ?? -110);
  const startingBankroll = Number(flags.bankroll ?? 1000);
  const from = flags.from;
  const to = flags.to;
  const season = flags.season ? Number(flags.season) : null;

  const where: string[] = [];
  if (status !== "all") where.push(`p."status" = '${sqlEsc(status)}'`);
  if (from) where.push(`h."date" >= '${sqlEsc(from)}'`);
  if (to) where.push(`h."date" <= '${sqlEsc(to)}'`);
  if (season != null) where.push(`g."season" = ${season}`);
  const whereSql = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

  const rows = await prisma.$queryRawUnsafe<Array<{ date: Date; hitBool: boolean; season: number }>>(
    `SELECT h."date" as "date", h."hitBool" as "hitBool", g."season" as "season"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     JOIN "Game" g ON g."id" = h."gameId"
     ${whereSql}
     ORDER BY h."date" ASC`,
  );

  console.log("\n=== Pattern V2 Bankroll Backtest ===\n");
  console.log(
    `Scope: status=${status}, stake=$${stake.toFixed(2)}, odds=${odds}, start=$${startingBankroll.toFixed(2)}` +
      `${from ? `, from=${from}` : ""}${to ? `, to=${to}` : ""}${season != null ? `, season=${season}` : ""}`,
  );
  console.log(`Rows: ${rows.length}`);
  if (rows.length === 0) return;

  const decimal = americanToDecimal(odds);
  let bankroll = startingBankroll;
  let peak = bankroll;
  let maxDrawdown = 0;
  let wins = 0;
  let losses = 0;

  const bySeason = new Map<number, { n: number; wins: number; pnl: number }>();
  for (const row of rows) {
    const profit = row.hitBool ? stake * (decimal - 1) : -stake;
    bankroll += profit;
    if (row.hitBool) wins++;
    else losses++;
    peak = Math.max(peak, bankroll);
    if (peak > 0) {
      maxDrawdown = Math.max(maxDrawdown, (peak - bankroll) / peak);
    }
    const seasonAgg = bySeason.get(row.season) ?? { n: 0, wins: 0, pnl: 0 };
    seasonAgg.n += 1;
    seasonAgg.wins += row.hitBool ? 1 : 0;
    seasonAgg.pnl += profit;
    bySeason.set(row.season, seasonAgg);
  }

  const bets = wins + losses;
  const hitRate = bets > 0 ? wins / bets : 0;
  const totalWagered = bets * stake;
  const netPnL = bankroll - startingBankroll;
  const roi = totalWagered > 0 ? netPnL / totalWagered : 0;

  console.log(`Hit rate: ${(hitRate * 100).toFixed(2)}% (${wins}/${bets})`);
  console.log(`Net P&L: $${netPnL.toFixed(2)}`);
  console.log(`Final bankroll: $${bankroll.toFixed(2)}`);
  console.log(`ROI on amount wagered: ${(roi * 100).toFixed(2)}%`);
  console.log(`Max drawdown: ${(maxDrawdown * 100).toFixed(2)}%`);

  const seasonKeys = [...bySeason.keys()].sort((a, b) => a - b);
  if (seasonKeys.length > 0) {
    console.log("\nBy season:");
    for (const s of seasonKeys) {
      const agg = bySeason.get(s);
      if (!agg) continue;
      const sHitRate = agg.n > 0 ? agg.wins / agg.n : 0;
      const sRoi = agg.n > 0 ? agg.pnl / (agg.n * stake) : 0;
      console.log(
        `  ${s}: ${(sHitRate * 100).toFixed(2)}% (${agg.wins}/${agg.n}), pnl=$${agg.pnl.toFixed(2)}, roi=${(sRoi * 100).toFixed(2)}%`,
      );
    }
  }
}

export async function evaluatePatternsV2Purged(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const folds = Math.max(3, Number(flags.folds ?? 6));
  const embargoDays = Math.max(0, Number(flags["embargo-days"] ?? 7));
  const minTrainSamples = Math.max(20, Number(flags["min-train-samples"] ?? 80));
  const minTestSamples = Math.max(10, Number(flags["min-test-samples"] ?? 25));

  const rows = await prisma.$queryRawUnsafe<
    Array<{ patternId: string; outcomeType: string; date: Date; hitBool: boolean }>
  >(
    `SELECT h."patternId" as "patternId",
            p."outcomeType" as "outcomeType",
            h."date" as "date",
            h."hitBool" as "hitBool"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     WHERE p."status" = 'deployed'`,
  );
  if (rows.length === 0) {
    console.log("No deployed PatternV2Hit rows found.");
    return;
  }
  const timestamps = rows.map((r) => new Date(r.date).getTime()).sort((a, b) => a - b);
  const minTs = timestamps[0] ?? Date.now();
  const maxTs = timestamps[timestamps.length - 1] ?? minTs + 1;
  const span = Math.max(1, maxTs - minTs);
  const embargoMs = embargoDays * 86_400_000;
  const byPattern = new Map<string, Array<{ ts: number; hit: boolean; outcomeType: string }>>();
  for (const row of rows) {
    const arr = byPattern.get(row.patternId) ?? [];
    arr.push({
      ts: new Date(row.date).getTime(),
      hit: row.hitBool,
      outcomeType: row.outcomeType,
    });
    byPattern.set(row.patternId, arr);
  }

  let usedFolds = 0;
  let aggTrainN = 0;
  let aggTestN = 0;
  let aggTrainHits = 0;
  let aggTestHits = 0;
  for (let i = 0; i < folds; i++) {
    const foldStart = minTs + Math.floor((span * i) / folds);
    const foldEnd = minTs + Math.floor((span * (i + 1)) / folds);
    let foldTrainN = 0;
    let foldTestN = 0;
    let foldTrainHits = 0;
    let foldTestHits = 0;
    for (const entries of byPattern.values()) {
      const train = entries.filter((e) => e.ts < foldStart - embargoMs);
      const test = entries.filter((e) => e.ts >= foldStart && e.ts < foldEnd);
      if (train.length < minTrainSamples || test.length < minTestSamples) continue;
      const trainHits = train.filter((e) => e.hit).length;
      const testHits = test.filter((e) => e.hit).length;
      foldTrainN += train.length;
      foldTestN += test.length;
      foldTrainHits += trainHits;
      foldTestHits += testHits;
    }
    if (foldTrainN === 0 || foldTestN === 0) continue;
    usedFolds++;
    aggTrainN += foldTrainN;
    aggTestN += foldTestN;
    aggTrainHits += foldTrainHits;
    aggTestHits += foldTestHits;
    const trainRate = foldTrainHits / foldTrainN;
    const testRate = foldTestHits / foldTestN;
    console.log(
      `Fold ${i + 1}/${folds} | train=${foldTrainN} test=${foldTestN} | trainHit=${(trainRate * 100).toFixed(2)}% testHit=${(testRate * 100).toFixed(2)}% delta=${((testRate - trainRate) * 100).toFixed(2)}pp`,
    );
  }

  console.log("\n=== Pattern V2 Purged/Embargo Evaluation ===");
  console.log(`usedFolds=${usedFolds}/${folds}, embargoDays=${embargoDays}`);
  if (aggTrainN > 0 && aggTestN > 0) {
    const trainRate = aggTrainHits / aggTrainN;
    const testRate = aggTestHits / aggTestN;
    console.log(
      `aggregate trainHit=${(trainRate * 100).toFixed(2)}% (n=${aggTrainN}) | testHit=${(testRate * 100).toFixed(2)}% (n=${aggTestN}) | delta=${((testRate - trainRate) * 100).toFixed(2)}pp`,
    );
  }
}

export async function getDiscoveryV2MatchesForGameIds(gameIds: string[]): Promise<
  Map<string, Array<{ patternId: string; outcomeType: string; conditions: string[]; posteriorHitRate: number; edge: number; score: number; n: number }>>
> {
  if (gameIds.length === 0) return new Map();
  const inClause = gameIds.map((id) => `'${sqlEsc(id)}'`).join(",");
  const tokenRows = await prisma.$queryRawUnsafe<Array<{ gameId: string; tokens: string[] }>>(
    `SELECT "gameId", "tokens" FROM "GameFeatureToken" WHERE "gameId" IN (${inClause})`,
  );
  const patternRows = await prisma.$queryRawUnsafe<
    Array<{
      id: string;
      outcomeType: string;
      conditions: string[];
      posteriorHitRate: number;
      edge: number;
      score: number;
      n: number;
    }>
  >(
    `SELECT "id","outcomeType","conditions","posteriorHitRate","edge","score","n"
     FROM "PatternV2"
     WHERE "status" = 'deployed'
     ORDER BY "score" DESC`,
  );

  const tokensByGame = new Map(tokenRows.map((r) => [r.gameId, new Set(r.tokens ?? [])]));
  const out = new Map<string, Array<{ patternId: string; outcomeType: string; conditions: string[]; posteriorHitRate: number; edge: number; score: number; n: number }>>();
  for (const gid of gameIds) out.set(gid, []);

  for (const p of patternRows) {
    for (const gid of gameIds) {
      const tokenSet = tokensByGame.get(gid);
      if (!tokenSet) continue;
      if (!matchesConditions(tokenSet, p.conditions ?? [])) continue;
      out.get(gid)?.push({
        patternId: p.id,
        outcomeType: p.outcomeType,
        conditions: p.conditions ?? [],
        posteriorHitRate: p.posteriorHitRate,
        edge: p.edge,
        score: p.score,
        n: p.n,
      });
    }
  }

  for (const gid of gameIds) {
    const list = out.get(gid) ?? [];
    list.sort((a, b) => b.score - a.score || b.edge - a.edge);
    out.set(gid, list.slice(0, 8));
  }
  return out;
}
