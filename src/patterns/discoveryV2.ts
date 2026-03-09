import { prisma } from "../db/prisma.js";

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

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
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

  let upserts = 0;
  for (const row of rows) {
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
    await prisma.$executeRawUnsafe(
      `INSERT INTO "GameFeatureToken" ("id","gameId","season","date","tokens","createdAt")
       VALUES ('${crypto.randomUUID()}','${sqlEsc(row.id)}',${row.season},'${row.date.toISOString().slice(0, 10)}','${arrayLiteral}',NOW())
       ON CONFLICT ("gameId")
       DO UPDATE SET "season" = EXCLUDED."season", "date" = EXCLUDED."date", "tokens" = EXCLUDED."tokens"`,
    );
    upserts++;
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

function computeStats(
  rows: TokenizedGame[],
  outcomeType: string,
  conditions: string[],
  baselineRate: number,
  priorStrength: number,
): SplitStats {
  let n = 0;
  let wins = 0;
  for (const r of rows) {
    if (!matchesConditions(r.tokens, conditions)) continue;
    n++;
    if (r.outcomes.has(outcomeType)) wins++;
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
): SplitStats {
  if (rows.length === 0) {
    return computeStats(rows, outcomeType, conditions, baselineRate, priorStrength);
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

function splitRowsBySeason(rows: TokenizedGame[], trainSeason: number, valSeason: number, forwardFromSeason: number) {
  const train = rows.filter((r) => r.season === trainSeason);
  const val = rows.filter((r) => r.season === valSeason);
  const forward = rows.filter((r) => r.season >= forwardFromSeason);
  return { train, val, forward };
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
  const trainSeason = Number(flags["train-season"] ?? 2023);
  const valSeason = Number(flags["val-season"] ?? 2024);
  const forwardFrom = Number(flags["forward-from"] ?? 2025);
  const maxDepth = Number(flags["tree-depth"] ?? 3);
  const minLeaf = Number(flags["min-samples"] ?? 50);
  const minTokenSupport = Number(flags["min-token-support"] ?? 40);
  const minOutcomeSamples = Number(flags["min-outcome-samples"] ?? 60);
  const maxItemset = Number(flags["max-itemset-size"] ?? 3);
  const minSupport = Number(flags["min-support"] ?? 50);
  const maxPatterns = Number(flags["max-patterns"] ?? 1000);
  const minConditionCount = Math.max(1, Number(flags["min-condition-count"] ?? 2));
  const minDistinctFeatureCount = Math.max(1, Number(flags["min-distinct-features"] ?? 2));
  const maxPerOutcome = Math.max(1, Number(flags["max-per-outcome"] ?? 80));
  const maxPerFamily = Math.max(1, Number(flags["max-per-family"] ?? 180));
  const maxTrainCoverage = Math.min(1, Math.max(0.01, Number(flags["max-train-coverage"] ?? 0.35)));
  const minOutcomeTrainSeasons = Math.max(1, Number(flags["min-outcome-train-seasons"] ?? 2));
  const minOutcomeTrainCoverage = Math.max(0, Number(flags["min-outcome-train-coverage"] ?? 0.005));
  const maxOutcomeTrainCoverage = Math.min(1, Math.max(0.01, Number(flags["max-outcome-train-coverage"] ?? 0.8)));
  const minValSamples = Math.max(1, Number(flags["min-val-samples"] ?? 15));
  const minForwardSamples = Math.max(1, Number(flags["min-forward-samples"] ?? 20));
  const minValEdge = Number(flags["min-val-edge"] ?? 0);
  const minForwardEdgeDiscovery = Number(flags["min-forward-edge-discovery"] ?? 0);
  const maxConditionOverlap = Math.min(0.99, Math.max(0, Number(flags["max-condition-overlap"] ?? 0.8)));
  const priorStrength = Number(flags["prior-strength"] ?? 25);
  const recencyHalfLifeDays = Math.max(7, Number(flags["recency-half-life-days"] ?? 90));
  const lowNPenaltyK = Math.max(1, Number(flags["low-n-penalty-k"] ?? 120));

  console.log("\n=== Discover Patterns v2 ===\n");
  const games = await loadTokenizedGames();
  const splits = splitRowsBySeason(games, trainSeason, valSeason, forwardFrom);
  if (splits.train.length === 0 || splits.val.length === 0 || splits.forward.length === 0) {
    throw new Error("Insufficient split coverage. Make sure train/val/forward seasons have quantized games.");
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
    const valBase =
      splits.val.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.val.length);
    const forwardBase =
      splits.forward.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.forward.length);

    const train = computeStats(splits.train, c.outcomeType, c.conditions, trainBase, priorStrength);
    if (train.n < minLeaf) continue;
    const trainRecency = computeWeightedStats(
      splits.train,
      c.outcomeType,
      c.conditions,
      trainBase,
      priorStrength,
      recencyHalfLifeDays,
    );
    const val = computeStats(splits.val, c.outcomeType, c.conditions, valBase, priorStrength);
    const forward = computeStats(splits.forward, c.outcomeType, c.conditions, forwardBase, priorStrength);
    if (val.n < minValSamples) continue;
    if (forward.n < minForwardSamples) continue;
    if (val.edge < minValEdge) continue;
    if (forward.edge < minForwardEdgeDiscovery) continue;

    const trainCoverage = train.n / Math.max(1, splits.train.length);
    if (trainCoverage > maxTrainCoverage) continue;

    const stabilityFactor =
      (val.edge > 0 ? 0.6 : 0.2) +
      (forward.edge > 0 ? 0.5 : 0.1) +
      (Math.sign(train.edge) === Math.sign(val.edge) ? 0.2 : 0);

    // Prefer specific patterns over broad "always-on" rules.
    const specificityFactor = Math.sqrt(Math.max(0.01, 1 - trainCoverage));
    const uncertaintyPenalty = Math.sqrt(trainRecency.n / (trainRecency.n + lowNPenaltyK));
    const portability = portabilitySignals(c.outcomeType, c.conditions ?? []);
    const fragilityPenalty = 1 - portability.dependencyRisk * 0.25;
    const portabilityFactor = 0.7 + portability.portabilityScore * 0.6;
    const score =
      trainRecency.edge *
      Math.log(Math.max(2, trainRecency.n)) *
      stabilityFactor *
      specificityFactor *
      uncertaintyPenalty *
      portabilityFactor *
      fragilityPenalty;
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
  const perFamily = new Map<string, number>();
  const selectedByOutcome = new Map<string, ScoredPattern[]>();
  for (const p of scored) {
    const used = perOutcome.get(p.outcomeType) ?? 0;
    if (used >= maxPerOutcome) continue;
    const family = outcomeFamily(p.outcomeType);
    const familyUsed = perFamily.get(family) ?? 0;
    if (familyUsed >= maxPerFamily) continue;
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
  console.log(
    `Scored ${scored.length} patterns. Stored top ${top.length} into PatternV2 (outcome filters: min samples=${minOutcomeSamples}, seasons>=${minOutcomeTrainSeasons}, coverage ${minOutcomeTrainCoverage}-${maxOutcomeTrainCoverage}; candidate filters: min conditions=${minConditionCount}, min distinct features=${minDistinctFeatureCount}, max/outcome=${maxPerOutcome}, max/family=${maxPerFamily}, max train coverage=${maxTrainCoverage}, min val samples=${minValSamples}, min forward samples=${minForwardSamples}, min val edge=${minValEdge}, min forward edge=${minForwardEdgeDiscovery}, recency half-life=${recencyHalfLifeDays}d, low-n penalty k=${lowNPenaltyK}, max overlap=${maxConditionOverlap}, outcomes=${perOutcome.size}, families=${perFamily.size}).`,
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

  let inserted = 0;
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    let matchedForPattern = 0;
    for (const g of games) {
      if (!matchesConditions(g.tokens, p.conditions ?? [])) continue;
      const hit = g.outcomes.has(p.outcomeType);
      matchedForPattern++;
      await prisma.$executeRawUnsafe(
        `INSERT INTO "PatternV2Hit" ("id","patternId","gameId","hitBool","date")
         VALUES ('${crypto.randomUUID()}','${sqlEsc(p.id)}','${sqlEsc(g.gameId)}',${hit ? "TRUE" : "FALSE"},'${g.date.toISOString().slice(0, 10)}')
         ON CONFLICT ("patternId","gameId") DO NOTHING`,
      );
      inserted++;
      if (inserted % progressEveryInsert === 0) {
        const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
        console.log(`  Hit rebuild inserts: ${inserted} (${elapsedSec}s elapsed)`);
      }
    }
    if ((i + 1) % progressEveryPattern === 0 || i + 1 === patterns.length) {
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(
        `  Pattern ${i + 1}/${patterns.length} processed (${matchedForPattern} matches, ${inserted} inserts total, ${elapsedSec}s elapsed)`,
      );
    }
  }
  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`Rebuilt ${inserted} PatternV2Hit rows in ${elapsedSec}s.`);
}

export async function validatePatternsV2(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const minTrainEdge = Number(flags["min-train-edge"] ?? 0.015);
  const minValEdge = Number(flags["min-val-edge"] ?? 0.008);
  const minForwardEdge = Number(flags["min-forward-edge"] ?? 0.005);
  const minValPosterior = Number(flags["min-val-posterior"] ?? 0.52);
  const minForwardPosterior = Number(flags["min-forward-posterior"] ?? 0.515);
  const minTrainSamples = Math.max(1, Number(flags["min-train-samples"] ?? 80));
  const minValSamples = Math.max(1, Number(flags["min-val-samples"] ?? 25));
  const minForwardSamples = Math.max(1, Number(flags["min-forward-samples"] ?? 30));
  const uncertaintyPenaltyK = Math.max(1, Number(flags["uncertainty-penalty-k"] ?? 120));
  const uncertaintyPenaltyScale = Math.max(0, Number(flags["uncertainty-penalty-scale"] ?? 0.35));
  const fdrAlpha = Math.max(0, Math.min(1, Number(flags["fdr-alpha"] ?? 0.1)));
  const runDecay = (flags["run-decay"] ?? "true") !== "false";
  const decayMinWindowSamples = Math.max(1, Number(flags["decay-min-window-samples"] ?? 25));
  const decayCollapseEdge = Number(flags["decay-collapse-edge"] ?? -0.005);
  const progressEvery = Math.max(1, Number(flags["progress-every"] ?? 50));
  const startedAt = Date.now();

  console.log("\n=== Validate Patterns v2 ===\n");
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
    const trainOk =
      (p.trainStats.n ?? 0) >= minTrainSamples &&
      trainPenalizedEdge >= minTrainEdge;
    const valOk =
      (p.valStats.n ?? 0) >= minValSamples &&
      valPenalizedEdge >= minValEdge &&
      (p.valStats.posteriorHitRate ?? 0) >= minValPosterior;
    const forwardOk =
      (p.forwardStats.n ?? 0) >= minForwardSamples &&
      forwardPenalizedEdge >= minForwardEdge &&
      (p.forwardStats.posteriorHitRate ?? 0) >= minForwardPosterior;

    const pVal = oneSidedPValueFromStats(p.valStats);
    const pForward = oneSidedPValueFromStats(p.forwardStats);
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

  for (let i = 0; i < evalRows.length; i++) {
    const row = evalRows[i];
    let nextStatus = "candidate";
    if (row.trainOk && row.valOk && row.forwardOk && fdrPass.has(row.id)) {
      nextStatus = "deployed";
      deployed++;
    } else if (row.trainOk && row.valOk) {
      nextStatus = "validated";
      validated++;
    } else if (row.status === "deployed" || row.status === "validated") {
      nextStatus = "retired";
      retired++;
    } else {
      candidates++;
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
  console.log(
    `Thresholds: train(penalized edge>=${minTrainEdge}, n>=${minTrainSamples}) | val(penalized edge>=${minValEdge}, posterior>=${minValPosterior}, n>=${minValSamples}) | forward(penalized edge>=${minForwardEdge}, posterior>=${minForwardPosterior}, n>=${minForwardSamples}) | uncertainty k=${uncertaintyPenaltyK}`,
  );
  console.log(
    `Uncertainty penalty: edge -= ${uncertaintyPenaltyScale}/sqrt(n + ${uncertaintyPenaltyK})`,
  );
  console.log(
    `FDR gate: alpha=${fdrAlpha.toFixed(3)} | eligible=${fdrEligible.length} | passed=${fdrPass.size}`,
  );
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
        Array<{ outcomeType: string; samples: number; avgEdge: number }>
      >(
        `SELECT l."outcomeType" as "outcomeType",
                COUNT(*)::int as "samples",
                AVG(COALESCE(l."modelEdge",0))::float8 as "avgEdge"
         FROM "SuggestedPlayLedger" l
         WHERE l."isActionable" = TRUE
           AND l."settledResult" IN ('HIT','MISS')
           AND l."date" >= NOW()::date - INTERVAL '${clvWindowDays} days'
         GROUP BY l."outcomeType"`,
      );
      for (const r of clvRows) {
        if ((r.samples ?? 0) >= clvMinSamples && (r.avgEdge ?? 0) < clvMinEdge) {
          clvBadOutcomes.add(r.outcomeType);
        }
      }
      if (clvRows.length > 0) {
        console.log(
          `CLV feedback: scanned ${clvRows.length} outcomes, suppressing ${clvBadOutcomes.size} under edge<${clvMinEdge} with n>=${clvMinSamples}`,
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
