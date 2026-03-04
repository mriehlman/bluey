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
    },
  });
}

function getConsensusOdds(row: GameWithContextAndOdds) {
  return row.odds.find((o) => o.source === "consensus") ?? row.odds[0] ?? null;
}

function featureExtractors(): Record<string, FeatureExtractor> {
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

function computeStats(
  rows: TokenizedGame[],
  outcomeType: string,
  conditions: string[],
  baselineRate: number,
  alpha: number,
  beta: number,
): SplitStats {
  let n = 0;
  let wins = 0;
  for (const r of rows) {
    if (!matchesConditions(r.tokens, conditions)) continue;
    n++;
    if (r.outcomes.has(outcomeType)) wins++;
  }
  const rawHitRate = n > 0 ? wins / n : 0;
  const posteriorHitRate = (wins + alpha) / (n + alpha + beta);
  const edge = posteriorHitRate - 0.524;
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
  const priorStrength = Number(flags["prior-strength"] ?? 25);

  console.log("\n=== Discover Patterns v2 ===\n");
  const games = await loadTokenizedGames();
  const splits = splitRowsBySeason(games, trainSeason, valSeason, forwardFrom);
  if (splits.train.length === 0 || splits.val.length === 0 || splits.forward.length === 0) {
    throw new Error("Insufficient split coverage. Make sure train/val/forward seasons have quantized games.");
  }

  const outcomes = new Map<string, number>();
  for (const row of splits.train) {
    for (const o of row.outcomes) outcomes.set(o, (outcomes.get(o) ?? 0) + 1);
  }
  const outcomeTypes = [...outcomes.entries()]
    .filter(([, n]) => n >= minOutcomeSamples)
    .map(([o]) => o)
    .sort();

  const alpha = 0.524 * priorStrength;
  const beta = (1 - 0.524) * priorStrength;

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
    const trainBase =
      splits.train.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.train.length);
    const valBase =
      splits.val.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.val.length);
    const forwardBase =
      splits.forward.filter((r) => r.outcomes.has(c.outcomeType)).length / Math.max(1, splits.forward.length);

    const train = computeStats(splits.train, c.outcomeType, c.conditions, trainBase, alpha, beta);
    if (train.n < minLeaf) continue;
    const val = computeStats(splits.val, c.outcomeType, c.conditions, valBase, alpha, beta);
    const forward = computeStats(splits.forward, c.outcomeType, c.conditions, forwardBase, alpha, beta);

    const stabilityFactor =
      (val.edge > 0 ? 0.6 : 0.2) +
      (forward.edge > 0 ? 0.5 : 0.1) +
      (Math.sign(train.edge) === Math.sign(val.edge) ? 0.2 : 0);

    const score = train.edge * Math.log(Math.max(2, train.n)) * stabilityFactor;
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
    });
  }

  scored.sort((a, b) => b.score - a.score || b.edge - a.edge || b.n - a.n);
  const top = scored.slice(0, maxPatterns);

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
  console.log(`Scored ${scored.length} patterns. Stored top ${top.length} into PatternV2.`);
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

async function rebuildPatternV2HitsForDeployed(): Promise<void> {
  const patterns = await prisma.$queryRawUnsafe<Array<{ id: string; outcomeType: string; conditions: string[] }>>(
    `SELECT "id","outcomeType","conditions" FROM "PatternV2" WHERE "status" = 'deployed'`,
  );
  if (patterns.length === 0) {
    await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2Hit"`);
    return;
  }
  const games = await loadTokenizedGames();
  await prisma.$executeRawUnsafe(`DELETE FROM "PatternV2Hit"`);

  let inserted = 0;
  for (const p of patterns) {
    for (const g of games) {
      if (!matchesConditions(g.tokens, p.conditions ?? [])) continue;
      const hit = g.outcomes.has(p.outcomeType);
      await prisma.$executeRawUnsafe(
        `INSERT INTO "PatternV2Hit" ("id","patternId","gameId","hitBool","date")
         VALUES ('${crypto.randomUUID()}','${sqlEsc(p.id)}','${sqlEsc(g.gameId)}',${hit ? "TRUE" : "FALSE"},'${g.date.toISOString().slice(0, 10)}')
         ON CONFLICT ("patternId","gameId") DO NOTHING`,
      );
      inserted++;
    }
  }
  console.log(`Rebuilt ${inserted} PatternV2Hit rows.`);
}

export async function validatePatternsV2(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const minTrainEdge = Number(flags["min-train-edge"] ?? 0.01);
  const minForwardEdge = Number(flags["min-forward-edge"] ?? 0);

  console.log("\n=== Validate Patterns v2 ===\n");
  const patterns = await loadStoredPatternsV2();
  let deployed = 0;
  let validated = 0;
  let retired = 0;
  let candidates = 0;

  for (const p of patterns) {
    const trainOk = (p.trainStats.edge ?? -1) > minTrainEdge;
    const valOk = (p.valStats.edge ?? -1) > 0;
    const forwardOk = (p.forwardStats.edge ?? -1) >= minForwardEdge;

    let nextStatus = "candidate";
    if (trainOk && valOk && forwardOk) {
      nextStatus = "deployed";
      deployed++;
    } else if (trainOk && valOk) {
      nextStatus = "validated";
      validated++;
    } else if (p.status === "deployed" || p.status === "validated") {
      nextStatus = "retired";
      retired++;
    } else {
      candidates++;
    }

    const retiredAt = nextStatus === "retired" ? "NOW()" : "NULL";
    await prisma.$executeRawUnsafe(
      `UPDATE "PatternV2"
       SET "status" = '${nextStatus}', "retiredAt" = ${retiredAt}, "updatedAt" = NOW()
       WHERE "id" = '${sqlEsc(p.id)}'`,
    );
  }

  await rebuildPatternV2HitsForDeployed();
  console.log(`Status counts: deployed=${deployed}, validated=${validated}, retired=${retired}, candidate=${candidates}`);
}

export async function monitorPatternDecay(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const minSamples = Number(flags["min-window-samples"] ?? 20);
  const collapseEdge = Number(flags["collapse-edge"] ?? -0.01);
  const priorStrength = Number(flags["prior-strength"] ?? 25);
  const alpha = 0.524 * priorStrength;
  const beta = (1 - 0.524) * priorStrength;

  console.log("\n=== Monitor Pattern Decay ===\n");
  const rows = await prisma.$queryRawUnsafe<
    Array<{ patternId: string; date: Date; hitBool: boolean }>
  >(
    `SELECT h."patternId", h."date", h."hitBool"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     WHERE p."status" = 'deployed'`,
  );

  const now = new Date();
  const byPattern = new Map<string, Array<{ date: Date; hit: boolean }>>();
  for (const r of rows) {
    const arr = byPattern.get(r.patternId) ?? [];
    arr.push({ date: new Date(r.date), hit: r.hitBool });
    byPattern.set(r.patternId, arr);
  }

  let retired = 0;
  for (const [patternId, hits] of byPattern) {
    const windows = [30, 60, 90].map((days) => {
      const minDate = new Date(now.getTime() - days * 86_400_000);
      const slice = hits.filter((h) => h.date >= minDate);
      const n = slice.length;
      const wins = slice.filter((h) => h.hit).length;
      const posterior = (wins + alpha) / (n + alpha + beta);
      return { days, n, edge: posterior - 0.524 };
    });

    const collapse = windows.some((w) => w.n >= minSamples && w.edge <= collapseEdge);
    if (collapse) {
      await prisma.$executeRawUnsafe(
        `UPDATE "PatternV2" SET "status" = 'retired', "retiredAt" = NOW(), "updatedAt" = NOW() WHERE "id" = '${sqlEsc(patternId)}'`,
      );
      retired++;
    }
  }

  if (retired > 0) {
    await rebuildPatternV2HitsForDeployed();
  }
  console.log(`Auto-retired ${retired} patterns due to decay.`);
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
