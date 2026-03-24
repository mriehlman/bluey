import crypto from "node:crypto";
import { mkdir, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { prisma } from "@bluey/db";
import { deriveLaneTag, ensurePickQualityTables, loadSourceReliabilitySnapshot, sourceFamilyFromModelId } from "./pickQuality";
import { predictGames } from "./predictGames";
import { laneKeyFromTags, normalizeVoteSource, normalizeVoteSourceFromModelId, reliabilityWeightFromHitRate } from "./voteWeighting";
import type { ModelVote } from "./predictionContract";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        flags[k] = next;
        i++;
      } else {
        flags[k] = "true";
      }
    }
  }
  return flags;
}

function sqlEsc(v: string): string {
  return v.replaceAll("'", "''");
}

function marketFamilySqlExpr(): string {
  return `CASE
    WHEN COALESCE(l."market",'') LIKE 'moneyline%' THEN 'moneyline'
    WHEN COALESCE(l."market",'') LIKE 'spread%' THEN 'spread'
    WHEN COALESCE(l."market",'') LIKE 'total%' THEN 'total'
    WHEN COALESCE(l."market",'') LIKE 'player_%' THEN 'player_prop'
    WHEN l."outcomeType" LIKE '%WIN%' THEN 'moneyline'
    WHEN l."outcomeType" LIKE '%COVERED%' THEN 'spread'
    WHEN l."outcomeType" LIKE '%OVER%' OR l."outcomeType" LIKE '%UNDER%' THEN 'total'
    WHEN l."outcomeType" LIKE '%PLAYER%' OR l."outcomeType" LIKE '%TOP_%' THEN 'player_prop'
    ELSE 'other'
  END`;
}

async function ensureSuggestedPlayLedgerQualityColumns(): Promise<void> {
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "rawWinProbability" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "calibratedWinProbability" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "impliedMarketProbability" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "edgeVsMarket" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "expectedValueScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "laneTag" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "lineSnapshot" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "priceSnapshot" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "adjustedEdgeScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "uncertaintyScore" double precision`);
}

async function ensureCanonicalComparisonColumns(): Promise<void> {
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "voteWeightingVersion" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "weightedSupportScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "weightedOppositionScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "weightedConsensusScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "weightedDisagreementPenalty" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "dominantSourceFamily" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "laneTag" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "edgeVsMarket" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "adjustedEdgeScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "calibratedWinProbability" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "uncertaintyScore" double precision`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "voteWeightBreakdown" jsonb`);
}

export async function buildPredictionCalibrators(args: string[] = []): Promise<void> {
  await ensurePickQualityTables();
  await ensureSuggestedPlayLedgerQualityColumns();
  const flags = parseFlags(args);
  const since = flags.since ?? "2025-10-01";
  const until = flags.until ?? new Date().toISOString().slice(0, 10);
  const minSample = Number(flags.minSample ?? 40);
  const bucketSize = Number(flags.bucketSize ?? 0.05);

  const rows = await prisma.$queryRawUnsafe<
    Array<{
      laneTag: string | null;
      marketFamily: string;
      bucketStart: number;
      sampleSize: number;
      actualHitRate: number;
    }>
  >(
    `WITH base AS (
       SELECT
         COALESCE(NULLIF(l."laneTag",''), 'other') AS "laneTag",
         ${marketFamilySqlExpr()} AS "marketFamily",
         COALESCE(l."rawWinProbability", l."estimatedProb", l."posteriorHitRate") AS "rawProb",
         l."settledHit" AS "settledHit"
       FROM "SuggestedPlayLedger" l
       WHERE l."settledHit" IS NOT NULL
         AND l."date" >= '${sqlEsc(since)}'::date
         AND l."date" <= '${sqlEsc(until)}'::date
     ),
     bucketed AS (
       SELECT
         "laneTag",
         "marketFamily",
         FLOOR(GREATEST(0, LEAST(0.999, "rawProb")) / ${bucketSize}) * ${bucketSize} AS "bucketStart",
         COUNT(*)::int AS "sampleSize",
         AVG(CASE WHEN "settledHit" = TRUE THEN 1.0 ELSE 0.0 END)::float8 AS "actualHitRate"
       FROM base
       WHERE "rawProb" IS NOT NULL
       GROUP BY 1,2,3
     )
     SELECT * FROM bucketed
     ORDER BY "laneTag","marketFamily","bucketStart"`,
  );

  type Agg = { sampleSize: number; bins: Array<{ min: number; max: number; sampleSize: number; actualHitRate: number }> };
  const byLaneAndFamily = new Map<string, Agg>();
  const byFamily = new Map<string, Agg>();
  const global: Agg = { sampleSize: 0, bins: [] };

  for (const r of rows) {
    const bin = {
      min: Number(r.bucketStart),
      max: Math.min(1, Number(r.bucketStart) + bucketSize),
      sampleSize: r.sampleSize,
      actualHitRate: r.actualHitRate,
    };
    const key = `${r.laneTag ?? "other"}::${r.marketFamily}`;
    const laneAgg = byLaneAndFamily.get(key) ?? { sampleSize: 0, bins: [] };
    laneAgg.sampleSize += r.sampleSize;
    laneAgg.bins.push(bin);
    byLaneAndFamily.set(key, laneAgg);

    const famAgg = byFamily.get(r.marketFamily) ?? { sampleSize: 0, bins: [] };
    famAgg.sampleSize += r.sampleSize;
    famAgg.bins.push(bin);
    byFamily.set(r.marketFamily, famAgg);

    global.sampleSize += r.sampleSize;
    global.bins.push(bin);
  }

  let written = 0;
  const nowIso = new Date().toISOString();
  const rowsToInsert: Array<{
    laneTag: string;
    marketFamily: string;
    minSample: number;
    sampleSize: number;
    bins: unknown;
  }> = [];
  for (const [key, agg] of byLaneAndFamily.entries()) {
    if (agg.sampleSize < minSample) continue;
    const [laneTag, marketFamily] = key.split("::");
    rowsToInsert.push({ laneTag, marketFamily, minSample, sampleSize: agg.sampleSize, bins: agg.bins });
  }
  for (const [marketFamily, agg] of byFamily.entries()) {
    if (agg.sampleSize < minSample) continue;
    rowsToInsert.push({ laneTag: "all", marketFamily, minSample, sampleSize: agg.sampleSize, bins: agg.bins });
  }
  if (global.sampleSize >= minSample) {
    rowsToInsert.push({ laneTag: "all", marketFamily: "global", minSample, sampleSize: global.sampleSize, bins: global.bins });
  }

  for (const row of rowsToInsert) {
    await prisma.$executeRawUnsafe(
      `INSERT INTO "PredictionCalibration"
       ("id","laneTag","marketFamily","minSample","sampleSize","bins","createdAt")
       VALUES
       ('${crypto.randomUUID()}','${sqlEsc(row.laneTag)}','${sqlEsc(row.marketFamily)}',
        ${row.minSample},${row.sampleSize},'${sqlEsc(JSON.stringify(row.bins))}'::jsonb,
        '${sqlEsc(nowIso)}'::timestamptz)`,
    );
    written += 1;
  }

  console.log(`Built calibration artifacts: ${written} rows (since=${since} until=${until}, minSample=${minSample})`);
}

export async function buildSourceReliabilitySnapshot(args: string[] = []): Promise<void> {
  await ensurePickQualityTables();
  await ensureSuggestedPlayLedgerQualityColumns();
  const flags = parseFlags(args);
  const since = flags.since ?? "2025-10-01";
  const until = flags.until ?? new Date().toISOString().slice(0, 10);
  const windowKey = flags.window ?? "rolling_180";

  const rows = await prisma.$queryRawUnsafe<
    Array<{
      modelVotes: unknown;
      outcomeType: string;
      market: string | null;
      settledHit: boolean;
    }>
  >(
    `WITH latest_run_per_date AS (
       SELECT DISTINCT ON (g."date"::date)
         g."date"::date AS "gameDate",
         cp."runId" AS "runId"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE cp."runId" IS NOT NULL
         AND g."date" >= '${sqlEsc(since)}'::date
         AND g."date" <= '${sqlEsc(until)}'::date
       ORDER BY g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
     ),
     canonical_latest AS (
       SELECT
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType",
         cp."modelVotes" AS "modelVotes"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       JOIN latest_run_per_date lr
         ON lr."gameDate" = g."date"::date
        AND lr."runId" = cp."runId"
     )
     SELECT
       cl."modelVotes",
       cl."outcomeType",
       s."market",
       s."settledHit"
     FROM canonical_latest cl
     JOIN LATERAL (
       SELECT s.*
       FROM "SuggestedPlayLedger" s
       WHERE s."date" = cl."gameDate"
         AND s."gameId" = cl."gameId"
         AND s."outcomeType" = cl."outcomeType"
         AND s."settledHit" IS NOT NULL
       ORDER BY s."confidence" DESC NULLS LAST, s."updatedAt" DESC
       LIMIT 1
     ) s ON TRUE`,
  );

  const agg = new Map<string, { sourceFamily: string; marketFamily: string; laneTag: string | null; picks: number; wins: number }>();
  const bump = (sourceFamily: string, marketFamily: string, laneTag: string | null, settledHit: boolean) => {
    const k = `${sourceFamily}::${marketFamily}::${laneTag ?? ""}`;
    const prev = agg.get(k) ?? { sourceFamily, marketFamily, laneTag, picks: 0, wins: 0 };
    prev.picks += 1;
    prev.wins += settledHit ? 1 : 0;
    agg.set(k, prev);
  };
  for (const row of rows) {
    const marketFamily =
      row.market?.startsWith("moneyline") ? "moneyline"
      : row.market?.startsWith("spread") ? "spread"
      : row.market?.startsWith("total") ? "total"
      : row.market?.startsWith("player_") ? "player_prop"
      : "global";
    const laneTag = deriveLaneTag(row.outcomeType, row.market);
    const votes = Array.isArray(row.modelVotes) ? row.modelVotes as Array<{ model_id?: string }> : [];
    const families = new Set<string>();
    for (const v of votes) {
      if (!v?.model_id) continue;
      families.add(sourceFamilyFromModelId(v.model_id));
    }
    for (const sourceFamily of families) {
      bump(sourceFamily, marketFamily, laneTag, row.settledHit);
      bump(sourceFamily, "global", null, row.settledHit);
    }
  }

  const asOf = until;
  let written = 0;
  for (const stat of agg.values()) {
    const hitRate = stat.picks > 0 ? stat.wins / stat.picks : 0;
    await prisma.$executeRawUnsafe(
      `INSERT INTO "PickSourceReliability"
       ("id","windowKey","sourceFamily","marketFamily","laneTag","sampleSize","wins","hitRate","asOfDate")
       VALUES
       ('${crypto.randomUUID()}','${sqlEsc(windowKey)}','${sqlEsc(stat.sourceFamily)}','${sqlEsc(stat.marketFamily)}',${stat.laneTag == null ? "NULL" : `'${sqlEsc(stat.laneTag)}'`},
        ${stat.picks},${stat.wins},${hitRate},'${sqlEsc(asOf)}'::date)`,
    );
    written += 1;
  }
  console.log(`Built source reliability snapshot: ${written} rows (window=${windowKey}, through=${until})`);
}

export async function backfillPickQualityFields(args: string[] = []): Promise<void> {
  await ensurePickQualityTables();
  await ensureSuggestedPlayLedgerQualityColumns();
  const flags = parseFlags(args);
  const since = flags.since ?? "2025-10-01";
  const until = flags.until ?? new Date().toISOString().slice(0, 10);
  await prisma.$executeRawUnsafe(
    `UPDATE "SuggestedPlayLedger" l
     SET
       "laneTag" = COALESCE(NULLIF("laneTag",''), CASE
         WHEN COALESCE(l."market",'') = 'player_points' THEN 'player_points'
         WHEN COALESCE(l."market",'') = 'player_rebounds' THEN 'player_rebounds'
         WHEN COALESCE(l."market",'') = 'player_assists' THEN 'player_assists'
         WHEN COALESCE(l."market",'') LIKE 'player_%' THEN 'other_prop'
         WHEN COALESCE(l."market",'') LIKE 'moneyline%' THEN 'moneyline'
         WHEN COALESCE(l."market",'') LIKE 'spread%' THEN 'spread'
         WHEN COALESCE(l."market",'') LIKE 'total%' THEN 'total'
         ELSE 'other'
       END),
       "rawWinProbability" = COALESCE("rawWinProbability","estimatedProb","posteriorHitRate"),
       "calibratedWinProbability" = COALESCE("calibratedWinProbability","estimatedProb","posteriorHitRate"),
       "impliedMarketProbability" = COALESCE("impliedMarketProbability","impliedProb"),
       "edgeVsMarket" = COALESCE("edgeVsMarket","modelEdge"),
       "expectedValueScore" = COALESCE("expectedValueScore","ev"),
       "lineSnapshot" = COALESCE("lineSnapshot","line"),
       "priceSnapshot" = COALESCE("priceSnapshot","priceAmerican")
     WHERE l."date" >= '${sqlEsc(since)}'::date
       AND l."date" <= '${sqlEsc(until)}'::date`,
  );
  console.log(`Backfilled pick-quality additive fields for SuggestedPlayLedger (${since}..${until}).`);
}

export async function reportPickQuality(args: string[] = []): Promise<void> {
  await ensureSuggestedPlayLedgerQualityColumns();
  const flags = parseFlags(args);
  const since = flags.since ?? "2025-10-01";
  const until = flags.until ?? new Date().toISOString().slice(0, 10);
  const rows = await prisma.$queryRawUnsafe<
    Array<{
      modelVersion: string;
      laneTag: string;
      picks: number;
      wins: number;
      hitRate: number;
      avgRaw: number | null;
      avgCal: number | null;
      avgEdge: number | null;
      avgAdjEdge: number | null;
      avgUncertainty: number | null;
    }>
  >(
    `WITH latest_run_per_date AS (
       SELECT DISTINCT ON (g."date"::date)
         g."date"::date AS "gameDate",
         cp."runId" AS "runId",
         COALESCE(cp."runContext"->>'modelVersionName','live') AS "modelVersion"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE cp."runId" IS NOT NULL
         AND g."date" >= '${sqlEsc(since)}'::date
         AND g."date" <= '${sqlEsc(until)}'::date
       ORDER BY g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
     ),
     canonical_latest AS (
       SELECT
         lr."modelVersion" AS "modelVersion",
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       JOIN latest_run_per_date lr
         ON lr."gameDate" = g."date"::date
        AND lr."runId" = cp."runId"
     )
     SELECT
       cl."modelVersion" AS "modelVersion",
       COALESCE(NULLIF(l."laneTag",''),'other') AS "laneTag",
       COUNT(*)::int AS "picks",
       SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)::int AS "wins",
       AVG(CASE WHEN l."settledHit" = TRUE THEN 1.0 ELSE 0.0 END)::float8 AS "hitRate",
       AVG(l."rawWinProbability")::float8 AS "avgRaw",
       AVG(l."calibratedWinProbability")::float8 AS "avgCal",
       AVG(l."edgeVsMarket")::float8 AS "avgEdge",
       AVG(l."adjustedEdgeScore")::float8 AS "avgAdjEdge",
       AVG(l."uncertaintyScore")::float8 AS "avgUncertainty"
     FROM canonical_latest cl
     JOIN LATERAL (
       SELECT s.*
       FROM "SuggestedPlayLedger" s
       WHERE s."date" = cl."gameDate"
         AND s."gameId" = cl."gameId"
         AND s."outcomeType" = cl."outcomeType"
         AND s."settledHit" IS NOT NULL
       ORDER BY s."confidence" DESC NULLS LAST, s."updatedAt" DESC
       LIMIT 1
     ) l ON TRUE
     GROUP BY cl."modelVersion", COALESCE(NULLIF(l."laneTag",''),'other')
     ORDER BY cl."modelVersion", "picks" DESC`,
  );
  console.log(`\n=== Pick Quality Report (${since}..${until}) ===\n`);
  if (rows.length === 0) {
    console.log("No graded rows found.");
    return;
  }
  for (const r of rows) {
    console.log(
      `${r.modelVersion} | ${r.laneTag} | picks=${r.picks} wins=${r.wins} hit=${(r.hitRate * 100).toFixed(1)}%` +
      ` | raw=${r.avgRaw != null ? (r.avgRaw * 100).toFixed(1) : "n/a"}%` +
      ` | cal=${r.avgCal != null ? (r.avgCal * 100).toFixed(1) : "n/a"}%` +
      ` | edge=${r.avgEdge != null ? (r.avgEdge * 100).toFixed(2) : "n/a"}%` +
      ` | adjEdge=${r.avgAdjEdge != null ? (r.avgAdjEdge * 100).toFixed(2) : "n/a"}%` +
      ` | uncertainty=${r.avgUncertainty != null ? r.avgUncertainty.toFixed(3) : "n/a"}`,
    );
  }
  console.log("");
}

export async function runVoteWeightingDual(args: string[] = []): Promise<void> {
  await ensureCanonicalComparisonColumns();
  const flags = parseFlags(args);
  const since = flags.since;
  const until = flags.until;
  const weightedStrength = flags.weightedStrength == null ? null : Number(flags.weightedStrength);
  if (!since || !until) {
    throw new Error("Usage: run:vote-weighting-dual --since YYYY-MM-DD --until YYYY-MM-DD [--weightedStrength 1.5]");
  }
  const rows = await prisma.$queryRawUnsafe<Array<{ d: string }>>(
    `SELECT to_char(date, 'YYYY-MM-DD') AS d
     FROM "Game"
     WHERE date >= '${sqlEsc(since)}'::date
       AND date <= '${sqlEsc(until)}'::date
     GROUP BY date
     ORDER BY date ASC`,
  );
  const dates = rows.map((r) => String(r.d)).filter(Boolean);
  console.log(`Running dual vote-weighting prediction passes for ${dates.length} dates (${since}..${until})`);
  for (const mode of ["false", "true"] as const) {
    const version = mode === "true" ? "weighted_v1" : "legacy";
    let ok = 0;
    let fail = 0;
    console.log(`\n=== Pass: ${version} ===\n`);
    for (let i = 0; i < dates.length; i++) {
      const d = dates[i]!;
      console.log(`[${i + 1}/${dates.length}] predictGames --date ${d} --dynamicVoteWeighting ${mode}`);
      try {
        const runArgs = ["--date", d, "--dynamicVoteWeighting", mode];
        if (mode === "true" && weightedStrength != null && Number.isFinite(weightedStrength) && weightedStrength > 0) {
          runArgs.push("--voteWeightingStrength", String(weightedStrength));
        }
        await predictGames(runArgs);
        ok += 1;
      } catch (err) {
        fail += 1;
        console.error(`Failed ${d}:`, err instanceof Error ? err.message : String(err));
      }
    }
    console.log(`\n=== ${version} complete | success=${ok} failed=${fail} ===`);
  }
}

type ComparisonRow = {
  voteWeightingVersion: string;
  laneTag: string | null;
  dominantSourceFamily: string | null;
  weightedDisagreementPenalty: number | null;
  uncertaintyScore: number | null;
  weightedSupportScore: number | null;
  confidenceScore: number;
  edgeVsMarket: number | null;
  adjustedEdgeScore: number | null;
  calibratedWinProbability: number | null;
  settledHit: boolean;
  modelVotes: unknown;
  voteWeightBreakdown: unknown;
};

function bucketBy(value: number | null, cuts: number[], labels: string[]): string {
  if (value == null || Number.isNaN(value)) return "unknown";
  for (let i = 0; i < cuts.length; i++) {
    if (value < cuts[i]!) return labels[i]!;
  }
  return labels[labels.length - 1]!;
}

function summarize(rows: ComparisonRow[]) {
  const total = rows.length;
  const wins = rows.filter((r) => r.settledHit).length;
  const avg = (xs: Array<number | null>) => {
    const vals = xs.filter((x): x is number => x != null && Number.isFinite(x));
    if (vals.length === 0) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  };
  return {
    totalPicks: total,
    winRate: total > 0 ? wins / total : 0,
    avgEdgeVsMarket: avg(rows.map((r) => r.edgeVsMarket)),
    avgAdjustedEdgeScore: avg(rows.map((r) => r.adjustedEdgeScore)),
    avgCalibratedWinProbability: avg(rows.map((r) => r.calibratedWinProbability)),
  };
}

function populatedPct(rows: ComparisonRow[], picker: (r: ComparisonRow) => unknown): number {
  if (rows.length === 0) return 0;
  const populated = rows.filter((r) => {
    const v = picker(r);
    if (v == null) return false;
    if (typeof v === "string") return v.trim().length > 0;
    if (typeof v === "number") return Number.isFinite(v);
    return true;
  }).length;
  return populated / rows.length;
}

function sortByScoreDesc(rows: ComparisonRow[], useWeighted: boolean): ComparisonRow[] {
  const score = (r: ComparisonRow) =>
    useWeighted
      ? (r.weightedSupportScore ?? r.confidenceScore)
      : r.confidenceScore;
  return [...rows].sort((a, b) => score(b) - score(a));
}

function safePct(v: number | null): string {
  if (v == null || Number.isNaN(v)) return "n/a";
  return `${(v * 100).toFixed(2)}%`;
}

function statsSummary(values: number[]): {
  min: number;
  max: number;
  mean: number;
  stddev: number;
  p50: number;
  p60: number;
  p70: number;
  p80: number;
  p90: number;
  p95: number;
} | null {
  const vals = values.filter((v) => Number.isFinite(v));
  if (vals.length === 0) return null;
  const sorted = [...vals].sort((a, b) => a - b);
  const mean = sorted.reduce((a, b) => a + b, 0) / sorted.length;
  const variance = sorted.reduce((a, b) => a + (b - mean) ** 2, 0) / sorted.length;
  const pct = (p: number) => {
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((p / 100) * (sorted.length - 1))));
    return sorted[idx]!;
  };
  return {
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
    mean,
    stddev: Math.sqrt(variance),
    p50: pct(50),
    p60: pct(60),
    p70: pct(70),
    p80: pct(80),
    p90: pct(90),
    p95: pct(95),
  };
}

function histogram(values: number[], bins = 10): Array<{ start: number; end: number; count: number }> {
  if (values.length === 0) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = max === min ? 1 : (max - min) / bins;
  const out: Array<{ start: number; end: number; count: number }> = [];
  for (let i = 0; i < bins; i++) {
    out.push({ start: min + width * i, end: min + width * (i + 1), count: 0 });
  }
  for (const v of values) {
    const idx = width === 0 ? 0 : Math.min(bins - 1, Math.floor((v - min) / width));
    out[idx]!.count += 1;
  }
  return out;
}

type BreakdownRow = {
  decision: "yes" | "no" | "abstain";
  source_family: string;
  confidence: number | null;
  base_weight?: number;
  reliability_weight?: number;
  sample_confidence_weight?: number;
  uncertainty_discount?: number;
  lane_fit_weight?: number;
  pre_strength_vote_weight?: number;
  vote_weighting_strength?: number;
  final_vote_weight?: number;
};

function parseBreakdownRows(v: unknown): BreakdownRow[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x) => x && typeof x === "object") as BreakdownRow[];
}

function recomputeConsensusFromBreakdown(rows: BreakdownRow[], strength: number): number | null {
  if (rows.length === 0) return null;
  const active = rows.filter((r) => r.decision !== "abstain");
  if (active.length < 2) return null;
  const abstains = rows.length - active.length;
  const byFamily = new Map<string, { support: number; opposition: number; totalWeight: number }>();
  for (const r of active) {
    const conf = r.confidence ?? null;
    if (conf == null || !Number.isFinite(conf)) continue;
    const preW =
      r.pre_strength_vote_weight ??
      ((r.base_weight ?? 1) *
        (r.reliability_weight ?? 1) *
        (r.uncertainty_discount ?? 1) *
        (r.lane_fit_weight ?? 1));
    const w = Math.pow(preW, strength);
    const contrib = w * conf;
    const key = r.source_family || "unknown";
    const agg = byFamily.get(key) ?? { support: 0, opposition: 0, totalWeight: 0 };
    if (r.decision === "yes") agg.support += contrib;
    if (r.decision === "no") agg.opposition += contrib;
    agg.totalWeight += w;
    byFamily.set(key, agg);
  }
  let support = 0;
  let opposition = 0;
  for (const agg of byFamily.values()) {
    const avgSupport = agg.totalWeight > 0 ? agg.support / agg.totalWeight : 0;
    const avgOpp = agg.totalWeight > 0 ? agg.opposition / agg.totalWeight : 0;
    support += avgSupport;
    opposition += avgOpp;
  }
  const total = support + opposition;
  if (total <= 0) return null;
  const supportRatio = support / total;
  const disagreement = Math.min(support, opposition) / total;
  const disagreementPenalty = disagreement * 0.25;
  const abstainPenalty = rows.length > 0 ? (abstains / rows.length) * 0.1 : 0;
  return Math.max(0, Math.min(1, supportRatio - disagreementPenalty - abstainPenalty));
}

function printDelta(label: string, legacy: number, weighted: number, asPct = true): void {
  const abs = weighted - legacy;
  const rel = legacy !== 0 ? (abs / legacy) * 100 : 0;
  const fmt = (n: number) => (asPct ? `${(n * 100).toFixed(2)}%` : n.toFixed(4));
  console.log(`${label}: legacy=${fmt(legacy)} weighted=${fmt(weighted)} delta=${fmt(abs)} rel=${rel.toFixed(1)}%`);
}

function extractSourceFamilies(modelVotes: unknown): string[] {
  const votes = Array.isArray(modelVotes) ? (modelVotes as ModelVote[]) : [];
  const set = new Set<string>();
  for (const v of votes) set.add(normalizeVoteSource(v));
  return [...set];
}

export async function reportVoteWeightingComparison(args: string[] = []): Promise<void> {
  await ensureCanonicalComparisonColumns();
  const flags = parseFlags(args);
  const since = flags.since;
  const until = flags.until;
  const jsonPathFlag = flags.jsonPath ?? null;
  const jsonEnabled = flags.json === "true" || flags.json === "1";
  const thresholdAnalysis = flags.thresholdAnalysis === "true" || flags.thresholdAnalysis === "1";
  const parsedStrengths = String(flags.strengths ?? "1.0,1.5,2.0,3.0")
    .split(",")
    .map((v) => Number(v.trim()))
    .filter((v) => Number.isFinite(v) && v > 0);
  const strengthList = parsedStrengths.length > 0 ? parsedStrengths : [1.0, 1.5, 2.0, 3.0];
  const minLaneSample = Math.max(1, Number(flags.minLaneSample ?? 20));
  const jsonPath = jsonPathFlag ?? (jsonEnabled ? "data/reports/vote-weighting-compare.json" : null);
  if (!since || !until) {
    throw new Error("Usage: report:vote-weighting-comparison --since YYYY-MM-DD --until YYYY-MM-DD [--jsonPath path]");
  }

  const rows = await prisma.$queryRawUnsafe<ComparisonRow[]>(
    `WITH latest_run_per_date_version AS (
       SELECT DISTINCT ON (COALESCE(cp."voteWeightingVersion",'legacy'), g."date"::date)
         COALESCE(cp."voteWeightingVersion",'legacy') AS "voteWeightingVersion",
         g."date"::date AS "gameDate",
         cp."runId" AS "runId"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE cp."runId" IS NOT NULL
         AND g."date" >= '${sqlEsc(since)}'::date
         AND g."date" <= '${sqlEsc(until)}'::date
         AND COALESCE(cp."voteWeightingVersion",'legacy') IN ('legacy','weighted_v1')
       ORDER BY COALESCE(cp."voteWeightingVersion",'legacy'), g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
     ),
     canonical_latest AS (
       SELECT
         COALESCE(cp."voteWeightingVersion",'legacy') AS "voteWeightingVersion",
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType",
         cp."laneTag" AS "laneTag",
         cp."dominantSourceFamily" AS "dominantSourceFamily",
         cp."weightedDisagreementPenalty" AS "weightedDisagreementPenalty",
         cp."uncertaintyScore" AS "uncertaintyScore",
         cp."weightedSupportScore" AS "weightedSupportScore",
         cp."confidenceScore" AS "confidenceScore",
         cp."edgeVsMarket" AS "edgeVsMarket",
         cp."adjustedEdgeScore" AS "adjustedEdgeScore",
         cp."calibratedWinProbability" AS "calibratedWinProbability",
         cp."modelVotes" AS "modelVotes",
         cp."voteWeightBreakdown" AS "voteWeightBreakdown"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       JOIN latest_run_per_date_version lr
         ON lr."voteWeightingVersion" = COALESCE(cp."voteWeightingVersion",'legacy')
        AND lr."gameDate" = g."date"::date
        AND lr."runId" = cp."runId"
     )
     SELECT
       cl."voteWeightingVersion",
       cl."laneTag",
       cl."dominantSourceFamily",
       cl."weightedDisagreementPenalty",
       cl."uncertaintyScore",
       cl."weightedSupportScore",
       cl."confidenceScore",
       cl."edgeVsMarket",
       cl."adjustedEdgeScore",
       cl."calibratedWinProbability",
       s."settledHit",
       cl."modelVotes",
       cl."voteWeightBreakdown"
     FROM canonical_latest cl
     JOIN LATERAL (
       SELECT s.*
       FROM "SuggestedPlayLedger" s
       WHERE s."date" = cl."gameDate"
         AND s."gameId" = cl."gameId"
         AND s."outcomeType" = cl."outcomeType"
         AND s."isActionable" = TRUE
         AND s."settledHit" IS NOT NULL
       ORDER BY s."confidence" DESC NULLS LAST, s."updatedAt" DESC
       LIMIT 1
     ) s ON TRUE`,
  );

  const legacyRows = rows.filter((r) => r.voteWeightingVersion === "legacy");
  const weightedRows = rows.filter((r) => r.voteWeightingVersion === "weighted_v1");
  const legacy = summarize(legacyRows);
  const weighted = summarize(weightedRows);

  const completeness = {
    laneTag: {
      legacy: populatedPct(legacyRows, (r) => r.laneTag),
      weighted: populatedPct(weightedRows, (r) => r.laneTag),
    },
    dominantSourceFamily: {
      legacy: populatedPct(legacyRows, (r) => r.dominantSourceFamily),
      weighted: populatedPct(weightedRows, (r) => r.dominantSourceFamily),
    },
    weightedDisagreementPenalty: {
      legacy: populatedPct(legacyRows, (r) => r.weightedDisagreementPenalty),
      weighted: populatedPct(weightedRows, (r) => r.weightedDisagreementPenalty),
    },
    weightedSupportScore: {
      legacy: populatedPct(legacyRows, (r) => r.weightedSupportScore),
      weighted: populatedPct(weightedRows, (r) => r.weightedSupportScore),
    },
    edgeVsMarket: {
      legacy: populatedPct(legacyRows, (r) => r.edgeVsMarket),
      weighted: populatedPct(weightedRows, (r) => r.edgeVsMarket),
    },
    adjustedEdgeScore: {
      legacy: populatedPct(legacyRows, (r) => r.adjustedEdgeScore),
      weighted: populatedPct(weightedRows, (r) => r.adjustedEdgeScore),
    },
    calibratedWinProbability: {
      legacy: populatedPct(legacyRows, (r) => r.calibratedWinProbability),
      weighted: populatedPct(weightedRows, (r) => r.calibratedWinProbability),
    },
  };

  console.log(`\n=== Vote Weighting Comparison (${since}..${until}) ===\n`);
  console.log("Metadata completeness:");
  console.log("field | legacy populated % | weighted populated %");
  for (const [field, vals] of Object.entries(completeness)) {
    console.log(
      `${field} | ${(vals.legacy * 100).toFixed(1)}% | ${(vals.weighted * 100).toFixed(1)}%`,
    );
  }
  const legacyDominantSufficient = completeness.dominantSourceFamily.legacy >= 0.5;
  if (!legacyDominantSufficient) {
    console.log("Note: legacy dominantSourceFamily completeness is low; cross-version source-family comparisons are limited.\n");
  } else {
    console.log("");
  }

  console.log("Core metrics:");
  printDelta("winRate", legacy.winRate, weighted.winRate, true);
  printDelta("avgEdgeVsMarket", legacy.avgEdgeVsMarket ?? 0, weighted.avgEdgeVsMarket ?? 0, true);
  printDelta("avgAdjustedEdgeScore", legacy.avgAdjustedEdgeScore ?? 0, weighted.avgAdjustedEdgeScore ?? 0, true);
  printDelta("avgCalibratedWinProbability", legacy.avgCalibratedWinProbability ?? 0, weighted.avgCalibratedWinProbability ?? 0, true);
  console.log(`pickCount: legacy=${legacy.totalPicks} weighted=${weighted.totalPicks} delta=${weighted.totalPicks - legacy.totalPicks}`);

  const weightedScoreValues = weightedRows
    .map((r) => r.weightedSupportScore)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  const legacyScoreValues = legacyRows
    .map((r) => r.confidenceScore)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  const weightedStats = statsSummary(weightedScoreValues);
  const legacyStats = statsSummary(legacyScoreValues);
  console.log("\nScore distribution diagnostics:");
  if (weightedStats && legacyStats) {
    console.log(
      `weightedSupportScore: min=${weightedStats.min.toFixed(4)} max=${weightedStats.max.toFixed(4)} mean=${weightedStats.mean.toFixed(4)} stddev=${weightedStats.stddev.toFixed(4)}` +
      ` p50=${weightedStats.p50.toFixed(4)} p60=${weightedStats.p60.toFixed(4)} p70=${weightedStats.p70.toFixed(4)} p80=${weightedStats.p80.toFixed(4)} p90=${weightedStats.p90.toFixed(4)} p95=${weightedStats.p95.toFixed(4)}`,
    );
    console.log(
      `legacy confidenceScore: min=${legacyStats.min.toFixed(4)} max=${legacyStats.max.toFixed(4)} mean=${legacyStats.mean.toFixed(4)} stddev=${legacyStats.stddev.toFixed(4)}` +
      ` p50=${legacyStats.p50.toFixed(4)} p60=${legacyStats.p60.toFixed(4)} p70=${legacyStats.p70.toFixed(4)} p80=${legacyStats.p80.toFixed(4)} p90=${legacyStats.p90.toFixed(4)} p95=${legacyStats.p95.toFixed(4)}`,
    );
  } else {
    console.log("Insufficient score data for distribution diagnostics.");
  }
  console.log("Weighted histogram (10 bins):");
  for (const b of histogram(weightedScoreValues, 10)) {
    console.log(`  [${b.start.toFixed(4)}, ${b.end.toFixed(4)}): ${b.count}`);
  }

  const weightedBreakdowns = weightedRows.flatMap((r) => parseBreakdownRows(r.voteWeightBreakdown));
  const componentStats = (picker: (r: BreakdownRow) => number | undefined) => {
    const vals = weightedBreakdowns
      .map(picker)
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
    if (vals.length === 0) return null;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length;
    return { mean, variance };
  };
  const componentRows: Array<[string, ((r: BreakdownRow) => number | undefined)]> = [
    ["baseWeight", (r) => r.base_weight],
    ["reliabilityWeight", (r) => r.reliability_weight],
    ["sampleConfidenceWeight", (r) => r.sample_confidence_weight],
    ["uncertaintyDiscount", (r) => r.uncertainty_discount],
    ["laneFitWeight", (r) => r.lane_fit_weight],
    ["preStrengthVoteWeight", (r) => r.pre_strength_vote_weight],
    ["finalVoteWeight", (r) => r.final_vote_weight],
  ];
  console.log("\nWeight component impact (weighted rows):");
  if (weightedBreakdowns.length === 0) {
    console.log("No voteWeightBreakdown rows found. Re-run weighted predictions to collect component diagnostics.");
  } else {
    console.log(`breakdown rows=${weightedBreakdowns.length}`);
    for (const [label, getter] of componentRows) {
      const s = componentStats(getter);
      if (!s) continue;
      console.log(`  ${label}: avg=${s.mean.toFixed(4)} variance=${s.variance.toFixed(6)}`);
    }
  }

  const byLane = (versionRows: ComparisonRow[]) => {
    const map = new Map<string, ComparisonRow[]>();
    for (const r of versionRows) {
      const k = r.laneTag ?? "unknown";
      const arr = map.get(k) ?? [];
      arr.push(r);
      map.set(k, arr);
    }
    return map;
  };
  const legacyLane = byLane(legacyRows);
  const weightedLane = byLane(weightedRows);
  console.log("\nBy laneTag:");
  const allLanes = new Set([...legacyLane.keys(), ...weightedLane.keys()]);
  for (const lane of allLanes) {
    const l = summarize(legacyLane.get(lane) ?? []);
    const w = summarize(weightedLane.get(lane) ?? []);
    console.log(
      `${lane}: legacy ${(l.winRate * 100).toFixed(1)}% (n=${l.totalPicks}) | weighted ${(w.winRate * 100).toFixed(1)}% (n=${w.totalPicks}) | delta ${((w.winRate - l.winRate) * 100).toFixed(1)}%`,
    );
  }

  const bySource = (versionRows: ComparisonRow[]) => {
    const map = new Map<string, ComparisonRow[]>();
    for (const r of versionRows) {
      const key = r.dominantSourceFamily ?? "unknown";
      const arr = map.get(key) ?? [];
      arr.push(r);
      map.set(key, arr);
    }
    return map;
  };
  console.log("\nBy dominantSourceFamily:");
  const weightedSourceMap = bySource(weightedRows);
  const legacySourceMap = bySource(legacyRows);
  for (const [label, group] of weightedSourceMap.entries()) {
    const sw = summarize(group);
    if (legacyDominantSufficient) {
      const sl = summarize(legacySourceMap.get(label) ?? []);
      console.log(
        `${label}: weighted ${(sw.winRate * 100).toFixed(1)}% (n=${sw.totalPicks}) edge=${((sw.avgEdgeVsMarket ?? 0) * 100).toFixed(2)}%` +
        ` | legacy ${(sl.winRate * 100).toFixed(1)}% (n=${sl.totalPicks}) edge=${((sl.avgEdgeVsMarket ?? 0) * 100).toFixed(2)}%`,
      );
    } else {
      console.log(`${label}: ${(sw.winRate * 100).toFixed(1)}% (n=${sw.totalPicks}) edge=${((sw.avgEdgeVsMarket ?? 0) * 100).toFixed(2)}%`);
    }
  }

  const bucketReport = (title: string, versionRows: ComparisonRow[], getter: (r: ComparisonRow) => string) => {
    console.log(`\n${title}:`);
    const map = new Map<string, ComparisonRow[]>();
    for (const r of versionRows) {
      const b = getter(r);
      const arr = map.get(b) ?? [];
      arr.push(r);
      map.set(b, arr);
    }
    for (const [b, g] of map.entries()) {
      const s = summarize(g);
      console.log(`${b}: ${(s.winRate * 100).toFixed(1)}% (n=${s.totalPicks})`);
    }
  };

  bucketReport(
    "Disagreement buckets (weighted)",
    weightedRows,
    (r) => bucketBy(r.weightedDisagreementPenalty, [0.08, 0.16], ["low", "medium", "high"]),
  );
  bucketReport(
    "Uncertainty buckets (both)",
    rows,
    (r) => bucketBy(r.uncertaintyScore, [0.33, 0.66], ["low", "mid", "high"]),
  );
  bucketReport(
    "Support score buckets (both)",
    rows,
    (r) => {
      const score = r.weightedSupportScore ?? r.confidenceScore;
      return bucketBy(score, [0.55, 0.6, 0.65], ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65+"]);
    },
  );

  console.log("\nScore distribution above thresholds:");
  for (const t of [0.55, 0.6, 0.65]) {
    const l = legacyRows.filter((r) => (r.weightedSupportScore ?? r.confidenceScore) >= t).length;
    const w = weightedRows.filter((r) => (r.weightedSupportScore ?? r.confidenceScore) >= t).length;
    const lp = legacyRows.length > 0 ? l / legacyRows.length : 0;
    const wp = weightedRows.length > 0 ? w / weightedRows.length : 0;
    console.log(`>=${t.toFixed(2)}: legacy ${(lp * 100).toFixed(1)}% (${l}) | weighted ${(wp * 100).toFixed(1)}% (${w})`);
  }

  const diversity = (versionRows: ComparisonRow[]) => {
    const counts = versionRows.map((r) => extractSourceFamilies(r.modelVotes).length);
    const avg = counts.length > 0 ? counts.reduce((a, b) => a + b, 0) / counts.length : 0;
    const single = versionRows.filter((r) => extractSourceFamilies(r.modelVotes).length <= 1);
    const multi = versionRows.filter((r) => extractSourceFamilies(r.modelVotes).length > 1);
    return { avg, single: summarize(single), multi: summarize(multi) };
  };
  const dLegacy = diversity(legacyRows);
  const dWeighted = diversity(weightedRows);
  console.log("\nFamily diversity:");
  console.log(`avg unique source families per pick: legacy=${dLegacy.avg.toFixed(2)} weighted=${dWeighted.avg.toFixed(2)}`);
  console.log(`single-family picks: legacy ${(dLegacy.single.winRate * 100).toFixed(1)}% (n=${dLegacy.single.totalPicks}) | weighted ${(dWeighted.single.winRate * 100).toFixed(1)}% (n=${dWeighted.single.totalPicks})`);
  console.log(`multi-family picks: legacy ${(dLegacy.multi.winRate * 100).toFixed(1)}% (n=${dLegacy.multi.totalPicks}) | weighted ${(dWeighted.multi.winRate * 100).toFixed(1)}% (n=${dWeighted.multi.totalPicks})`);

  if (thresholdAnalysis) {
    const thresholds = [0.55, 0.6, 0.65, 0.7];
    const legacySorted = sortByScoreDesc(legacyRows, false);
    console.log("\nWeighted threshold analysis:");
    for (const strength of strengthList) {
      const weightedAtStrength = weightedRows
        .map((r) => {
          const fromBreakdown = recomputeConsensusFromBreakdown(parseBreakdownRows(r.voteWeightBreakdown), strength);
          const score = fromBreakdown ?? r.weightedSupportScore ?? r.confidenceScore;
          return { row: r, score };
        })
        .sort((a, b) => b.score - a.score);
      console.log(`\nstrength=${strength.toFixed(2)}`);
      for (const t of thresholds) {
        const wr = weightedAtStrength.filter((x) => x.score >= t).map((x) => x.row);
        if (wr.length < 10 && t >= 0.7) continue;
        const ws = summarize(wr);
        console.log(
          `threshold ${t.toFixed(2)}: picks=${ws.totalPicks} win=${safePct(ws.winRate)} edge=${safePct(ws.avgEdgeVsMarket)} adjEdge=${safePct(ws.avgAdjustedEdgeScore)} cal=${safePct(ws.avgCalibratedWinProbability)}`,
        );
        const laneMap = new Map<string, ComparisonRow[]>();
        for (const r of wr) {
          const k = r.laneTag ?? "unknown";
          const arr = laneMap.get(k) ?? [];
          arr.push(r);
          laneMap.set(k, arr);
        }
        for (const [lane, group] of laneMap.entries()) {
          if (group.length < minLaneSample) continue;
          const ls = summarize(group);
          console.log(`  lane ${lane}: picks=${ls.totalPicks} win=${safePct(ls.winRate)} edge=${safePct(ls.avgEdgeVsMarket)}`);
        }
        const disagreementLow = wr.filter((r) => (r.weightedDisagreementPenalty ?? 0) < 0.08);
        const disagreementMid = wr.filter((r) => (r.weightedDisagreementPenalty ?? 0) >= 0.08 && (r.weightedDisagreementPenalty ?? 0) < 0.16);
        const disagreementHigh = wr.filter((r) => (r.weightedDisagreementPenalty ?? 0) >= 0.16);
        const lowS = summarize(disagreementLow);
        const midS = summarize(disagreementMid);
        const highS = summarize(disagreementHigh);
        console.log(
          `  disagreement: low n=${lowS.totalPicks} win=${safePct(lowS.winRate)} | mid n=${midS.totalPicks} win=${safePct(midS.winRate)} | high n=${highS.totalPicks} win=${safePct(highS.winRate)}`,
        );

        const n = wr.length;
        const legacyTopN = legacySorted.slice(0, n);
        const lTop = summarize(legacyTopN);
        console.log(
          `  matched-N (${n}): weighted win=${safePct(ws.winRate)} vs legacy win=${safePct(lTop.winRate)}` +
          ` | weighted edge=${safePct(ws.avgEdgeVsMarket)} vs legacy edge=${safePct(lTop.avgEdgeVsMarket)}` +
          ` | weighted adjEdge=${safePct(ws.avgAdjustedEdgeScore)} vs legacy adjEdge=${safePct(lTop.avgAdjustedEdgeScore)}`,
        );
      }
    }
  }

  if (jsonPath) {
    let outputPath = jsonPath;
    const normalized = outputPath.replaceAll("\\", "/");
    const hasJsonExt = normalized.toLowerCase().endsWith(".json");
    if (!hasJsonExt) {
      outputPath = normalized.endsWith("/")
        ? `${normalized}vote-weighting-compare.json`
        : `${normalized}/vote-weighting-compare.json`;
    }
    const absOutputPath = path.isAbsolute(outputPath)
      ? outputPath
      : path.join(process.cwd(), outputPath);
    const dir = path.dirname(absOutputPath);
    await mkdir(dir, { recursive: true });
    try {
      const s = await stat(absOutputPath);
      if (s.isDirectory()) {
        throw new Error(`jsonPath resolves to a directory: ${absOutputPath}`);
      }
    } catch {
      // File does not exist yet; safe to write.
    }
    await writeFile(
      absOutputPath,
      JSON.stringify(
        {
          since,
          until,
          completeness,
          core: { legacy, weighted },
          weightedDistribution: weightedStats,
          legacyDistribution: legacyStats,
          rows,
        },
        null,
        2,
      ),
      "utf8",
    );
    console.log(`\nWrote JSON report: ${absOutputPath}`);
  }
  console.log("");
}

export async function reportVoteWeightingThresholds(args: string[] = []): Promise<void> {
  await reportVoteWeightingComparison([...args, "--thresholdAnalysis", "true"]);
}

type VoteInputAuditRow = {
  voteWeightingVersion: string;
  laneTag: string | null;
  regimeTags: unknown;
  uncertaintyScore: number | null;
  weightedSupportScore: number | null;
  confidenceScore: number;
  modelVotes: unknown;
  voteWeightBreakdown: unknown;
  settledHit: boolean;
};

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function sampleBucket(n: number | null): string {
  if (n == null || !Number.isFinite(n)) return "no_match";
  if (n < 10) return "<10";
  if (n < 25) return "10-24";
  if (n < 50) return "25-49";
  if (n < 100) return "50-99";
  return "100+";
}

function parseModelVotes(modelVotes: unknown): Array<{ model_id?: string }> {
  return Array.isArray(modelVotes) ? (modelVotes as Array<{ model_id?: string }>) : [];
}

export async function reportVoteWeightInputAudit(args: string[] = []): Promise<void> {
  await ensureCanonicalComparisonColumns();
  const flags = parseFlags(args);
  const since = flags.since;
  const until = flags.until;
  const windowKey = flags.window ?? "rolling_180";
  const minSample = Math.max(1, Number(flags.minSample ?? 40));
  if (!since || !until) {
    throw new Error("Usage: report:vote-weight-input-audit --since YYYY-MM-DD --until YYYY-MM-DD [--window rolling_180]");
  }

  const snapshot = await loadSourceReliabilitySnapshot(windowKey);
  const sourceReliabilityMap: Record<string, { hitRate: number; sampleSize: number }> = {};
  for (const row of snapshot?.rows ?? []) {
    const sf = normalizeVoteSourceFromModelId(String(row.sourceFamily ?? ""));
    if (!sf) continue;
    const marketFamily = String(row.marketFamily ?? "global").trim() || "global";
    sourceReliabilityMap[`${sf}::${marketFamily}`] = {
      hitRate: Number(row.hitRate),
      sampleSize: Number(row.sampleSize),
    };
    if (row.laneTag) {
      sourceReliabilityMap[`${sf}::${row.laneTag}`] = {
        hitRate: Number(row.hitRate),
        sampleSize: Number(row.sampleSize),
      };
    }
  }

  const rows = await prisma.$queryRawUnsafe<VoteInputAuditRow[]>(
    `WITH latest_run_per_date_version AS (
       SELECT DISTINCT ON (COALESCE(cp."voteWeightingVersion",'legacy'), g."date"::date)
         COALESCE(cp."voteWeightingVersion",'legacy') AS "voteWeightingVersion",
         g."date"::date AS "gameDate",
         cp."runId" AS "runId"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE cp."runId" IS NOT NULL
         AND g."date" >= '${sqlEsc(since)}'::date
         AND g."date" <= '${sqlEsc(until)}'::date
         AND COALESCE(cp."voteWeightingVersion",'legacy') IN ('legacy','weighted_v1')
       ORDER BY COALESCE(cp."voteWeightingVersion",'legacy'), g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
     ),
     canonical_latest AS (
       SELECT
         COALESCE(cp."voteWeightingVersion",'legacy') AS "voteWeightingVersion",
         cp."laneTag" AS "laneTag",
         cp."regimeTags" AS "regimeTags",
         cp."uncertaintyScore" AS "uncertaintyScore",
         cp."weightedSupportScore" AS "weightedSupportScore",
         cp."confidenceScore" AS "confidenceScore",
         cp."modelVotes" AS "modelVotes",
         cp."voteWeightBreakdown" AS "voteWeightBreakdown",
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       JOIN latest_run_per_date_version lr
         ON lr."voteWeightingVersion" = COALESCE(cp."voteWeightingVersion",'legacy')
        AND lr."gameDate" = g."date"::date
        AND lr."runId" = cp."runId"
     )
     SELECT
       cl."voteWeightingVersion",
       cl."laneTag",
       cl."regimeTags",
       cl."uncertaintyScore",
       cl."weightedSupportScore",
       cl."confidenceScore",
       cl."modelVotes",
       cl."voteWeightBreakdown",
       s."settledHit"
     FROM canonical_latest cl
     JOIN LATERAL (
       SELECT s.*
       FROM "SuggestedPlayLedger" s
       WHERE s."date" = cl."gameDate"
         AND s."gameId" = cl."gameId"
         AND s."outcomeType" = cl."outcomeType"
         AND s."isActionable" = TRUE
         AND s."settledHit" IS NOT NULL
       ORDER BY s."confidence" DESC NULLS LAST, s."updatedAt" DESC
       LIMIT 1
     ) s ON TRUE`,
  );

  type Counter = {
    votes: number;
    exact: number;
    fallback: number;
    neutral: number;
    lanePresent: number;
    laneNonNeutral: number;
    relWeightSum: number;
    sampleConfSum: number;
    rawRelWeightSum: number;
    preShrinkDeltaSum: number;
  };
  const newCounter = (): Counter => ({
    votes: 0,
    exact: 0,
    fallback: 0,
    neutral: 0,
    lanePresent: 0,
    laneNonNeutral: 0,
    relWeightSum: 0,
    sampleConfSum: 0,
    rawRelWeightSum: 0,
    preShrinkDeltaSum: 0,
  });
  const byVersionFamily = new Map<string, Counter>();
  const byVersionLane = new Map<string, Counter>();
  const byVersionSampleBucket = new Map<string, Counter>();
  const normalization = new Map<string, number>();
  const byVersionScores = new Map<string, Array<{ score: number; hit: boolean }>>();

  for (const row of rows) {
    const version = row.voteWeightingVersion || "legacy";
    const laneTag = row.laneTag ?? "unknown";
    const regimeTags = Array.isArray(row.regimeTags)
      ? (row.regimeTags as string[]).filter((x) => typeof x === "string")
      : [];
    const laneKey = laneKeyFromTags({ laneTag, regimeTags });
    const uncertainty = row.uncertaintyScore ?? 0;
    const score =
      version === "weighted_v1"
        ? (row.weightedSupportScore ?? row.confidenceScore)
        : row.confidenceScore;
    const rowScore = byVersionScores.get(version) ?? [];
    rowScore.push({ score, hit: row.settledHit });
    byVersionScores.set(version, rowScore);

    const votes = parseModelVotes(row.modelVotes);
    for (const v of votes) {
      const rawSource = String(v.model_id ?? "unknown");
      const sourceFamily = normalizeVoteSourceFromModelId(rawSource);
      const normKey = `${rawSource} -> ${sourceFamily}`;
      normalization.set(normKey, (normalization.get(normKey) ?? 0) + 1);

      const laneSpecificKey = `${sourceFamily}::${laneKey}`;
      const laneWideKey = `${sourceFamily}::${laneTag}`;
      const globalKey = `${sourceFamily}::global`;
      const laneSpecific = sourceReliabilityMap[laneSpecificKey];
      const laneWide = sourceReliabilityMap[laneWideKey];
      const global = sourceReliabilityMap[globalKey];
      const rel = laneSpecific ?? laneWide ?? global ?? null;

      const rawRelWeight = rel ? reliabilityWeightFromHitRate(rel.hitRate) : 1;
      const sampleConfidenceWeight = rel ? clamp(rel.sampleSize / minSample, 0, 1) : 1;
      const reliabilityWeight = 1 + (rawRelWeight - 1) * sampleConfidenceWeight;
      const laneFitWeight = laneSpecific || laneWide ? 1.05 : 1;
      const uncertaintyDiscount = clamp(1 - clamp(uncertainty, 0, 1) * 0.35, 0.65, 1);
      void uncertaintyDiscount;

      const update = (map: Map<string, Counter>, key: string) => {
        const c = map.get(key) ?? newCounter();
        c.votes += 1;
        if (laneSpecific || laneWide) c.exact += 1;
        else if (global) c.fallback += 1;
        else c.neutral += 1;
        if (laneTag !== "unknown") c.lanePresent += 1;
        if (laneFitWeight !== 1) c.laneNonNeutral += 1;
        c.relWeightSum += reliabilityWeight;
        c.sampleConfSum += sampleConfidenceWeight;
        c.rawRelWeightSum += rawRelWeight;
        c.preShrinkDeltaSum += Math.abs(rawRelWeight - reliabilityWeight);
        map.set(key, c);
      };
      update(byVersionFamily, `${version}::${sourceFamily}`);
      update(byVersionLane, `${version}::${laneTag}`);
      update(byVersionSampleBucket, `${version}::${sampleBucket(rel?.sampleSize ?? null)}`);
    }
  }

  const printCoverage = (title: string, map: Map<string, Counter>, keyHeader: string) => {
    console.log(`\n${title}`);
    console.log(`${keyHeader} | votes | exact lane match % | fallback match % | neutral fallback % | avg reliabilityWeight`);
    const entries = [...map.entries()].sort((a, b) => b[1].votes - a[1].votes);
    for (const [k, v] of entries) {
      const denom = Math.max(1, v.votes);
      console.log(
        `${k} | ${v.votes} | ${((v.exact / denom) * 100).toFixed(1)}% | ${((v.fallback / denom) * 100).toFixed(1)}% | ${((v.neutral / denom) * 100).toFixed(1)}% | ${(v.relWeightSum / denom).toFixed(4)}`,
      );
    }
  };

  console.log(`\n=== Vote Weight Input Audit (${since}..${until}) window=${windowKey} ===`);
  console.log(`snapshot rows loaded: ${snapshot?.rows.length ?? 0}`);
  printCoverage("Reliability coverage by sourceFamily", byVersionFamily, "version::sourceFamily");
  printCoverage("Reliability coverage by laneTag", byVersionLane, "version::laneTag");
  printCoverage("Reliability coverage by sample bucket", byVersionSampleBucket, "version::sampleBucket");

  console.log("\nSample shrinkage audit (by sample bucket):");
  console.log("version::sampleBucket | votes | avg sampleConfidenceWeight | avg rawReliabilityWeight | avg postShrinkReliabilityWeight | avg |raw-post|");
  for (const [k, v] of [...byVersionSampleBucket.entries()].sort((a, b) => b[1].votes - a[1].votes)) {
    const d = Math.max(1, v.votes);
    console.log(
      `${k} | ${v.votes} | ${(v.sampleConfSum / d).toFixed(4)} | ${(v.rawRelWeightSum / d).toFixed(4)} | ${(v.relWeightSum / d).toFixed(4)} | ${(v.preShrinkDeltaSum / d).toFixed(4)}`,
    );
  }

  console.log("\nSource-family normalization audit (raw -> normalized):");
  const normalizationRows = [...normalization.entries()].sort((a, b) => b[1] - a[1]);
  for (const [k, c] of normalizationRows.slice(0, 40)) {
    console.log(`${k} | ${c}`);
  }
  const totalNormalized = normalizationRows.reduce((s, [, c]) => s + c, 0);
  const unknownNormalized = normalizationRows
    .filter(([k]) => k.endsWith("-> unknown"))
    .reduce((s, [, c]) => s + c, 0);
  console.log(`unknown normalized share: ${totalNormalized > 0 ? ((unknownNormalized / totalNormalized) * 100).toFixed(2) : "0.00"}%`);

  console.log("\nLane-fit diagnosis:");
  console.log("version::laneTag | votes | laneTag present % | laneFitWeight!=1 %");
  for (const [k, v] of [...byVersionLane.entries()].sort((a, b) => b[1].votes - a[1].votes)) {
    const d = Math.max(1, v.votes);
    console.log(`${k} | ${v.votes} | ${((v.lanePresent / d) * 100).toFixed(1)}% | ${((v.laneNonNeutral / d) * 100).toFixed(1)}%`);
  }

  console.log("\nNormalized support (percentile rank within version):");
  for (const [version, vals] of byVersionScores.entries()) {
    const sorted = [...vals].sort((a, b) => a.score - b.score);
    const ranked = sorted.map((v, i) => ({
      normalized: sorted.length <= 1 ? 1 : i / (sorted.length - 1),
      hit: v.hit,
    }));
    const summarizeTop = (cut: number) => {
      const slice = ranked.filter((r) => r.normalized >= cut);
      const wins = slice.filter((r) => r.hit).length;
      return {
        n: slice.length,
        winRate: slice.length > 0 ? wins / slice.length : 0,
      };
    };
    const p70 = summarizeTop(0.7);
    const p80 = summarizeTop(0.8);
    const p90 = summarizeTop(0.9);
    console.log(
      `${version}: p70 n=${p70.n} win=${safePct(p70.winRate)} | p80 n=${p80.n} win=${safePct(p80.winRate)} | p90 n=${p90.n} win=${safePct(p90.winRate)}`,
    );
  }
  console.log("");
}
