import { prisma } from "@bluey/db";
import type { GameContext } from "@bluey/db";
import type { BetFamily } from "./productionPickSelection";
import { impliedProbFromAmerican, payoutFromAmerican } from "./productionPickSelection";
import { normalizeVoteSourceFromModelId } from "./voteWeighting";

export type MarketFamily = "moneyline" | "spread" | "total" | "player_prop" | "other";

export type CalibratorBin = {
  min: number;
  max: number;
  sampleSize: number;
  actualHitRate: number;
};

export type CalibrationArtifact = {
  laneTag: string;
  marketFamily: MarketFamily | "global";
  minSample: number;
  sampleSize: number;
  bins: CalibratorBin[];
  createdAtIso: string;
};

export type SourceReliabilityRow = {
  sourceFamily: string;
  marketFamily: string;
  laneTag?: string | null;
  sampleSize: number;
  hitRate: number;
  asOfDate: string;
};

export type SourceReliabilitySnapshot = {
  rows: SourceReliabilityRow[];
};

export type PickQualityContext = {
  rawWinProbability: number;
  calibratedWinProbability: number;
  impliedMarketProbability: number | null;
  edgeVsMarket: number | null;
  expectedValueScore: number | null;
  marketType: MarketFamily;
  marketSubType: string | null;
  selectionSide: "home" | "away" | "over" | "under" | "player_over" | "other";
  lineSnapshot: number | null;
  priceSnapshot: number | null;
  laneTag: string;
  regimeTags: string[];
  sourceReliabilityScore: number | null;
  uncertaintyScore: number;
  uncertaintyPenaltyApplied: number;
  adjustedEdgeScore: number | null;
};

function clamp01(n: number): number {
  return Math.max(0, Math.min(1, n));
}

export function deriveMarketFamily(marketType: string, outcomeType: string): MarketFamily {
  const m = marketType.toLowerCase();
  if (m.includes("moneyline")) return "moneyline";
  if (m.includes("spread")) return "spread";
  if (m.includes("total")) return "total";
  if (m.includes("player")) return "player_prop";
  const base = outcomeType.replace(/:.*$/, "");
  if (base.includes("WIN")) return "moneyline";
  if (base.includes("COVERED")) return "spread";
  if (base.includes("OVER") || base.includes("UNDER")) return "total";
  if (base.includes("PLAYER") || base.includes("TOP_")) return "player_prop";
  return "other";
}

export function deriveLaneTag(outcomeType: string, market: string | null): string {
  const base = outcomeType.replace(/:.*$/, "");
  if (market === "player_points") return "player_points";
  if (market === "player_rebounds") return "player_rebounds";
  if (market === "player_assists") return "player_assists";
  if (market === "player_threes") return "player_threes";
  if (market?.startsWith("moneyline")) return "moneyline";
  if (market?.startsWith("spread")) return "spread";
  if (market?.startsWith("total")) return "total";
  if (base.includes("WIN")) return "moneyline";
  if (base.includes("COVERED")) return "spread";
  if (base.includes("OVER") || base.includes("UNDER")) return "total";
  if (base.includes("PLAYER") || base.includes("TOP_")) return "other_prop";
  return "other";
}

export function deriveSelectionSide(outcomeType: string, market: string | null): PickQualityContext["selectionSide"] {
  const base = outcomeType.replace(/:.*$/, "");
  if (market === "moneyline_home" || base === "HOME_WIN" || base === "HOME_COVERED") return "home";
  if (market === "moneyline_away" || base === "AWAY_WIN" || base === "AWAY_COVERED") return "away";
  if (market?.includes("under") || base.includes("UNDER")) return "under";
  if (market?.includes("over") || base.includes("OVER")) return "over";
  if (market?.startsWith("player_")) return "player_over";
  return "other";
}

export function deriveRegimeTags(args: { gameContext: GameContext; season: number }): string[] {
  const tags: string[] = [];
  const gc = args.gameContext;
  const totalGamesEstimate = (gc.homeWins + gc.homeLosses + gc.awayWins + gc.awayLosses) / 2;
  if (totalGamesEstimate < 20) tags.push("early_season");
  else if (totalGamesEstimate < 55) tags.push("mid_season");
  else tags.push("late_season");
  const restDiff = (gc.homeRestDays ?? 1) - (gc.awayRestDays ?? 1);
  if (Math.abs(restDiff) >= 2) tags.push(restDiff > 0 ? "rest_advantage_home" : "rest_advantage_away");
  if (gc.homeIsB2b || gc.awayIsB2b) tags.push("back_to_back");
  const injuryLoad =
    (gc.homeInjuryOutCount ?? 0) + (gc.homeInjuryDoubtfulCount ?? 0) + (gc.awayInjuryOutCount ?? 0) + (gc.awayInjuryDoubtfulCount ?? 0);
  if (injuryLoad >= 5) tags.push("high_injury_environment");
  const lineupStable =
    (gc.homeLineupCertainty ?? 0.75) >= 0.75 &&
    (gc.awayLineupCertainty ?? 0.75) >= 0.75 &&
    (gc.homeLateScratchRisk ?? 0.2) <= 0.25 &&
    (gc.awayLateScratchRisk ?? 0.2) <= 0.25;
  tags.push(lineupStable ? "lineup_stable" : "lineup_unstable");
  return tags;
}

export function calibrateProbability(args: {
  rawProbability: number;
  laneTag: string;
  marketFamily: MarketFamily;
  artifacts: CalibrationArtifact[];
  minSample: number;
}): { calibratedProbability: number; calibratorKey: string | null } {
  const raw = clamp01(args.rawProbability);
  const byPriority = [
    (a: CalibrationArtifact) => a.laneTag === args.laneTag && a.sampleSize >= args.minSample,
    (a: CalibrationArtifact) => a.marketFamily === args.marketFamily && a.sampleSize >= args.minSample,
    (a: CalibrationArtifact) => a.marketFamily === "global" && a.sampleSize >= args.minSample,
  ];
  const match = byPriority
    .map((pred) => args.artifacts.find(pred))
    .find((x): x is CalibrationArtifact => x != null);
  if (!match || match.bins.length === 0) {
    return { calibratedProbability: raw, calibratorKey: null };
  }
  const hitBin = match.bins.find((b) => raw >= b.min && raw < b.max) ?? null;
  if (!hitBin) return { calibratedProbability: raw, calibratorKey: `${match.laneTag}:${match.marketFamily}` };
  return {
    calibratedProbability: clamp01(hitBin.actualHitRate),
    calibratorKey: `${match.laneTag}:${match.marketFamily}`,
  };
}

export function computeSourceReliabilityScore(args: {
  sourceFamilies: string[];
  marketFamily: MarketFamily;
  snapshot: SourceReliabilitySnapshot | null;
  minSample: number;
}): number | null {
  if (!args.snapshot || args.snapshot.rows.length === 0 || args.sourceFamilies.length === 0) return null;
  const values: number[] = [];
  for (const sf of args.sourceFamilies) {
    const specific = args.snapshot.rows.find(
      (r) => r.sourceFamily === sf && r.marketFamily === args.marketFamily && r.sampleSize >= args.minSample,
    );
    const fallback = args.snapshot.rows.find(
      (r) => r.sourceFamily === sf && r.marketFamily === "global" && r.sampleSize >= args.minSample,
    );
    const row = specific ?? fallback;
    if (row) values.push(clamp01(row.hitRate));
  }
  if (values.length === 0) return null;
  return clamp01(values.reduce((a, b) => a + b, 0) / values.length);
}

export function computeUncertaintyScore(args: {
  gameContext: GameContext;
  supportCount: number;
  activeVotes: number;
  yesVotes: number;
  sourceReliabilityScore: number | null;
}): number {
  const lineupUncertainty =
    1 -
    ((args.gameContext.homeLineupCertainty ?? 0.75) + (args.gameContext.awayLineupCertainty ?? 0.75)) / 2;
  const lateScratch = ((args.gameContext.homeLateScratchRisk ?? 0.2) + (args.gameContext.awayLateScratchRisk ?? 0.2)) / 2;
  const injuryLoad = Math.min(
    1,
    ((args.gameContext.homeInjuryOutCount ?? 0) +
      (args.gameContext.homeInjuryDoubtfulCount ?? 0) +
      (args.gameContext.awayInjuryOutCount ?? 0) +
      (args.gameContext.awayInjuryDoubtfulCount ?? 0)) / 10,
  );
  const sampleUncertainty = args.supportCount >= 120 ? 0 : 1 - args.supportCount / 120;
  const voteDisagreement =
    args.activeVotes <= 0 ? 0.5 : 1 - clamp01(args.yesVotes / args.activeVotes);
  const reliabilityUncertainty = args.sourceReliabilityScore == null ? 0.25 : 1 - args.sourceReliabilityScore;
  const score =
    lineupUncertainty * 0.25 +
    lateScratch * 0.2 +
    injuryLoad * 0.15 +
    sampleUncertainty * 0.2 +
    voteDisagreement * 0.1 +
    reliabilityUncertainty * 0.1;
  return clamp01(score);
}

export function applyUncertaintyPenalty(args: {
  edgeVsMarket: number | null;
  uncertaintyScore: number;
  penaltyScale: number;
}): { uncertaintyPenaltyApplied: number; adjustedEdgeScore: number | null } {
  const penalty = args.uncertaintyScore * Math.max(0, args.penaltyScale);
  if (args.edgeVsMarket == null) return { uncertaintyPenaltyApplied: penalty, adjustedEdgeScore: null };
  return {
    uncertaintyPenaltyApplied: penalty,
    adjustedEdgeScore: args.edgeVsMarket - penalty,
  };
}

export function computeExpectedValueScore(args: {
  calibratedWinProbability: number;
  priceAmerican: number | null;
}): number | null {
  if (args.priceAmerican == null || !Number.isFinite(args.priceAmerican) || args.priceAmerican === 0) return null;
  const p = clamp01(args.calibratedWinProbability);
  return p * payoutFromAmerican(args.priceAmerican) - (1 - p);
}

export function buildPickQualityContext(args: {
  outcomeType: string;
  market: string | null;
  marketSubType: string | null;
  rawWinProbability: number;
  calibratedWinProbability: number;
  marketPriceAmerican: number | null;
  lineSnapshot: number | null;
  gameContext: GameContext;
  season: number;
  sourceReliabilityScore: number | null;
  supportCount: number;
  activeVotes: number;
  yesVotes: number;
  uncertaintyPenaltyScale: number;
}): PickQualityContext {
  const marketType = deriveMarketFamily(args.market ?? "other", args.outcomeType);
  const impliedMarketProbability = impliedProbFromAmerican(args.marketPriceAmerican);
  const edgeVsMarket =
    impliedMarketProbability == null ? null : args.calibratedWinProbability - impliedMarketProbability;
  const expectedValueScore = computeExpectedValueScore({
    calibratedWinProbability: args.calibratedWinProbability,
    priceAmerican: args.marketPriceAmerican,
  });
  const uncertaintyScore = computeUncertaintyScore({
    gameContext: args.gameContext,
    supportCount: args.supportCount,
    activeVotes: args.activeVotes,
    yesVotes: args.yesVotes,
    sourceReliabilityScore: args.sourceReliabilityScore,
  });
  const penalty = applyUncertaintyPenalty({
    edgeVsMarket,
    uncertaintyScore,
    penaltyScale: args.uncertaintyPenaltyScale,
  });
  return {
    rawWinProbability: clamp01(args.rawWinProbability),
    calibratedWinProbability: clamp01(args.calibratedWinProbability),
    impliedMarketProbability,
    edgeVsMarket,
    expectedValueScore,
    marketType,
    marketSubType: args.marketSubType,
    selectionSide: deriveSelectionSide(args.outcomeType, args.market),
    lineSnapshot: args.lineSnapshot,
    priceSnapshot: args.marketPriceAmerican,
    laneTag: deriveLaneTag(args.outcomeType, args.market),
    regimeTags: deriveRegimeTags({ gameContext: args.gameContext, season: args.season }),
    sourceReliabilityScore: args.sourceReliabilityScore,
    uncertaintyScore,
    uncertaintyPenaltyApplied: penalty.uncertaintyPenaltyApplied,
    adjustedEdgeScore: penalty.adjustedEdgeScore,
  };
}

export async function ensurePickQualityTables(): Promise<void> {
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "PredictionCalibration" (
      "id" text PRIMARY KEY,
      "laneTag" text NOT NULL,
      "marketFamily" text NOT NULL,
      "minSample" int NOT NULL,
      "sampleSize" int NOT NULL,
      "bins" jsonb NOT NULL,
      "createdAt" timestamptz NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(
    `CREATE INDEX IF NOT EXISTS "PredictionCalibration_lane_market_created_idx"
     ON "PredictionCalibration" ("laneTag","marketFamily","createdAt")`,
  );
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "PickSourceReliability" (
      "id" text PRIMARY KEY,
      "windowKey" text NOT NULL,
      "sourceFamily" text NOT NULL,
      "marketFamily" text NOT NULL,
      "laneTag" text,
      "sampleSize" int NOT NULL,
      "wins" int NOT NULL,
      "hitRate" double precision NOT NULL,
      "asOfDate" date NOT NULL,
      "createdAt" timestamptz NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(`ALTER TABLE "PickSourceReliability" ADD COLUMN IF NOT EXISTS "laneTag" text`);
  await prisma.$executeRawUnsafe(
    `CREATE INDEX IF NOT EXISTS "PickSourceReliability_window_source_market_idx"
     ON "PickSourceReliability" ("windowKey","sourceFamily","marketFamily","createdAt")`,
  );
}

export async function loadCalibrationArtifacts(): Promise<CalibrationArtifact[]> {
  await ensurePickQualityTables();
  const rows = await prisma.$queryRawUnsafe<
    Array<{
      laneTag: string;
      marketFamily: string;
      minSample: number;
      sampleSize: number;
      bins: unknown;
      createdAt: Date;
    }>
  >(
    `SELECT DISTINCT ON ("laneTag","marketFamily")
       "laneTag","marketFamily","minSample","sampleSize","bins","createdAt"
     FROM "PredictionCalibration"
     ORDER BY "laneTag","marketFamily","createdAt" DESC`,
  );
  return rows.map((r) => ({
    laneTag: r.laneTag,
    marketFamily: (r.marketFamily as MarketFamily | "global"),
    minSample: r.minSample,
    sampleSize: r.sampleSize,
    bins: (r.bins as CalibratorBin[]) ?? [],
    createdAtIso: r.createdAt.toISOString(),
  }));
}

export async function loadSourceReliabilitySnapshot(windowKey = "rolling_180"): Promise<SourceReliabilitySnapshot | null> {
  await ensurePickQualityTables();
  const rows = await prisma.$queryRawUnsafe<SourceReliabilityRow[]>(
    `SELECT DISTINCT ON ("sourceFamily","marketFamily",COALESCE("laneTag",''))
       "sourceFamily","marketFamily","laneTag","sampleSize","hitRate",to_char("asOfDate",'YYYY-MM-DD') AS "asOfDate"
     FROM "PickSourceReliability"
     WHERE "windowKey" = '${windowKey.replaceAll("'", "''")}'
     ORDER BY "sourceFamily","marketFamily",COALESCE("laneTag",''),"createdAt" DESC`,
  );
  if (rows.length === 0) return null;
  return { rows };
}

export function sourceFamilyFromModelId(modelId: string): string {
  return normalizeVoteSourceFromModelId(modelId);
}

export function marketFamilyFromBetFamily(family: BetFamily): MarketFamily {
  if (family === "MONEYLINE") return "moneyline";
  if (family === "SPREAD") return "spread";
  if (family === "TOTAL") return "total";
  if (family === "PLAYER") return "player_prop";
  return "other";
}
