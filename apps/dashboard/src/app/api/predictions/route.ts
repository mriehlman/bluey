import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import type { GameContext } from "@bluey/db";
import { getEasternDateFromUtc } from "@/lib/format";
import { GAME_EVENT_CATALOG } from "@bluey/core/features/gameEventCatalog";
import type { GameEventContext } from "@bluey/core/features/gameEventCatalog";
import { LEDGER_TUNING, PICK_QUALITY_TUNING } from "@bluey/core/config/tuning";
import {
  generateGamePredictions,
  evaluatePlayerPointsMlVote,
  parsePlayerOutcomeRequirement,
  loadLatestFeatureBins,
  payoutFromAmerican,
  type DeployedPatternV2,
  type V2PlayerTarget,
  type SuggestedMarketPick,
  type PlayerPropRow,
  type GateDiagnostics,
  type GamePlayerContext,
  type PlayerInfo,
} from "@bluey/core/features/predictionEngine";
import { loadCalibrationArtifacts, loadSourceReliabilitySnapshot } from "@bluey/core/features/pickQuality";
import {
  loadEarlyInjuriesForDate,
  buildTeamAliasLookup,
  computeLineupSignalsFromCounts,
} from "@bluey/core/features/injuryContext";
import { syncNbaStatsForDate, syncUpcomingFromNba } from "@bluey/core/ingest/syncNbaStats";
import { syncOddsForDate, syncPlayerPropsForDate } from "@bluey/core/ingest/syncOdds";
import { syncInjuries } from "@bluey/core/ingest/syncInjuries";
import { syncLineups } from "@bluey/core/ingest/syncLineups";
import * as path from "path";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type CanonicalPredictionRow = {
  predictionId: string;
  runId: string | null;
  runStartedAt: Date | string | null;
  gameId: string;
  market: string;
  selection: string;
  confidenceScore: number;
  edgeEstimate: number;
  generatedAt: Date | string;
  predictionContractVersion: string;
  modelBundleVersion: string;
  featureSchemaVersion: string;
  rankingPolicyVersion: string;
  aggregationPolicyVersion: string;
  featureSnapshotId: string;
  modelVotes: unknown;
  supportingPatterns: string[] | null;
  supportingPatternCount: number;
  featureSnapshotPayloadSummary: unknown;
  oddsTimestampUsed: string | null;
  statsSnapshotCutoff: string | null;
  injuryLineupCutoff: string | null;
};

async function canonicalPredictionTableExists(): Promise<boolean> {
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."CanonicalPrediction"') IS NOT NULL as "exists"`,
  );
  return Boolean(rows[0]?.exists);
}

async function handleCanonicalPredictionList(url: URL) {
  const exists = await canonicalPredictionTableExists();
  if (!exists) {
    return NextResponse.json({
      count: 0,
      predictions: [],
      message: "CanonicalPrediction table not found yet.",
    });
  }
  const dateStr = url.searchParams.get("date");
  const runId = url.searchParams.get("runId");
  const gameId = url.searchParams.get("gameId");
  const market = url.searchParams.get("market");
  const requestedLimit = Number.parseInt(url.searchParams.get("limit") ?? "200", 10);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0
    ? Math.min(1000, requestedLimit)
    : 200;

  const where: string[] = ["1=1"];
  if (dateStr) where.push(`cp."generatedAt"::date = '${sqlEsc(dateStr)}'::date`);
  if (runId) where.push(`cp."runId" = '${sqlEsc(runId)}'`);
  if (gameId) where.push(`cp."gameId" = '${sqlEsc(gameId)}'`);
  if (market) where.push(`cp."market" = '${sqlEsc(market)}'`);

  try {
    const rows = await prisma.$queryRawUnsafe<CanonicalPredictionRow[]>(
      `SELECT
      cp."predictionId",
      cp."runId",
      cp."runStartedAt",
      cp."gameId",
      cp."market",
      cp."selection",
      cp."confidenceScore",
      cp."edgeEstimate",
      cp."generatedAt",
      cp."predictionContractVersion",
      cp."modelBundleVersion",
      cp."featureSchemaVersion",
      cp."rankingPolicyVersion",
      cp."aggregationPolicyVersion",
      cp."featureSnapshotId",
      cp."modelVotes",
      cp."supportingPatterns",
      COALESCE(array_length(cp."supportingPatterns", 1), 0)::int as "supportingPatternCount",
      jsonb_build_object(
        'feature_schema_version', cp."featureSnapshotPayload"->>'feature_schema_version',
        'token_count', COALESCE(jsonb_array_length(cp."featureSnapshotPayload"->'tokens'), 0),
        'generated_from', cp."featureSnapshotPayload"->>'generated_from'
      ) as "featureSnapshotPayloadSummary",
      cp."featureSnapshotPayload"->>'odds_timestamp_used' as "oddsTimestampUsed",
      cp."featureSnapshotPayload"->>'stats_snapshot_cutoff' as "statsSnapshotCutoff",
      cp."featureSnapshotPayload"->>'injury_lineup_cutoff' as "injuryLineupCutoff"
     FROM "CanonicalPrediction" cp
     WHERE ${where.join(" AND ")}
     ORDER BY cp."generatedAt" DESC, cp."gameId" ASC
     LIMIT ${limit}`,
    );

    return NextResponse.json({
      date: dateStr,
      runId,
      gameId,
      market,
      count: rows.length,
      predictions: rows.map((row) => ({
        predictionId: row.predictionId,
        runId: row.runId,
        runStartedAt: row.runStartedAt,
        gameId: row.gameId,
        market: row.market,
        selection: row.selection,
        confidenceScore: row.confidenceScore,
        edgeEstimate: row.edgeEstimate,
        generatedAt: row.generatedAt,
        predictionContractVersion: row.predictionContractVersion,
        modelBundleVersion: row.modelBundleVersion,
        featureSchemaVersion: row.featureSchemaVersion,
        rankingPolicyVersion: row.rankingPolicyVersion,
        aggregationPolicyVersion: row.aggregationPolicyVersion,
        featureSnapshotId: row.featureSnapshotId,
        modelVotes: row.modelVotes,
        supportingPatternCount: row.supportingPatternCount,
        supportingPatterns: row.supportingPatterns ?? [],
        featureSnapshotPayloadSummary: row.featureSnapshotPayloadSummary,
        sourceTimeMetadata: {
          oddsTimestampUsed: row.oddsTimestampUsed,
          statsSnapshotCutoff: row.statsSnapshotCutoff,
          injuryLineupCutoff: row.injuryLineupCutoff,
        },
      })),
    });
  } catch (error) {
    try {
      const legacyWhere: string[] = ["1=1"];
      if (dateStr) legacyWhere.push(`cp."generatedAt"::date = '${sqlEsc(dateStr)}'::date`);
      if (gameId) legacyWhere.push(`cp."gameId" = '${sqlEsc(gameId)}'`);
      if (market) legacyWhere.push(`cp."market" = '${sqlEsc(market)}'`);

      const legacyRows = await prisma.$queryRawUnsafe<Array<{
        predictionId: string;
        gameId: string;
        market: string;
        selection: string;
        confidenceScore: number;
        edgeEstimate: number;
        generatedAt: Date | string;
        predictionContractVersion: string;
        modelBundleVersion: string;
        featureSchemaVersion: string;
        rankingPolicyVersion: string;
        aggregationPolicyVersion: string;
        featureSnapshotId: string;
        modelVotes: unknown;
        supportingPatterns: string[] | null;
        supportingPatternCount: number;
        featureSnapshotPayloadSummary: unknown;
        oddsTimestampUsed: string | null;
        statsSnapshotCutoff: string | null;
        injuryLineupCutoff: string | null;
      }>>(
        `SELECT
          cp."predictionId",
          cp."gameId",
          cp."market",
          cp."selection",
          cp."confidenceScore",
          cp."edgeEstimate",
          cp."generatedAt",
          cp."predictionContractVersion",
          cp."modelBundleVersion",
          cp."featureSchemaVersion",
          cp."rankingPolicyVersion",
          cp."aggregationPolicyVersion",
          cp."featureSnapshotId",
          cp."modelVotes",
          cp."supportingPatterns",
          COALESCE(array_length(cp."supportingPatterns", 1), 0)::int as "supportingPatternCount",
          jsonb_build_object(
            'feature_schema_version', cp."featureSnapshotPayload"->>'feature_schema_version',
            'token_count', COALESCE(jsonb_array_length(cp."featureSnapshotPayload"->'tokens'), 0),
            'generated_from', cp."featureSnapshotPayload"->>'generated_from'
          ) as "featureSnapshotPayloadSummary",
          cp."featureSnapshotPayload"->>'odds_timestamp_used' as "oddsTimestampUsed",
          cp."featureSnapshotPayload"->>'stats_snapshot_cutoff' as "statsSnapshotCutoff",
          cp."featureSnapshotPayload"->>'injury_lineup_cutoff' as "injuryLineupCutoff"
         FROM "CanonicalPrediction" cp
         WHERE ${legacyWhere.join(" AND ")}
         ORDER BY cp."generatedAt" DESC, cp."gameId" ASC
         LIMIT ${limit}`,
      );

      return NextResponse.json({
        date: dateStr,
        runId,
        gameId,
        market,
        count: legacyRows.length,
        predictions: legacyRows.map((row) => ({
          predictionId: row.predictionId,
          runId: null,
          runStartedAt: null,
          gameId: row.gameId,
          market: row.market,
          selection: row.selection,
          confidenceScore: row.confidenceScore,
          edgeEstimate: row.edgeEstimate,
          generatedAt: row.generatedAt,
          predictionContractVersion: row.predictionContractVersion,
          modelBundleVersion: row.modelBundleVersion,
          featureSchemaVersion: row.featureSchemaVersion,
          rankingPolicyVersion: row.rankingPolicyVersion,
          aggregationPolicyVersion: row.aggregationPolicyVersion,
          featureSnapshotId: row.featureSnapshotId,
          modelVotes: row.modelVotes,
          supportingPatternCount: row.supportingPatternCount,
          supportingPatterns: row.supportingPatterns ?? [],
          featureSnapshotPayloadSummary: row.featureSnapshotPayloadSummary,
          sourceTimeMetadata: {
            oddsTimestampUsed: row.oddsTimestampUsed,
            statsSnapshotCutoff: row.statsSnapshotCutoff,
            injuryLineupCutoff: row.injuryLineupCutoff,
          },
        })),
        message: runId
          ? "Run filter unavailable for legacy records (run columns missing)."
          : "Loaded legacy canonical predictions (run columns missing).",
      });
    } catch (fallbackError) {
      return NextResponse.json({
        date: dateStr,
        runId,
        gameId,
        market,
        count: 0,
        predictions: [],
        message:
          fallbackError instanceof Error
            ? `Governance list unavailable: ${fallbackError.message}`
            : error instanceof Error
              ? `Governance list unavailable: ${error.message}`
              : "Governance list unavailable.",
      });
    }
  }
}

async function loadActiveModelVersionFromDb() {
  try {
    const active = await prisma.modelVersion.findFirst({
      where: { isActive: true },
    });
    if (!active) return null;
    return {
      name: active.name ?? null,
      deployedPatterns: active.deployedPatterns as unknown as DeployedPatternV2[],
      featureBins: active.featureBins as unknown as Record<string, any>,
      metaModel: active.metaModel as unknown as any,
      tuningConfig: active.tuningConfig as unknown as any,
    };
  } catch {
    return null;
  }
}

let suggestedPlayLedgerAvailableCache: boolean | null = null;
let suggestedPlayLedgerColumnsEnsured = false;
let suggestedPlayLedgerScopeColumnsEnsured = false;
type WagerSummary = {
  bets: number;
  settledBets: number;
  pendingBets: number;
  wins: number;
  losses: number;
  totalStaked: number;
  settledStaked: number;
  netPnl: number;
  roi: number | null;
};

interface TeamSnapshot {
  wins: number;
  losses: number;
  ppg: number;
  oppg: number;
  pace: number | null;
  rebPg: number | null;
  astPg: number | null;
  fg3Pct: number | null;
  ftPct: number | null;
  rankOff: number | null;
  rankDef: number | null;
  rankPace: number | null;
  streak: number;
  lastGameDate: Date | null;
}

type OutcomeEval = {
  hit: boolean;
  explanation: string | null;
  scope: "target" | "outcome";
};

type DayBetSummary = {
  hits: number;
  total: number;
  hitRate: number | null;
};


const OUTCOME_EVENT_DEFS = GAME_EVENT_CATALOG.filter((d) => d.type === "outcome");

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function hasCompletedScore(game: {
  homeScore: number;
  awayScore: number;
  status: string | null;
}): boolean {
  const hasAnyScore = (game.homeScore ?? 0) > 0 || (game.awayScore ?? 0) > 0;
  return !!game.status?.includes("Final") && hasAnyScore;
}

/** Look up outcome in gameOutcomeMap. Keys are stored as eventKey:side (e.g. AWAY_WIN:game). */
function resolveOutcomeResult(
  gameOutcomeMap: Map<string, { hit: true; meta: unknown }>,
  outcome: string,
): { hit: true; meta: unknown } | undefined {
  if (gameOutcomeMap.has(outcome)) return gameOutcomeMap.get(outcome);
  const baseOutcome = outcome.replace(/:.*$/, "");
  for (const side of ["game", "home", "away"]) {
    const key = `${baseOutcome}:${side}`;
    if (gameOutcomeMap.has(key)) return gameOutcomeMap.get(key);
  }
  return undefined;
}

function buildOutcomeExplanation(
  meta: unknown,
  playerMap: Map<number, string>,
): string | null {
  if (!meta || typeof meta !== "object") return null;
  const obj = meta as Record<string, unknown>;
  const parts: string[] = [];
  if (obj.playerId && typeof obj.playerId === "number") {
    parts.push(playerMap.get(obj.playerId) ?? `Player ${obj.playerId}`);
  }
  if (obj.points !== undefined) parts.push(`${obj.points} pts`);
  if (obj.rebounds !== undefined) parts.push(`${obj.rebounds} reb`);
  if (obj.assists !== undefined) parts.push(`${obj.assists} ast`);
  if (obj.fg3m !== undefined) parts.push(`${obj.fg3m} 3PM`);
  if (obj.actual !== undefined && obj.line !== undefined) {
    parts.push(`${obj.actual} total (line: ${obj.line})`);
  }
  if (typeof obj.margin === "number" && typeof obj.spread === "number") {
    parts.push(
      `margin ${obj.margin > 0 ? "+" : ""}${obj.margin} (spread: ${obj.spread > 0 ? "+" : ""}${obj.spread})`,
    );
  }
  return parts.length > 0 ? parts.join(", ") : null;
}

function computeOutcomeFromFinalScore(
  outcomeKey: string,
  game: { homeScore: number; awayScore: number; status: string | null },
  odds?: { spreadHome: number | null; totalOver: number | null } | null,
): { hit: boolean; explanation: string } | null {
  const base = outcomeKey.replace(/:.*$/, "");
  const isFinal = hasCompletedScore(game);
  if (!isFinal) return null;

  const home = game.homeScore ?? 0;
  const away = game.awayScore ?? 0;
  const total = home + away;
  const marginHome = home - away; // positive => home won by X
  const absMargin = Math.abs(marginHome);

  // Moneyline
  if (base === "HOME_WIN") {
    const hit = home > away;
    return { hit, explanation: `Final score ${away}-${home}` };
  }
  if (base === "AWAY_WIN") {
    const hit = away > home;
    return { hit, explanation: `Final score ${away}-${home}` };
  }

  // Threshold totals (e.g., TOTAL_OVER_220)
  const mOver = base.match(/^TOTAL_OVER_(\d+(?:\.\d+)?)$/);
  if (mOver) {
    const line = Number(mOver[1]);
    const hit = total > line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }
  const mUnder = base.match(/^TOTAL_UNDER_(\d+(?:\.\d+)?)$/);
  if (mUnder) {
    const line = Number(mUnder[1]);
    const hit = total < line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }

  // Totals from market line (requires odds)
  if (base === "OVER_HIT" && odds?.totalOver != null) {
    const line = odds.totalOver;
    const hit = total > line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }
  if (base === "UNDER_HIT" && odds?.totalOver != null) {
    const line = odds.totalOver;
    const hit = total < line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }

  // Spread outcomes (requires spreadHome; spreadHome applies to home team)
  if (odds?.spreadHome != null) {
    const spreadHome = odds.spreadHome;
    const homeCovers = home + spreadHome > away;
    const awayCovers = away > home + spreadHome;
    const push = !homeCovers && !awayCovers;

    if (base === "HOME_COVERED") {
      return { hit: homeCovers, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "AWAY_COVERED") {
      return { hit: awayCovers, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "FAVORITE_COVERED") {
      const homeFav = spreadHome < 0;
      const hit = homeFav ? homeCovers : awayCovers;
      return { hit, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "UNDERDOG_COVERED") {
      const homeDog = spreadHome > 0;
      const hit = homeDog ? homeCovers : awayCovers;
      return { hit, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
  }

  // Margin outcomes (no odds required)
  if (base === "MARGIN_UNDER_5") return { hit: absMargin < 5, explanation: `Final margin ${absMargin}` };
  if (base === "MARGIN_UNDER_10") return { hit: absMargin < 10, explanation: `Final margin ${absMargin}` };
  if (base === "BLOWOUT_20_PLUS") return { hit: absMargin >= 20, explanation: `Final margin ${absMargin}` };

  return null;
}

function computeOutcomeFromCatalog(
  outcomeKey: string,
  ctx: GameEventContext,
): { hit: boolean; meta: unknown } | null {
  const base = outcomeKey.replace(/:.*$/, "");
  const sideMatch = outcomeKey.match(/:([^:]+)$/);
  const preferredSide = sideMatch?.[1];
  const defs = OUTCOME_EVENT_DEFS.filter((d) => d.key === base);
  if (defs.length === 0) return null;
  for (const def of defs) {
    const sides = preferredSide && def.sides.includes(preferredSide as "home" | "away" | "game")
      ? [preferredSide as "home" | "away" | "game", ...def.sides.filter((s) => s !== preferredSide)]
      : def.sides;
    for (const side of sides) {
      const computed = def.compute(ctx, side);
      if (computed.hit) return { hit: true, meta: computed.meta ?? null };
    }
  }
  return { hit: false, meta: null };
}


function ledgerDedupKey(play: {
  outcomeType: string;
  playerTarget: V2PlayerTarget | null;
  marketPick?: SuggestedMarketPick | null;
}): string {
  const targetId = play.playerTarget?.id ?? 0;
  const market = play.marketPick?.market ?? "none";
  const line =
    typeof play.marketPick?.line === "number"
      ? play.marketPick.line.toFixed(3)
      : "none";
  const price = play.marketPick?.overPrice ?? 0;
  return `${play.outcomeType}|${targetId}|${market}|${line}|${price}`;
}


function buildTargetOutcomeExplanation(
  outcomeType: string,
  target: V2PlayerTarget | null,
  stats: { playerId: number; points: number; rebounds: number; assists: number; fg3m: number | null }[],
): string | null {
  if (!target) return null;
  const req = parsePlayerOutcomeRequirement(outcomeType);
  if (!req) return null;
  const row = stats.find((s) => s.playerId === target.id);
  if (!row) return `No box score found for ${target.name}.`;
  const rawActual =
    req.actualStat === "points" ? row.points :
    req.actualStat === "rebounds" ? row.rebounds :
    req.actualStat === "assists" ? row.assists :
    (row.fg3m ?? 0);
  const actual = Number.isFinite(rawActual) ? Number(rawActual) : 0;
  if (actual >= req.line) {
    return `${target.name}, ${actual} ${req.label}`;
  }
  return `${target.name}, ${actual} ${req.label} (needed ${req.line}+)`;
}

function evaluatePlayerPropMarketResult(args: {
  marketPick: SuggestedMarketPick | null | undefined;
  playerTarget: V2PlayerTarget | null;
  isFinal: boolean;
  stats: { playerId: number; points: number; rebounds: number; assists: number; fg3m: number | null }[];
}): OutcomeEval | null {
  if (!args.isFinal) return null;
  const marketPick = args.marketPick ?? null;
  if (!marketPick || marketPick.marketType !== "player_prop") return null;
  const side = marketPick.selectionSide === "under" ? "under" : "over";
  const line = marketPick.line;
  if (!Number.isFinite(line)) return null;
  const playerId = args.playerTarget?.id ?? (Number.isFinite(marketPick.playerId) ? marketPick.playerId : null);
  if (playerId == null) return null;
  const row = args.stats.find((s) => s.playerId === playerId);
  if (!row) {
    return null;
  }
  const actual =
    marketPick.market === "player_points"
      ? row.points
      : marketPick.market === "player_rebounds"
        ? row.rebounds
        : marketPick.market === "player_assists"
          ? row.assists
          : (row.fg3m ?? 0);
  const hit = side === "under" ? actual < line : actual > line;
  const suffix =
    marketPick.market === "player_points"
      ? "pts"
      : marketPick.market === "player_rebounds"
        ? "reb"
        : marketPick.market === "player_assists"
          ? "ast"
          : "3PM";
  const actor = args.playerTarget?.name ?? marketPick.playerName ?? `Player ${playerId}`;
  return {
    hit,
    scope: "target",
    explanation: `${actor}, ${actual} ${suffix} vs ${side} ${line}`,
  };
}


function isMissingTableError(err: unknown): boolean {
  const e = err as { code?: string; meta?: { code?: string } };
  return e?.code === "P2021" || (e?.code === "P2010" && e?.meta?.code === "42P01");
}

async function loadDeployedV2PatternsSafe(): Promise<DeployedPatternV2[]> {
  try {
    return await prisma.$queryRawUnsafe<DeployedPatternV2[]>(
      `SELECT "id","outcomeType","conditions","posteriorHitRate","edge","score","n"
       FROM "PatternV2"
       WHERE "status" = 'deployed'
       ORDER BY "score" DESC`,
    );
  } catch (err) {
    if (isMissingTableError(err)) {
      console.warn(
        "PatternV2 table is missing; continuing without v2 deployed matches.",
      );
      return [];
    }
    throw err;
  }
}

async function loadFeatureBinsSafe() {
  try {
    return await loadLatestFeatureBins(prisma);
  } catch (err) {
    if (isMissingTableError(err)) {
      console.warn(
        "FeatureBin table is missing; continuing with empty feature bins.",
      );
      return new Map();
    }
    throw err;
  }
}

function getRepoRoot(): string {
  const cwd = process.cwd();
  if (cwd.includes("apps") && cwd.includes("dashboard")) return path.resolve(cwd, "../..");
  if (cwd.endsWith("dashboard")) return path.resolve(cwd, "..");
  return cwd;
}

async function autoSyncForDate(dateStr: string): Promise<boolean> {
  console.log(`[predictions] Auto-syncing data for ${dateStr}...`);
  try {
    await syncNbaStatsForDate(dateStr);
  } catch (e) {
    console.warn("[predictions] Stats sync warning:", String(e).slice(0, 200));
  }
  try {
    await syncUpcomingFromNba(dateStr);
  } catch (e) {
    console.warn("[predictions] Games sync warning:", String(e).slice(0, 200));
  }
  try {
    await syncOddsForDate(dateStr);
  } catch (e) {
    console.warn("[predictions] Odds sync warning:", String(e).slice(0, 200));
  }
  try {
    await syncPlayerPropsForDate(dateStr);
  } catch (e) {
    console.warn("[predictions] Player props sync warning:", String(e).slice(0, 200));
  }
  try {
    await syncInjuries(["--date", dateStr]);
  } catch (e) {
    console.warn("[predictions] Injuries sync warning:", String(e).slice(0, 200));
  }
  try {
    await syncLineups(["--date", dateStr]);
  } catch (e) {
    console.warn("[predictions] Lineups sync warning:", String(e).slice(0, 200));
  }
  console.log(`[predictions] Auto-sync complete for ${dateStr}`);
  return true;
}

function isDateTodayOrFuture(dateStr: string): boolean {
  const todayStr = getEasternDateFromUtc(new Date());
  return dateStr >= todayStr;
}

async function queryGamesForDate(targetDate: Date, dateStr: string) {
  const dayMs = 86400000;
  const rangeStart = new Date(targetDate.getTime() - dayMs);
  const rangeEnd = new Date(targetDate.getTime() + dayMs);

  const gamesRaw = await prisma.game.findMany({
    where: { date: { gte: rangeStart, lte: rangeEnd } },
    include: {
      homeTeam: true,
      awayTeam: true,
      odds: true,
      context: true,
      playerStats: true,
    },
    orderBy: { tipoffTimeUtc: "asc" },
  });

  const oddsPriceRows = gamesRaw.length > 0
    ? await prisma.$queryRawUnsafe<Array<{
        id: string; spreadHomePrice: number | null; spreadAwayPrice: number | null;
        totalOverPrice: number | null; totalUnderPrice: number | null;
      }>>(
        `SELECT "id", "spreadHomePrice", "spreadAwayPrice", "totalOverPrice", "totalUnderPrice"
         FROM "GameOdds" WHERE "gameId" IN (${gamesRaw.map(g => `'${g.id}'`).join(",")})`,
      )
    : [];
  const oddsPriceMap = new Map(oddsPriceRows.map(r => [r.id, r]));
  for (const game of gamesRaw) {
    for (const o of game.odds) {
      const prices = oddsPriceMap.get(o.id);
      if (prices) Object.assign(o, prices);
    }
  }

  const gamesForDate = gamesRaw.filter((g) => {
    if (!g.tipoffTimeUtc) {
      const storedDate = g.date instanceof Date ? g.date.toISOString().slice(0, 10) : String(g.date).slice(0, 10);
      return storedDate === dateStr;
    }
    const easternDate = getEasternDateFromUtc(g.tipoffTimeUtc);
    return easternDate === dateStr;
  });

  const todayEastern = getEasternDateFromUtc(new Date());
  const isPastDate = dateStr < todayEastern;
  const activeGames = isPastDate
    ? gamesForDate.filter((g) => {
        const hasScore = (g.homeScore ?? 0) > 0 || (g.awayScore ?? 0) > 0;
        const isFinal = !!g.status?.includes("Final");
        if (!isFinal && !hasScore) return false;
        return true;
      })
    : gamesForDate;

  const seenMatchups = new Map<string, (typeof gamesForDate)[0]>();
  for (const g of activeGames) {
    const homeCode = g.homeTeam?.code ?? g.homeTeamId.toString();
    const awayCode = g.awayTeam?.code ?? g.awayTeamId.toString();
    const key = [homeCode, awayCode].sort().join(":");
    const existing = seenMatchups.get(key);
    if (!existing) {
      seenMatchups.set(key, g);
    } else {
      const gHasScore = (g.homeScore ?? 0) > 0 || (g.awayScore ?? 0) > 0;
      const existingHasScore = (existing.homeScore ?? 0) > 0 || (existing.awayScore ?? 0) > 0;
      const gIsFinal = !!g.status?.includes("Final");
      const existingIsFinal = !!existing.status?.includes("Final");
      const better =
        (gIsFinal && !existingIsFinal) ? g :
        (existingIsFinal && !gIsFinal) ? existing :
        (gHasScore && !existingHasScore) ? g :
        (existingHasScore && !gHasScore) ? existing :
        (g.context && !existing.context) ? g :
        (g.odds?.length && !existing.odds?.length) ? g :
        existing;
      seenMatchups.set(key, better);
    }
  }

  return [...seenMatchups.values()].sort(
    (a, b) => (a.tipoffTimeUtc?.getTime() ?? 0) - (b.tipoffTimeUtc?.getTime() ?? 0),
  );
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  if (url.searchParams.get("governance") === "1") {
    return handleCanonicalPredictionList(url);
  }
  try {
    return await predictionsMain(req);
  } catch (err) {
    console.error("[predictions] GET failed", err);
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: "predictions_failed", message }, { status: 500 });
  }
}

async function predictionsMain(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");
  const refreshLedger = url.searchParams.get("refreshLedger") === "1";
  const includeDebugPlays = url.searchParams.get("debugPlays") === "1";
  const gateModeRaw = (url.searchParams.get("gateMode") ?? "legacy").toLowerCase();
  const gateMode: "legacy" | "strict" = gateModeRaw === "strict" ? "strict" : "legacy";

  if (!dateStr) {
    const today = getEasternDateFromUtc(new Date());
    return NextResponse.redirect(new URL(`/api/predictions?date=${today}`, req.url));
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = getSeasonForDate(targetDate);
  const defaultStake = LEDGER_TUNING.stake;
  const bankrollStart = LEDGER_TUNING.bankrollStart;
  const maxBetPicksPerGame = Math.max(1, LEDGER_TUNING.maxBetPicksPerGame);
  const allowFallbackOddsForLedger = LEDGER_TUNING.allowFallbackOddsForLedger;
  const fallbackAmericanOdds = LEDGER_TUNING.fallbackAmericanOdds;
  let calibrationArtifacts: Awaited<ReturnType<typeof loadCalibrationArtifacts>> = [];
  let sourceReliabilitySnapshot: Awaited<ReturnType<typeof loadSourceReliabilitySnapshot>> = null;
  try {
    calibrationArtifacts = await loadCalibrationArtifacts();
    sourceReliabilitySnapshot = await loadSourceReliabilitySnapshot();
  } catch (e) {
    console.error("[predictions] calibration / reliability load failed:", e);
  }
  let seasonV2Hits = 0;
  let seasonV2Total = 0;
  let gateDiagnostics: GateDiagnostics | null = null;
  let wagerTracking: Awaited<ReturnType<typeof getWagerTrackingSummary>> = null;

  let games = await queryGamesForDate(targetDate, dateStr);
  let didAutoSync = false;

  // Auto-sync when no games found and date is today or future
  if (games.length === 0 && isDateTodayOrFuture(dateStr)) {
    console.log(`[predictions] No games found for ${dateStr}, triggering auto-sync...`);
    await autoSyncForDate(dateStr);
    games = await queryGamesForDate(targetDate, dateStr);
    didAutoSync = true;
  }

  if (games.length === 0) {
    return NextResponse.json({
      date: dateStr,
      season,
      seasonToDate: {
        throughDate: dateStr,
        v2: {
          hits: seasonV2Hits,
          total: seasonV2Total,
          hitRate: seasonV2Total > 0 ? seasonV2Hits / seasonV2Total : null,
        },
      },
      wagerTracking,
      games: [],
      message: "No games found",
    });
  }

  const gameIds = games.map((g) => g.id);
  const playerPropsRaw = await prisma.playerPropOdds.findMany({
    where: { gameId: { in: gameIds } },
    select: {
      gameId: true,
      playerId: true,
      market: true,
      line: true,
      overPrice: true,
      underPrice: true,
      player: { select: { firstname: true, lastname: true } },
    },
  });
  const playerPropsByGame = new Map<string, PlayerPropRow[]>();
  for (const gid of gameIds) playerPropsByGame.set(gid, []);
  for (const r of playerPropsRaw) {
    playerPropsByGame.get(r.gameId)?.push(r);
  }

  const activeVersion = await loadActiveModelVersionFromDb();
  const selectedModelVersionName = activeVersion?.name ?? "live";
  let deployedV2Patterns: DeployedPatternV2[];
  let featureBins: Map<string, any>;
  let activeMetaModel: any = null;

  if (activeVersion) {
    deployedV2Patterns = activeVersion.deployedPatterns;
    featureBins = new Map(Object.entries(activeVersion.featureBins));
    activeMetaModel = activeVersion.metaModel ?? null;
  } else {
    [deployedV2Patterns, featureBins] = await Promise.all([
      loadDeployedV2PatternsSafe(),
      loadFeatureBinsSafe(),
    ]);
  }

  try {
    const seasonBetSummary = await getSeasonBetHitSummary(
      dateStr,
      season,
      selectedModelVersionName,
      gateMode,
    );
    seasonV2Hits = seasonBetSummary.hits;
    seasonV2Total = seasonBetSummary.total;
  } catch (e) {
    console.error("[predictions] season bet summary failed:", e);
  }
  try {
    wagerTracking = await getWagerTrackingSummary({
      dateStr,
      season,
      modelVersionName: selectedModelVersionName,
      gateMode,
      stakePerPick: defaultStake,
      bankrollStart,
    });
  } catch (e) {
    console.error("[predictions] wager tracking (initial) failed:", e);
  }

  const teamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const teamSnapshots = await computeTeamSnapshots(season, targetDate, teamIds);

  // Load injury data for live context
  const allTeamsForInjury = await prisma.team.findMany({
    select: { id: true, city: true, name: true, code: true },
  });
  const teamAliasLookup = buildTeamAliasLookup(allTeamsForInjury);
  const injuryCache = new Map();
  const repoRoot = getRepoRoot();
  const injuryDataDir = path.join(repoRoot, "data");
  const injuriesByTeam = loadEarlyInjuriesForDate(dateStr, teamAliasLookup, injuryCache, injuryDataDir);
  if (injuriesByTeam.size > 0) {
    let injTotal = 0;
    for (const t of injuriesByTeam.values()) injTotal += t.out + t.doubtful + t.questionable + t.probable;
    console.log(`[predictions] Loaded injury data: ${injTotal} entries across ${injuriesByTeam.size} teams`);
  }

  // For completed games, fetch outcome events to show hit/miss status
  const completedGameIds = games
    .filter((g) => hasCompletedScore(g))
    .map((g) => g.id);

  const [gameOutcomes] = await Promise.all([
    completedGameIds.length > 0
      ? prisma.gameEvent.findMany({
          where: {
            gameId: { in: completedGameIds },
            type: "outcome",
          },
          select: {
            gameId: true,
            eventKey: true,
            side: true,
            meta: true,
          },
        })
      : [],
  ]);

  // Build lookup: gameId -> Set of outcome keys that hit (from GameEvent)
  const outcomesByGame = new Map<string, Map<string, { hit: true; meta: unknown }>>();
  for (const ev of gameOutcomes) {
    if (!outcomesByGame.has(ev.gameId)) {
      outcomesByGame.set(ev.gameId, new Map());
    }
    const key = `${ev.eventKey}:${ev.side}`;
    outcomesByGame.get(ev.gameId)!.set(key, { hit: true, meta: ev.meta });
  }

  // For player outcomes, fetch player names
  const playerIds = new Set<number>();
  for (const ev of gameOutcomes) {
    const meta = ev.meta as Record<string, unknown> | null;
    if (meta?.playerId && typeof meta.playerId === "number") {
      playerIds.add(meta.playerId);
    }
  }
  const players = playerIds.size > 0
    ? await prisma.player.findMany({
        where: { id: { in: [...playerIds] } },
        select: { id: true, firstname: true, lastname: true },
      })
    : [];
  const playerMap = new Map(players.map((p) => [p.id, `${p.firstname} ${p.lastname}`]));

  // Fetch player contexts for games (from DB), or compute on-the-fly for upcoming games
  const playerContexts = await prisma.playerGameContext.findMany({
    where: { gameId: { in: gameIds } },
    include: { player: true },
  });

  // Build team code -> all team IDs (for fallback when PlayerGameContext is empty)
  const scheduledTeams = await prisma.team.findMany({
    where: { id: { in: teamIds } },
    select: { id: true, code: true },
  });
  const teamCodes = [...new Set(scheduledTeams.map((t) => t.code).filter(Boolean))] as string[];
  const allTeamsWithCodes = await prisma.team.findMany({
    where: { code: { in: teamCodes } },
    select: { id: true, code: true },
  });
  const teamIdToAllIds = new Map<number, number[]>();
  for (const t of scheduledTeams) {
    if (!t.code) continue;
    const ids = allTeamsWithCodes.filter((x) => x.code === t.code).map((x) => x.id);
    teamIdToAllIds.set(t.id, ids);
  }

  const playerContextByGame = new Map<string, GamePlayerContext>();

  for (const game of games) {
    const contexts = playerContexts.filter((c) => c.gameId === game.id);
    const homeContexts = contexts.filter((c) => c.teamId === game.homeTeamId);
    const awayContexts = contexts.filter((c) => c.teamId === game.awayTeamId);

    const getTop = (list: typeof contexts, stat: "ppg" | "rpg" | "apg"): PlayerInfo | null => {
      const sorted = [...list].sort((a, b) => b[stat] - a[stat]);
      const top = sorted[0];
      return top ? { id: top.playerId, name: `${top.player.firstname} ${top.player.lastname}`, stat: top[stat] } : null;
    };

    // Use DB context if available; otherwise compute on-the-fly from PlayerGameStat
    if (contexts.length > 0) {
      playerContextByGame.set(game.id, {
        homeTopScorer: getTop(homeContexts, "ppg"),
        homeTopRebounder: getTop(homeContexts, "rpg"),
        homeTopPlaymaker: getTop(homeContexts, "apg"),
        awayTopScorer: getTop(awayContexts, "ppg"),
        awayTopRebounder: getTop(awayContexts, "rpg"),
        awayTopPlaymaker: getTop(awayContexts, "apg"),
      });
    } else {
      const fallback = await computePlayerContextFallback(
        prisma,
        season,
        targetDate,
        game.homeTeamId,
        game.awayTeamId,
        teamIdToAllIds,
      );
      playerContextByGame.set(game.id, fallback);
    }
  }

  const result = games.map((game) => {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    const consensus = game.odds.find((o) => o.source === "consensus") ?? game.odds[0];

    // Commercial odds excluded from public API; used internally for pattern matching only
    const context = {
      home: homeSnap
        ? {
            record: `${homeSnap.wins}-${homeSnap.losses}`,
            ppg: homeSnap.ppg,
            oppg: homeSnap.oppg,
            rankOff: homeSnap.rankOff,
            rankDef: homeSnap.rankDef,
            streak: homeSnap.streak,
          }
        : null,
      away: awaySnap
        ? {
            record: `${awaySnap.wins}-${awaySnap.losses}`,
            ppg: awaySnap.ppg,
            oppg: awaySnap.oppg,
            rankOff: awaySnap.rankOff,
            rankDef: awaySnap.rankDef,
            streak: awaySnap.streak,
          }
        : null,
    };

    const gamePlayerContexts = playerContexts.filter((c) => c.gameId === game.id);
    const gameEventContext = {
      game: game as unknown as GameEventContext["game"],
      context: (game.context ??
        {
          id: "",
          gameId: game.id,
          homeWins: homeSnap?.wins ?? 0,
          homeLosses: homeSnap?.losses ?? 0,
          homePpg: homeSnap?.ppg ?? 0,
          homeOppg: homeSnap?.oppg ?? 0,
          homePace: homeSnap?.pace ?? null,
          homeRebPg: homeSnap?.rebPg ?? null,
          homeAstPg: homeSnap?.astPg ?? null,
          homeFg3Pct: homeSnap?.fg3Pct ?? null,
          homeFtPct: homeSnap?.ftPct ?? null,
          homeRankOff: homeSnap?.rankOff ?? null,
          homeRankDef: homeSnap?.rankDef ?? null,
          homeRankPace: homeSnap?.rankPace ?? null,
          homeStreak: homeSnap?.streak ?? 0,
          awayWins: awaySnap?.wins ?? 0,
          awayLosses: awaySnap?.losses ?? 0,
          awayPpg: awaySnap?.ppg ?? 0,
          awayOppg: awaySnap?.oppg ?? 0,
          awayPace: awaySnap?.pace ?? null,
          awayRebPg: awaySnap?.rebPg ?? null,
          awayAstPg: awaySnap?.astPg ?? null,
          awayFg3Pct: awaySnap?.fg3Pct ?? null,
          awayFtPct: awaySnap?.ftPct ?? null,
          awayRankOff: awaySnap?.rankOff ?? null,
          awayRankDef: awaySnap?.rankDef ?? null,
          awayRankPace: awaySnap?.rankPace ?? null,
          awayStreak: awaySnap?.streak ?? 0,
          homeRestDays: homeSnap?.lastGameDate
            ? Math.max(0, Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1)
            : null,
          awayRestDays: awaySnap?.lastGameDate
            ? Math.max(0, Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1)
            : null,
          homeIsB2b: homeSnap?.lastGameDate
            ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1 === 0
            : false,
          awayIsB2b: awaySnap?.lastGameDate
            ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1 === 0
            : false,
          homeInjuryOutCount: injuriesByTeam.get(game.homeTeamId)?.out ?? null,
          homeInjuryDoubtfulCount: injuriesByTeam.get(game.homeTeamId)?.doubtful ?? null,
          homeInjuryQuestionableCount: injuriesByTeam.get(game.homeTeamId)?.questionable ?? null,
          homeInjuryProbableCount: injuriesByTeam.get(game.homeTeamId)?.probable ?? null,
          awayInjuryOutCount: injuriesByTeam.get(game.awayTeamId)?.out ?? null,
          awayInjuryDoubtfulCount: injuriesByTeam.get(game.awayTeamId)?.doubtful ?? null,
          awayInjuryQuestionableCount: injuriesByTeam.get(game.awayTeamId)?.questionable ?? null,
          awayInjuryProbableCount: injuriesByTeam.get(game.awayTeamId)?.probable ?? null,
          homeLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam.get(game.homeTeamId)).certainty,
          awayLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam.get(game.awayTeamId)).certainty,
          homeLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam.get(game.homeTeamId)).lateScratchRisk,
          awayLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam.get(game.awayTeamId)).lateScratchRisk,
          h2hHomeWins: 0,
          h2hAwayWins: 0,
        }) as GameEventContext["context"],
      playerContexts: gamePlayerContexts as unknown as GameEventContext["playerContexts"],
      stats: game.playerStats as unknown as GameEventContext["stats"],
      odds: (consensus ?? null) as GameEventContext["odds"],
    } as GameEventContext;
    const isFinal = hasCompletedScore(game);
    const gameOutcomeMap = outcomesByGame.get(game.id);
    const gamePlayerCtx = playerContextByGame.get(game.id);
    const effectiveGamePlayerCtx = gamePlayerCtx ?? {
      homeTopScorer: null, homeTopRebounder: null, homeTopPlaymaker: null,
      awayTopScorer: null, awayTopRebounder: null, awayTopPlaymaker: null,
    };
    const propsForGame = playerPropsByGame.get(game.id) ?? [];

    const engineOdds = consensus
      ? {
          spreadHome: consensus.spreadHome ?? null,
          spreadHomePrice: (consensus as Record<string, unknown>).spreadHomePrice as number | null ?? null,
          spreadAwayPrice: (consensus as Record<string, unknown>).spreadAwayPrice as number | null ?? null,
          totalOver: consensus.totalOver ?? null,
          totalOverPrice: (consensus as Record<string, unknown>).totalOverPrice as number | null ?? null,
          totalUnderPrice: (consensus as Record<string, unknown>).totalUnderPrice as number | null ?? null,
          mlHome: consensus.mlHome ?? null,
          mlAway: consensus.mlAway ?? null,
        }
      : null;

    const engineOutput = generateGamePredictions({
      season,
      gameContext: gameEventContext.context,
      odds: engineOdds,
      deployedV2Patterns,
      featureBins,
      metaModel: activeMetaModel,
      gamePlayerContext: effectiveGamePlayerCtx,
      propsForGame,
      maxBetPicksPerGame,
      fallbackAmericanOdds,
      includeDebugPlays,
      calibrationArtifacts,
      sourceReliabilitySnapshot,
      strictActionabilityGatesEnabled: gateMode === "strict",
      // Ledger refresh (e.g. backfill) should capture all pick types for simulator filtering
      overrideExcludeFamilies: refreshLedger ? [] : undefined,
    });

    if (engineOutput.gateDiagnostics) {
      if (!gateDiagnostics) {
        gateDiagnostics = engineOutput.gateDiagnostics;
      } else {
        for (const stage of ["quality", "bettable"] as const) {
          gateDiagnostics[stage].considered += engineOutput.gateDiagnostics[stage].considered;
          gateDiagnostics[stage].passed += engineOutput.gateDiagnostics[stage].passed;
          gateDiagnostics[stage].rejected += engineOutput.gateDiagnostics[stage].rejected;
          for (const [r, c] of Object.entries(engineOutput.gateDiagnostics[stage].reasons)) {
            gateDiagnostics[stage].reasons[r] = (gateDiagnostics[stage].reasons[r] ?? 0) + c;
          }
        }
      }
    }

    const evaluateOutcome = (outcomeKey: string, target: V2PlayerTarget | null): OutcomeEval | null => {
      if (!isFinal) return null;
      const outcomeResult = gameOutcomeMap ? resolveOutcomeResult(gameOutcomeMap, outcomeKey) : undefined;
      if (outcomeResult) {
        const meta = outcomeResult.meta as Record<string, unknown> | undefined;
        const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
        if (target && actualPlayerId != null) {
          const targetHit = actualPlayerId === target.id;
          const actualName = playerMap.get(actualPlayerId) ?? `Player ${actualPlayerId}`;
          return {
            hit: targetHit,
            scope: "target",
            explanation: targetHit
              ? buildOutcomeExplanation(outcomeResult.meta, playerMap)
              : `Outcome hit by ${actualName}, not predicted target ${target.name}.`,
          };
        }
        return {
          hit: true,
          scope: "outcome",
          explanation: buildOutcomeExplanation(outcomeResult.meta, playerMap),
        };
      }
      const catalogOutcome = computeOutcomeFromCatalog(outcomeKey, gameEventContext);
      if (catalogOutcome) {
        const meta = (catalogOutcome.meta ?? null) as Record<string, unknown> | null;
        const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
        if (target && actualPlayerId != null) {
          const targetHit = actualPlayerId === target.id;
          const actualName = playerMap.get(actualPlayerId) ?? `Player ${actualPlayerId}`;
          return {
            hit: targetHit,
            scope: "target",
            explanation: targetHit
              ? buildOutcomeExplanation(catalogOutcome.meta, playerMap) ??
                buildTargetOutcomeExplanation(outcomeKey, target, game.playerStats)
              : `Outcome hit by ${actualName}, not predicted target ${target.name}.`,
          };
        }
        const targetExplanation = buildTargetOutcomeExplanation(
          outcomeKey,
          target,
          game.playerStats,
        );
        return {
          hit: catalogOutcome.hit,
          scope: "outcome",
          explanation:
            targetExplanation ??
            buildOutcomeExplanation(catalogOutcome.meta, playerMap) ??
            computeOutcomeFromFinalScore(outcomeKey, game, consensus ?? null)?.explanation ??
            `Final score ${game.awayScore}-${game.homeScore}`,
        };
      }
      const computed = computeOutcomeFromFinalScore(outcomeKey, game, consensus ?? null);
      if (computed) {
        return {
          hit: computed.hit,
          scope: "outcome",
          explanation: computed.explanation,
        };
      }
      return null;
    };

    const enrichedDiscoveryV2Matches = engineOutput.discoveryV2Matches.map((p) => ({
      ...p,
      result: evaluateOutcome(p.outcomeType, p.playerTarget),
    }));
    const suggestedBetPicks = engineOutput.suggestedBetPicks.map((p) => ({
      ...p,
      result:
        evaluatePlayerPropMarketResult({
          marketPick: p.marketPick ?? null,
          playerTarget: p.playerTarget,
          isFinal,
          stats: game.playerStats,
        }) ?? evaluateOutcome(p.outcomeType, p.playerTarget),
      mlInvolved:
        evaluatePlayerPointsMlVote({
          outcomeType: p.outcomeType,
          playerTarget: p.playerTarget,
          marketPick: p.marketPick ?? null,
          gameContext: gameEventContext.context as GameContext,
          gamePlayerCtx: effectiveGamePlayerCtx,
        }).decision !== "abstain",
    }));
    const suggestedPlays = engineOutput.suggestedPlays.map((p) => ({
      ...p,
      result: evaluateOutcome(p.outcomeType, p.playerTarget),
    }));
    const modelPicks = engineOutput.modelPicks.map((p) => ({
      ...p,
      result: evaluateOutcome(p.outcomeType, p.playerTarget),
    }));

    return {
      id: game.id,
      homeTeam: { id: game.homeTeamId, code: game.homeTeam.code, name: game.homeTeam.name },
      awayTeam: { id: game.awayTeamId, code: game.awayTeam.code, name: game.awayTeam.name },
      tipoff: game.tipoffTimeUtc,
      status: game.status,
      homeScore: game.homeScore,
      awayScore: game.awayScore,
      odds: null,
      context,
      discoveryV2Matches: enrichedDiscoveryV2Matches,
      suggestedPlays,
      suggestedBetPicks,
      modelPicks,
    };
  });

  const dayBetSummary: DayBetSummary = (() => {
    let hits = 0;
    let total = 0;
    for (const game of result) {
      for (const play of game.suggestedBetPicks ?? []) {
        if (!play.result) continue;
        total += 1;
        if (play.result.hit) hits += 1;
      }
    }
    return {
      hits,
      total,
      hitRate: total > 0 ? hits / total : null,
    };
  })();

  try {
    const ledgerRunId = crypto.randomUUID();
    const shouldUpsert = await shouldUpsertLedgerSnapshot(
      dateStr,
      refreshLedger,
      selectedModelVersionName,
      gateMode,
    );
    if (shouldUpsert) {
      await upsertSuggestedPlayLedger({
        dateStr,
        season,
        runId: ledgerRunId,
        modelVersionName: selectedModelVersionName,
        gateMode,
        games: result,
        defaultStake,
        fallbackAmericanOdds,
        allowFallbackOddsForLedger,
      });
    }
    await voidStaleNonFinalPicks();
    wagerTracking = await getWagerTrackingSummary({
      dateStr,
      season,
      modelVersionName: selectedModelVersionName,
      gateMode,
      stakePerPick: defaultStake,
      bankrollStart,
    });
  } catch (err) {
    // Ledger capture should not block predictions API responses.
    console.error("Failed to upsert SuggestedPlayLedger rows:", err);
  }

  return NextResponse.json({
    date: dateStr,
    season,
    modelVersion: selectedModelVersionName,
    gateMode,
    dayBetSummary,
    ...(includeDebugPlays ? { gateDiagnostics } : {}),
    ...(didAutoSync ? { autoSynced: true } : {}),
    seasonToDate: {
      throughDate: dateStr,
      v2: {
        hits: seasonV2Hits,
        total: seasonV2Total,
        hitRate: seasonV2Total > 0 ? seasonV2Hits / seasonV2Total : null,
      },
    },
    wagerTracking,
    games: result,
  });
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function sqlStr(v: string | null | undefined): string {
  return v == null ? "NULL" : `'${sqlEsc(v)}'`;
}

function sqlNum(v: number | null | undefined): string {
  return v == null || !Number.isFinite(v) ? "NULL" : String(v);
}

function sqlBool(v: boolean | null | undefined): string {
  return v == null ? "NULL" : v ? "TRUE" : "FALSE";
}

async function upsertSuggestedPlayLedger(args: {
  dateStr: string;
  season: number;
  runId: string;
  modelVersionName: string;
  gateMode: "legacy" | "strict";
  defaultStake: number;
  fallbackAmericanOdds: number;
  allowFallbackOddsForLedger: boolean;
  games: Array<{
    id: string;
    suggestedPlays: Array<{
      outcomeType: string;
      displayLabel: string | null;
      confidence: number;
      posteriorHitRate: number;
      edge: number;
      metaScore: number | null;
      votes: number;
      result: OutcomeEval | null;
      playerTarget: V2PlayerTarget | null;
      marketPick?: SuggestedMarketPick | null;
    }>;
    suggestedBetPicks?: Array<{
      outcomeType: string;
      displayLabel: string | null;
      confidence: number;
      posteriorHitRate: number;
      edge: number;
      metaScore: number | null;
      votes: number;
      mlInvolved?: boolean;
      rawWinProbability?: number;
      calibratedWinProbability?: number;
      impliedMarketProbability?: number | null;
      edgeVsMarket?: number | null;
      expectedValueScore?: number | null;
      marketType?: string;
      marketSubType?: string | null;
      selectionSide?: string;
      lineSnapshot?: number | null;
      priceSnapshot?: number | null;
      laneTag?: string;
      regimeTags?: string[];
      sourceReliabilityScore?: number | null;
      uncertaintyScore?: number;
      uncertaintyPenaltyApplied?: number;
      adjustedEdgeScore?: number | null;
      result: OutcomeEval | null;
      playerTarget: V2PlayerTarget | null;
      marketPick?: SuggestedMarketPick | null;
    }>;
  }>;
}): Promise<void> {
  if (!(await isSuggestedPlayLedgerAvailable())) return;
  const stake = Number.isFinite(args.defaultStake) ? args.defaultStake : 10;
  const fallbackOdds = Number.isFinite(args.fallbackAmericanOdds) &&
    args.fallbackAmericanOdds !== 0
    ? args.fallbackAmericanOdds
    : -110;
  if (!suggestedPlayLedgerColumnsEnsured) {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "mlInvolved" boolean NOT NULL DEFAULT FALSE`,
    );
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "rawWinProbability" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "calibratedWinProbability" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "impliedMarketProbability" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "edgeVsMarket" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "expectedValueScore" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "marketType" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "marketSubType" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "selectionSide" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "lineSnapshot" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "priceSnapshot" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "laneTag" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "regimeTags" text[]`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "sourceReliabilityScore" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "uncertaintyScore" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "uncertaintyPenaltyApplied" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "adjustedEdgeScore" double precision`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "actionabilityVersion" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "runId" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "modelVersionName" text`);
    await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "gateMode" text`);
    suggestedPlayLedgerColumnsEnsured = true;
    suggestedPlayLedgerScopeColumnsEnsured = true;
  }
  const valueRows: string[] = [];
  for (const game of args.games) {
    const playsForLedger = game.suggestedBetPicks ?? [];
    for (const play of playsForLedger) {
      const dedupKey = `${args.modelVersionName}|${args.gateMode}|${ledgerDedupKey(play)}`;
      const marketPick = play.marketPick ?? null;
      const settledHit =
        typeof play.result?.hit === "boolean" ? play.result.hit : null;
      const settledResult =
        settledHit == null ? "PENDING" : settledHit ? "HIT" : "MISS";
      const hasMarketPrice = marketPick?.overPrice != null && Number.isFinite(marketPick.overPrice);
      const priceAmerican = hasMarketPrice
        ? (marketPick?.overPrice ?? null)
        : args.allowFallbackOddsForLedger
          ? fallbackOdds
          : null;
      const isActionable = hasMarketPrice && Number.isFinite(priceAmerican) && priceAmerican !== 0;
      let payout: number | null = null;
      let profit: number | null = null;
      if (settledHit != null && priceAmerican != null && Number.isFinite(priceAmerican) && priceAmerican !== 0) {
        const decimalOdds = 1 + payoutFromAmerican(priceAmerican);
        payout = settledHit ? stake * decimalOdds : 0;
        profit = settledHit ? payout - stake : -stake;
      }
      const targetName = play.playerTarget?.name ?? marketPick?.playerName ?? null;
      valueRows.push(
        `('${crypto.randomUUID()}','${sqlEsc(args.dateStr)}',${args.season},'${sqlEsc(game.id)}','${sqlEsc(dedupKey)}',` +
          `${sqlStr(args.runId)},${sqlStr(args.modelVersionName)},${sqlStr(args.gateMode)},` +
          `'${sqlEsc(play.outcomeType)}',${sqlStr(play.displayLabel)},${sqlNum(play.playerTarget?.id ?? null)},${sqlStr(targetName)},` +
          `${sqlStr(marketPick?.market ?? null)},${sqlNum(marketPick?.line ?? null)},${sqlNum(priceAmerican)},${sqlNum(marketPick?.impliedProb ?? null)},` +
          `${sqlNum(marketPick?.impliedProb ?? null)},${sqlNum(marketPick?.estimatedProb ?? null)},${sqlNum(marketPick?.edge ?? null)},${sqlNum(marketPick?.ev ?? null)},` +
          `${sqlNum(play.rawWinProbability ?? null)},${sqlNum(play.calibratedWinProbability ?? null)},${sqlNum(play.impliedMarketProbability ?? null)},` +
          `${sqlNum(play.edgeVsMarket ?? null)},${sqlNum(play.expectedValueScore ?? null)},${sqlStr(play.marketType ?? null)},${sqlStr(play.marketSubType ?? null)},` +
          `${sqlStr(play.selectionSide ?? null)},${sqlNum(play.lineSnapshot ?? null)},${sqlNum(play.priceSnapshot ?? null)},${sqlStr(play.laneTag ?? null)},` +
          `${play.regimeTags?.length ? `'${sqlEsc(`{${play.regimeTags.map((t) => `"${t.replaceAll('"', '\\"')}"`).join(",")}}`)}'::text[]` : "NULL"},` +
          `${sqlNum(play.sourceReliabilityScore ?? null)},${sqlNum(play.uncertaintyScore ?? null)},${sqlNum(play.uncertaintyPenaltyApplied ?? null)},` +
          `${sqlNum(play.adjustedEdgeScore ?? null)},'${PICK_QUALITY_TUNING.enableStrictActionabilityGates ? "strict_v1" : "legacy_v1"}',` +
          `NULL,NULL,NULL,NULL,NULL,` +
          `${sqlNum(play.posteriorHitRate)},${sqlNum(play.metaScore)},${sqlNum(play.confidence)},${Math.max(1, play.votes ?? 1)},` +
          `${sqlNum(stake)},${isActionable ? "TRUE" : "FALSE"},${play.mlInvolved ? "TRUE" : "FALSE"},'${settledResult}',${sqlBool(settledHit)},${sqlNum(payout)},${sqlNum(profit)},NOW(),NOW())`,
      );
    }
  }
  const gateModeExpr =
    `COALESCE("gateMode", CASE WHEN COALESCE("actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  await prisma.$executeRawUnsafe(
    `DELETE FROM "SuggestedPlayLedger"
      WHERE "date" = '${sqlEsc(args.dateStr)}'
        AND COALESCE("modelVersionName",'live') = '${sqlEsc(args.modelVersionName)}'
        AND ${gateModeExpr} = '${sqlEsc(args.gateMode)}'`,
  );
  if (valueRows.length === 0) return;
  await prisma.$executeRawUnsafe(
    `INSERT INTO "SuggestedPlayLedger"
      ("id","date","season","gameId","dedupKey","runId","modelVersionName","gateMode","outcomeType","displayLabel","targetPlayerId","targetPlayerName","market","line","priceAmerican","impliedProb","betImpliedProb","estimatedProb","modelEdge","ev","rawWinProbability","calibratedWinProbability","impliedMarketProbability","edgeVsMarket","expectedValueScore","marketType","marketSubType","selectionSide","lineSnapshot","priceSnapshot","laneTag","regimeTags","sourceReliabilityScore","uncertaintyScore","uncertaintyPenaltyApplied","adjustedEdgeScore","actionabilityVersion","closePriceAmerican","closeImpliedProb","clvDeltaProb","clvDeltaCents","clvStatus","posteriorHitRate","metaScore","confidence","votes","stake","isActionable","mlInvolved","settledResult","settledHit","payout","profit","capturedAt","updatedAt")
     VALUES ${valueRows.join(",")}
     ON CONFLICT ("date","gameId","dedupKey") DO UPDATE SET
      "runId" = EXCLUDED."runId",
      "modelVersionName" = EXCLUDED."modelVersionName",
      "gateMode" = EXCLUDED."gateMode",
       "displayLabel" = EXCLUDED."displayLabel",
       "targetPlayerId" = EXCLUDED."targetPlayerId",
       "targetPlayerName" = EXCLUDED."targetPlayerName",
       "market" = EXCLUDED."market",
       "line" = EXCLUDED."line",
       "priceAmerican" = EXCLUDED."priceAmerican",
       "impliedProb" = EXCLUDED."impliedProb",
      "betImpliedProb" = EXCLUDED."betImpliedProb",
       "estimatedProb" = EXCLUDED."estimatedProb",
       "modelEdge" = EXCLUDED."modelEdge",
       "ev" = EXCLUDED."ev",
      "rawWinProbability" = EXCLUDED."rawWinProbability",
      "calibratedWinProbability" = EXCLUDED."calibratedWinProbability",
      "impliedMarketProbability" = EXCLUDED."impliedMarketProbability",
      "edgeVsMarket" = EXCLUDED."edgeVsMarket",
      "expectedValueScore" = EXCLUDED."expectedValueScore",
      "marketType" = EXCLUDED."marketType",
      "marketSubType" = EXCLUDED."marketSubType",
      "selectionSide" = EXCLUDED."selectionSide",
      "lineSnapshot" = EXCLUDED."lineSnapshot",
      "priceSnapshot" = EXCLUDED."priceSnapshot",
      "laneTag" = EXCLUDED."laneTag",
      "regimeTags" = EXCLUDED."regimeTags",
      "sourceReliabilityScore" = EXCLUDED."sourceReliabilityScore",
      "uncertaintyScore" = EXCLUDED."uncertaintyScore",
      "uncertaintyPenaltyApplied" = EXCLUDED."uncertaintyPenaltyApplied",
      "adjustedEdgeScore" = EXCLUDED."adjustedEdgeScore",
      "actionabilityVersion" = EXCLUDED."actionabilityVersion",
      "closePriceAmerican" = EXCLUDED."closePriceAmerican",
      "closeImpliedProb" = EXCLUDED."closeImpliedProb",
      "clvDeltaProb" = EXCLUDED."clvDeltaProb",
      "clvDeltaCents" = EXCLUDED."clvDeltaCents",
      "clvStatus" = EXCLUDED."clvStatus",
       "posteriorHitRate" = EXCLUDED."posteriorHitRate",
       "metaScore" = EXCLUDED."metaScore",
       "confidence" = EXCLUDED."confidence",
       "votes" = EXCLUDED."votes",
       "stake" = EXCLUDED."stake",
       "isActionable" = EXCLUDED."isActionable",
       "mlInvolved" = EXCLUDED."mlInvolved",
       "settledResult" = EXCLUDED."settledResult",
       "settledHit" = EXCLUDED."settledHit",
       "payout" = EXCLUDED."payout",
       "profit" = EXCLUDED."profit",
       "updatedAt" = NOW()`,
  );
}

let lastStaleVoidCheck = 0;

async function voidStaleNonFinalPicks(): Promise<void> {
  const now = Date.now();
  if (now - lastStaleVoidCheck < 60_000 * 30) return; // at most once per 30 min
  lastStaleVoidCheck = now;
  try {
    const voided = await prisma.$executeRawUnsafe(`
      UPDATE "SuggestedPlayLedger" s
      SET "settledHit" = NULL,
          "settledResult" = 'VOID',
          "payout" = 0,
          "profit" = 0,
          "updatedAt" = NOW()
      FROM "Game" g
      WHERE g."id" = s."gameId"
        AND s."settledHit" IS NULL
        AND g."date" < CURRENT_DATE - INTERVAL '2 days'
        AND (g."status" IS NULL OR g."status" NOT LIKE '%Final%')
    `);
    if (voided > 0) {
      console.log(`[ledger] Voided ${voided} stale picks on non-Final games older than 2 days`);
    }
  } catch (err) {
    console.error("[ledger] Failed to void stale picks:", err);
  }
}

function toWagerSummary(rows: Array<{
  settledResult: string | null;
  count: number;
  totalStaked: number | null;
  settledStaked: number | null;
  netPnl: number | null;
}>): WagerSummary {
  const byResult = new Map(rows.map((r) => [r.settledResult ?? "PENDING", r]));
  const wins = byResult.get("HIT")?.count ?? 0;
  const losses = byResult.get("MISS")?.count ?? 0;
  const pendingBets = byResult.get("PENDING")?.count ?? 0;
  const bets = wins + losses + pendingBets;
  const settledBets = wins + losses;
  const totalStaked = rows.reduce((sum, r) => sum + Number(r.totalStaked ?? 0), 0);
  const settledStaked = rows.reduce((sum, r) => sum + Number(r.settledStaked ?? 0), 0);
  const netPnl = rows.reduce((sum, r) => sum + Number(r.netPnl ?? 0), 0);
  const roi = settledStaked > 0 ? netPnl / settledStaked : null;
  return {
    bets,
    settledBets,
    pendingBets,
    wins,
    losses,
    totalStaked,
    settledStaked,
    netPnl,
    roi,
  };
}

async function getWagerTrackingSummary(args: {
  dateStr: string;
  season: number;
  modelVersionName: string;
  gateMode: "legacy" | "strict";
  stakePerPick: number;
  bankrollStart: number;
}): Promise<{
  stakePerPick: number;
  bankrollStart: number;
  day: WagerSummary & { date: string };
  seasonToDate: WagerSummary & { throughDate: string; bankrollCurrent: number };
} | null> {
  if (!(await isSuggestedPlayLedgerAvailable())) return null;
  const gateModeExpr =
    `COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  const [dayRows, seasonRows] = await Promise.all([
    prisma.$queryRawUnsafe<
      Array<{
        settledResult: string | null;
        count: number;
        totalStaked: number | null;
        settledStaked: number | null;
        netPnl: number | null;
      }>
    >(
      `SELECT
         COALESCE(l."settledResult", 'PENDING') as "settledResult",
         COUNT(*)::int as "count",
         SUM(l."stake")::float8 as "totalStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN l."stake" ELSE 0 END)::float8 as "settledStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN COALESCE(l."profit",0) ELSE 0 END)::float8 as "netPnl"
       FROM "SuggestedPlayLedger" l
       WHERE l."isActionable" = TRUE
         AND l."date" = '${sqlEsc(args.dateStr)}'
         AND COALESCE(l."modelVersionName",'live') = '${sqlEsc(args.modelVersionName)}'
         AND ${gateModeExpr} = '${sqlEsc(args.gateMode)}'
       GROUP BY COALESCE(l."settledResult", 'PENDING')`,
    ),
    prisma.$queryRawUnsafe<
      Array<{
        settledResult: string | null;
        count: number;
        totalStaked: number | null;
        settledStaked: number | null;
        netPnl: number | null;
      }>
    >(
      `SELECT
         COALESCE(l."settledResult", 'PENDING') as "settledResult",
         COUNT(*)::int as "count",
         SUM(l."stake")::float8 as "totalStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN l."stake" ELSE 0 END)::float8 as "settledStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN COALESCE(l."profit",0) ELSE 0 END)::float8 as "netPnl"
       FROM "SuggestedPlayLedger" l
       WHERE l."isActionable" = TRUE
         AND l."season" = ${args.season}
         AND l."date" <= '${sqlEsc(args.dateStr)}'
         AND COALESCE(l."modelVersionName",'live') = '${sqlEsc(args.modelVersionName)}'
         AND ${gateModeExpr} = '${sqlEsc(args.gateMode)}'
       GROUP BY COALESCE(l."settledResult", 'PENDING')`,
    ),
  ]);

  const day = toWagerSummary(dayRows);
  const seasonToDate = toWagerSummary(seasonRows);
  return {
    stakePerPick: args.stakePerPick,
    bankrollStart: args.bankrollStart,
    day: {
      date: args.dateStr,
      ...day,
    },
    seasonToDate: {
      throughDate: args.dateStr,
      bankrollCurrent: args.bankrollStart + seasonToDate.netPnl,
      ...seasonToDate,
    },
  };
}

async function isSuggestedPlayLedgerAvailable(): Promise<boolean> {
  if (suggestedPlayLedgerAvailableCache != null) {
    return suggestedPlayLedgerAvailableCache;
  }
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."SuggestedPlayLedger"') IS NOT NULL as "exists"`,
  );
  const exists = Boolean(rows[0]?.exists);
  suggestedPlayLedgerAvailableCache = exists;
  return exists;
}

async function hasLedgerRowsForDate(
  dateStr: string,
  modelVersionName: string,
  gateMode: "legacy" | "strict",
): Promise<boolean> {
  if (!(await isSuggestedPlayLedgerAvailable())) return false;
  if (!suggestedPlayLedgerScopeColumnsEnsured) {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "actionabilityVersion" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "modelVersionName" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "gateMode" text`,
    );
    suggestedPlayLedgerScopeColumnsEnsured = true;
  }
  const gateModeExpr =
    `COALESCE("gateMode", CASE WHEN COALESCE("actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  const rows = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "SuggestedPlayLedger"
     WHERE "date" = '${sqlEsc(dateStr)}'
       AND COALESCE("modelVersionName",'live') = '${sqlEsc(modelVersionName)}'
       AND ${gateModeExpr} = '${sqlEsc(gateMode)}'`,
  );
  return (rows[0]?.count ?? 0) > 0;
}

async function hasPendingLedgerRowsForDate(
  dateStr: string,
  modelVersionName: string,
  gateMode: "legacy" | "strict",
): Promise<boolean> {
  if (!(await isSuggestedPlayLedgerAvailable())) return false;
  if (!suggestedPlayLedgerScopeColumnsEnsured) {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "actionabilityVersion" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "modelVersionName" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "gateMode" text`,
    );
    suggestedPlayLedgerScopeColumnsEnsured = true;
  }
  const gateModeExpr =
    `COALESCE("gateMode", CASE WHEN COALESCE("actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  const rows = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "SuggestedPlayLedger"
     WHERE "date" = '${sqlEsc(dateStr)}'
       AND COALESCE("modelVersionName",'live') = '${sqlEsc(modelVersionName)}'
       AND ${gateModeExpr} = '${sqlEsc(gateMode)}'
       AND "settledHit" IS NULL`,
  );
  return (rows[0]?.count ?? 0) > 0;
}

async function getSeasonBetHitSummary(
  dateStr: string,
  season: number,
  modelVersionName: string,
  gateMode: "legacy" | "strict",
): Promise<{ hits: number; total: number; hitRate: number | null }> {
  if (!(await isSuggestedPlayLedgerAvailable())) {
    return { hits: 0, total: 0, hitRate: null };
  }
  const gateModeExpr =
    `COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  const rows = await prisma.$queryRawUnsafe<Array<{ settledHit: boolean; count: number }>>(
    `SELECT
       l."settledHit" as "settledHit",
       COUNT(*)::int as "count"
     FROM "SuggestedPlayLedger" l
     WHERE l."isActionable" = TRUE
       AND l."season" = ${season}
       AND l."date" <= '${sqlEsc(dateStr)}'
       AND COALESCE(l."modelVersionName",'live') = '${sqlEsc(modelVersionName)}'
       AND ${gateModeExpr} = '${sqlEsc(gateMode)}'
       AND COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS')
     GROUP BY l."settledHit"`,
  );
  const hits = rows.find((r) => r.settledHit)?.count ?? 0;
  const total = rows.reduce((sum, r) => sum + r.count, 0);
  return {
    hits,
    total,
    hitRate: total > 0 ? hits / total : null,
  };
}

async function shouldUpsertLedgerSnapshot(
  dateStr: string,
  refreshLedger: boolean,
  modelVersionName: string,
  gateMode: "legacy" | "strict",
): Promise<boolean> {
  if (refreshLedger) return true;
  const today = getEasternDateFromUtc(new Date());
  if (dateStr >= today) return true; // today/future can change as markets/results update
  const hasRows = await hasLedgerRowsForDate(dateStr, modelVersionName, gateMode);
  if (!hasRows) return true;
  // Re-grade past days if this model/gate scope still has unresolved rows.
  return await hasPendingLedgerRowsForDate(dateStr, modelVersionName, gateMode);
}

async function computePlayerContextFallback(
  p: typeof prisma,
  season: number,
  beforeDate: Date,
  homeTeamId: number,
  awayTeamId: number,
  teamIdToAllIds: Map<number, number[]>,
): Promise<{
  homeTopScorer: { id: number; name: string; stat: number } | null;
  homeTopRebounder: { id: number; name: string; stat: number } | null;
  homeTopPlaymaker: { id: number; name: string; stat: number } | null;
  awayTopScorer: { id: number; name: string; stat: number } | null;
  awayTopRebounder: { id: number; name: string; stat: number } | null;
  awayTopPlaymaker: { id: number; name: string; stat: number } | null;
}> {
  const homeIds = teamIdToAllIds.get(homeTeamId) ?? [homeTeamId];
  const awayIds = teamIdToAllIds.get(awayTeamId) ?? [awayTeamId];

  const getTopForTeam = async (teamIds: number[]): Promise<{ ppg: { id: number; name: string; stat: number } | null; rpg: { id: number; name: string; stat: number } | null; apg: { id: number; name: string; stat: number } | null }> => {
    const stats = await p.playerGameStat.groupBy({
      by: ["playerId", "teamId"],
      where: {
        game: { season, date: { lt: beforeDate }, homeScore: { gt: 0 } },
        teamId: { in: teamIds },
      },
      _sum: { points: true, rebounds: true, assists: true },
      _count: true,
    });

    const playerAggs = stats
      .filter((s) => (s._count ?? 0) >= 5)
      .map((s) => ({
        playerId: s.playerId,
        ppg: ((s._sum?.points ?? 0) / (s._count ?? 1)),
        rpg: ((s._sum?.rebounds ?? 0) / (s._count ?? 1)),
        apg: ((s._sum?.assists ?? 0) / (s._count ?? 1)),
      }));

    if (playerAggs.length === 0) return { ppg: null, rpg: null, apg: null };

    const topPpg = playerAggs.sort((a, b) => b.ppg - a.ppg)[0];
    const topRpg = playerAggs.sort((a, b) => b.rpg - a.rpg)[0];
    const topApg = playerAggs.sort((a, b) => b.apg - a.apg)[0];

    const playerIds = [...new Set([topPpg?.playerId, topRpg?.playerId, topApg?.playerId].filter(Boolean))];
    const players = await p.player.findMany({
      where: { id: { in: playerIds } },
      select: { id: true, firstname: true, lastname: true },
    });
    const nameMap = new Map(players.map((pl) => [pl.id, `${pl.firstname} ${pl.lastname}`]));

    return {
      ppg: topPpg ? { id: topPpg.playerId, name: nameMap.get(topPpg.playerId) ?? `Player ${topPpg.playerId}`, stat: topPpg.ppg } : null,
      rpg: topRpg ? { id: topRpg.playerId, name: nameMap.get(topRpg.playerId) ?? `Player ${topRpg.playerId}`, stat: topRpg.rpg } : null,
      apg: topApg ? { id: topApg.playerId, name: nameMap.get(topApg.playerId) ?? `Player ${topApg.playerId}`, stat: topApg.apg } : null,
    };
  };

  const [home, away] = await Promise.all([getTopForTeam(homeIds), getTopForTeam(awayIds)]);

  return {
    homeTopScorer: home.ppg,
    homeTopRebounder: home.rpg,
    homeTopPlaymaker: home.apg,
    awayTopScorer: away.ppg,
    awayTopRebounder: away.rpg,
    awayTopPlaymaker: away.apg,
  };
}

async function computeTeamSnapshots(
  season: number,
  beforeDate: Date,
  teamIds: number[],
): Promise<Map<number, TeamSnapshot>> {
  const result = new Map<number, TeamSnapshot>();

  // Get team codes for the given IDs (scheduled games may use different IDs than historical data)
  const scheduledTeams = await prisma.team.findMany({
    where: { id: { in: teamIds } },
    select: { id: true, code: true },
  });

  // Build a map of team code -> all team IDs with that code (handles duplicate team entries)
  const teamCodes = [...new Set(scheduledTeams.map((t) => t.code).filter(Boolean))] as string[];
  const allTeamsWithCodes = await prisma.team.findMany({
    where: { code: { in: teamCodes } },
    select: { id: true, code: true },
  });

  // Map scheduled team ID -> all IDs that share its code (for historical lookups)
  const teamIdToAllIds = new Map<number, number[]>();
  for (const scheduled of scheduledTeams) {
    if (!scheduled.code) continue;
    const matchingIds = allTeamsWithCodes
      .filter((t) => t.code === scheduled.code)
      .map((t) => t.id);
    teamIdToAllIds.set(scheduled.id, matchingIds);
  }

  for (const teamId of teamIds) {
    const allMatchingIds = teamIdToAllIds.get(teamId) ?? [teamId];

    const games = await prisma.game.findMany({
      where: {
        season,
        date: { lt: beforeDate },
        OR: [
          { homeTeamId: { in: allMatchingIds } },
          { awayTeamId: { in: allMatchingIds } },
        ],
        homeScore: { gt: 0 },
      },
      orderBy: { date: "desc" },
      take: 82,
      select: {
        id: true,
        date: true,
        homeTeamId: true,
        awayTeamId: true,
        homeScore: true,
        awayScore: true,
      },
    });

    if (games.length < 5) continue;

    let wins = 0, losses = 0, pointsFor = 0, pointsAgainst = 0;
    let streak = 0, lastResult: "W" | "L" | null = null;

    for (let i = 0; i < games.length; i++) {
      const g = games[i];
      const isHome = allMatchingIds.includes(g.homeTeamId);
      const won = isHome ? g.homeScore > g.awayScore : g.awayScore > g.homeScore;
      const pf = isHome ? g.homeScore : g.awayScore;
      const pa = isHome ? g.awayScore : g.homeScore;

      if (won) wins++;
      else losses++;
      pointsFor += pf;
      pointsAgainst += pa;

      if (i === 0) {
        lastResult = won ? "W" : "L";
        streak = 1;
      } else if (i < 10 && lastResult && ((won && lastResult === "W") || (!won && lastResult === "L"))) {
        streak++;
      }
    }

    const gp = games.length;
    result.set(teamId, {
      wins,
      losses,
      ppg: pointsFor / gp,
      oppg: pointsAgainst / gp,
      pace: null,
      rebPg: null,
      astPg: null,
      fg3Pct: null,
      ftPct: null,
      rankOff: null,
      rankDef: null,
      rankPace: null,
      streak: lastResult === "W" ? streak : -streak,
      lastGameDate: games[0]?.date ?? null,
    });
  }

  const allTeamStats = [...result.entries()].map(([id, snap]) => ({
    id,
    ppg: snap.ppg,
    oppg: snap.oppg,
  }));

  allTeamStats.sort((a, b) => b.ppg - a.ppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankOff = i + 1;
  });

  allTeamStats.sort((a, b) => a.oppg - b.oppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankDef = i + 1;
  });

  return result;
}

