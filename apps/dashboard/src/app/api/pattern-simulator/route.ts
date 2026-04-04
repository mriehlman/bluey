import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import type { GameContext } from "@bluey/db";
import { getEasternDateFromUtc } from "@/lib/format";
import { GAME_EVENT_CATALOG } from "@bluey/core/features/gameEventCatalog";
import type { GameEventContext } from "@bluey/core/features/gameEventCatalog";
import {
  generateGamePredictions,
  loadLatestFeatureBins,
  evaluatePlayerPointsMlVote,
  type DeployedPatternV2,
  type PlayerPropRow,
  type GamePlayerContext,
  type V2PlayerTarget,
} from "@bluey/core/features/predictionEngine";
import { loadCalibrationArtifacts, loadSourceReliabilitySnapshot } from "@bluey/core/features/pickQuality";
import { LEDGER_TUNING } from "@bluey/core/config/tuning";
import { loadActiveModelVersion } from "@bluey/core/patterns/modelVersion";
import { loadEarlyInjuriesForDate, buildTeamAliasLookup, computeLineupSignalsFromCounts } from "@bluey/core/features/injuryContext";
import * as path from "path";

export const dynamic = "force-dynamic";
export const maxDuration = 300;

type RunBody = {
  season?: number;
  from?: string;
  to?: string;
  patternIds?: string[];
  modelVersion?: string; // "live" | <snapshot name> | omitted -> active
};

type SimPick = {
  date: string;
  season: number;
  gameId: string;
  gameLabel: string;
  outcomeType: string;
  label: string;
  odds: number;
  confidence: number;
  posterior: number;
  meta: number | null;
  edge: number;
  mlInvolved: boolean;
  hit: boolean | null;
};

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function parseDateInput(value: string): Date {
  return new Date(`${value}T00:00:00Z`);
}

function getRepoRoot(): string {
  const cwd = process.cwd();
  if (cwd.includes("apps") && cwd.includes("dashboard")) return path.resolve(cwd, "../..");
  if (cwd.endsWith("dashboard")) return path.resolve(cwd, "..");
  return cwd;
}

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function isCompletedGame(game: { status: string | null; homeScore: number | null; awayScore: number | null }): boolean {
  if (game.status?.includes("Final")) return true;
  return Number(game.homeScore ?? 0) > 0 || Number(game.awayScore ?? 0) > 0;
}

function parseOutcomeType(outcomeType: string): { eventKey: string; side: "home" | "away" | "game" } {
  const [eventKey, sideRaw] = outcomeType.split(":");
  const side = sideRaw === "home" || sideRaw === "away" || sideRaw === "game" ? sideRaw : "game";
  return { eventKey, side };
}

function buildGamePlayerContext(gameContexts: Array<{
  teamId: number;
  playerId: number;
  ppg: number;
  rpg: number;
  apg: number;
  player: { firstname: string | null; lastname: string | null };
}>, homeTeamId: number, awayTeamId: number): GamePlayerContext {
  const toName = (p: { firstname: string | null; lastname: string | null }) =>
    `${p.firstname ?? ""} ${p.lastname ?? ""}`.trim() || "Unknown";

  const top = (
    rows: typeof gameContexts,
    stat: "ppg" | "rpg" | "apg",
  ): { id: number; name: string; stat: number } | null => {
    const sorted = [...rows].sort((a, b) => Number(b[stat]) - Number(a[stat]));
    const row = sorted[0];
    return row
      ? {
          id: row.playerId,
          name: toName(row.player),
          stat: Number(row[stat]),
        }
      : null;
  };

  const homeRows = gameContexts.filter((r) => r.teamId === homeTeamId);
  const awayRows = gameContexts.filter((r) => r.teamId === awayTeamId);
  return {
    homeTopScorer: top(homeRows, "ppg"),
    homeTopRebounder: top(homeRows, "rpg"),
    homeTopPlaymaker: top(homeRows, "apg"),
    awayTopScorer: top(awayRows, "ppg"),
    awayTopRebounder: top(awayRows, "rpg"),
    awayTopPlaymaker: top(awayRows, "apg"),
  };
}

function fallbackGameContext(game: {
  id: string;
  homeTeamId: number;
  awayTeamId: number;
}): GameContext {
  return {
    id: `fallback-${game.id}`,
    gameId: game.id,
    homeWins: 0,
    homeLosses: 0,
    homePpg: 0,
    homeOppg: 0,
    homePace: null,
    homeRebPg: null,
    homeAstPg: null,
    homeFg3Pct: null,
    homeFtPct: null,
    homeRankOff: null,
    homeRankDef: null,
    homeRankPace: null,
    homeStreak: 0,
    awayWins: 0,
    awayLosses: 0,
    awayPpg: 0,
    awayOppg: 0,
    awayPace: null,
    awayRebPg: null,
    awayAstPg: null,
    awayFg3Pct: null,
    awayFtPct: null,
    awayRankOff: null,
    awayRankDef: null,
    awayRankPace: null,
    awayStreak: 0,
    homeRestDays: null,
    awayRestDays: null,
    homeIsB2b: false,
    awayIsB2b: false,
    homeInjuryOutCount: null,
    homeInjuryDoubtfulCount: null,
    homeInjuryQuestionableCount: null,
    homeInjuryProbableCount: null,
    awayInjuryOutCount: null,
    awayInjuryDoubtfulCount: null,
    awayInjuryQuestionableCount: null,
    awayInjuryProbableCount: null,
    homeLineupCertainty: null,
    awayLineupCertainty: null,
    homeLateScratchRisk: null,
    awayLateScratchRisk: null,
    h2hHomeWins: 0,
    h2hAwayWins: 0,
  };
}

function evaluateOutcome(args: {
  outcomeType: string;
  target: V2PlayerTarget | null;
  game: {
    status: string | null;
    homeScore: number;
    awayScore: number;
    homeTeamId: number;
    awayTeamId: number;
    context: GameContext;
    playerStats: unknown[];
  };
  consensusOdds: { spreadHome?: number | null; totalOver?: number | null; mlHome?: number | null; mlAway?: number | null } | null;
  gameOutcomeMap: Map<string, { meta: unknown }>;
  gamePlayerContexts: Array<{
    playerId: number;
    teamId: number;
    ppg: number;
    rpg: number;
    apg: number;
  }>;
}): boolean | null {
  if (!isCompletedGame(args.game)) return null;

  const { eventKey, side } = parseOutcomeType(args.outcomeType);
  const direct = args.gameOutcomeMap.get(`${eventKey}:${side}`);
  if (direct) {
    if (args.target) {
      const meta = (direct.meta ?? null) as Record<string, unknown> | null;
      const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
      return actualPlayerId == null ? true : actualPlayerId === args.target.id;
    }
    return true;
  }

  const gameEventContext: GameEventContext = {
    game: {
      id: args.game.context.gameId,
      homeTeamId: args.game.homeTeamId,
      awayTeamId: args.game.awayTeamId,
      homeScore: args.game.homeScore,
      awayScore: args.game.awayScore,
      status: args.game.status,
    } as any,
    context: args.game.context,
    playerContexts: args.gamePlayerContexts as any,
    stats: args.game.playerStats as any,
    odds: args.consensusOdds as any,
  };
  const def = GAME_EVENT_CATALOG.find((d) => d.type === "outcome" && d.key === eventKey && d.sides.includes(side));
  if (!def) return null;
  const computed = def.compute(gameEventContext, side);
  if (args.target && computed.hit) {
    const meta = (computed.meta ?? null) as Record<string, unknown> | null;
    const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
    return actualPlayerId == null ? true : actualPlayerId === args.target.id;
  }
  return computed.hit;
}

async function loadArtifacts(modelVersion: string | null): Promise<{
  modelVersionUsed: string;
  deployedPatterns: DeployedPatternV2[];
  featureBins: Map<string, any>;
  metaModel: null;
}> {
  if (modelVersion && modelVersion !== "live") {
    const snapshot = await prisma.modelVersion.findUnique({
      where: { name: modelVersion },
      select: { name: true, deployedPatterns: true, featureBins: true, metaModel: true },
    });
    if (!snapshot) {
      throw new Error(`Model version "${modelVersion}" not found.`);
    }
    return {
      modelVersionUsed: snapshot.name,
      deployedPatterns: snapshot.deployedPatterns as unknown as DeployedPatternV2[],
      featureBins: new Map(Object.entries(snapshot.featureBins as Record<string, unknown>)),
      metaModel: null,
    };
  }

  if (!modelVersion) {
    const active = await loadActiveModelVersion();
    if (active) {
      return {
        modelVersionUsed: active.name ?? "active",
        deployedPatterns: active.deployedPatterns,
        featureBins: new Map(Object.entries(active.featureBins)),
        metaModel: null,
      };
    }
  }

  const [deployedPatterns, featureBins] = await Promise.all([
    prisma.patternV2.findMany({
      where: { status: "deployed" },
      select: {
        id: true,
        outcomeType: true,
        conditions: true,
        posteriorHitRate: true,
        edge: true,
        score: true,
        n: true,
      },
    }),
    loadLatestFeatureBins(prisma),
  ]);

  return {
    modelVersionUsed: "live",
    deployedPatterns: deployedPatterns as unknown as DeployedPatternV2[],
    featureBins,
    metaModel: null,
  };
}

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as RunBody;
    const season = Number(body.season);
    const patternIds = (body.patternIds ?? []).filter((id) => typeof id === "string" && id.trim().length > 0);
    const modelVersion = body.modelVersion?.trim() || null;
    if (!Number.isFinite(season)) {
      return NextResponse.json({ error: "season is required" }, { status: 400 });
    }
    if (patternIds.length === 0) {
      return NextResponse.json({ error: "patternIds must include at least one id" }, { status: 400 });
    }

    const today = new Date().toISOString().slice(0, 10);
    const defaultFrom = `${season}-10-01`;
    const defaultTo = `${season + 1}-07-31`;
    const from = body.from ?? defaultFrom;
    const to = body.to ?? defaultTo;
    const toCapped = to > today ? today : to;

    const { modelVersionUsed, deployedPatterns, featureBins, metaModel } = await loadArtifacts(modelVersion);
    const selectedPatterns = deployedPatterns.filter((p) => patternIds.includes(p.id));
    if (selectedPatterns.length === 0) {
      return NextResponse.json({
        season,
        from,
        to: toCapped,
        modelVersion: modelVersionUsed,
        selectedPatternCount: 0,
        days: [],
        picks: [],
        summary: { days: 0, picks: 0, resolved: 0, hits: 0, hitRate: null },
      });
    }

    const dateRows = await prisma.$queryRawUnsafe<Array<{ d: string }>>(
      `SELECT TO_CHAR("date",'YYYY-MM-DD') AS d
       FROM "Game"
       WHERE "season" = ${season}
         AND "date" >= '${sqlEsc(from)}'::date
         AND "date" <= '${sqlEsc(toCapped)}'::date
       GROUP BY "date"
       ORDER BY "date" ASC`,
    );
    const dates = dateRows.map((r) => r.d).filter(Boolean);

    const fallbackAmericanOdds = LEDGER_TUNING.fallbackAmericanOdds;
    const calibrationArtifacts = await loadCalibrationArtifacts();
    const sourceReliabilitySnapshot = await loadSourceReliabilitySnapshot();
    const allTeamsForInjury = await prisma.team.findMany({
      select: { id: true, city: true, name: true, code: true },
    });
    const teamAliasLookup = buildTeamAliasLookup(allTeamsForInjury);
    const injuryCache = new Map();
    const injuryDataDir = path.join(getRepoRoot(), "data");

    const picks: SimPick[] = [];
    const byDate = new Map<string, { picks: number; resolved: number; hits: number }>();

    for (const dateStr of dates) {
      const targetDate = parseDateInput(dateStr);
      const dayStart = new Date(targetDate.getTime() - 86_400_000);
      const dayEnd = new Date(targetDate.getTime() + 86_400_000);
      const gamesRaw = await prisma.game.findMany({
        where: { date: { gte: dayStart, lte: dayEnd } },
        include: {
          homeTeam: true,
          awayTeam: true,
          odds: true,
          context: true,
          playerStats: true,
        },
        orderBy: { tipoffTimeUtc: "asc" },
      });
      const games = gamesRaw.filter((g) => {
        if (!g.tipoffTimeUtc) {
          return g.date.toISOString().slice(0, 10) === dateStr;
        }
        return getEasternDateFromUtc(g.tipoffTimeUtc) === dateStr;
      });
      if (games.length === 0) continue;

      const gameIds = games.map((g) => g.id);
      const [playerPropsRaw, gamePlayerContexts, outcomeRows] = await Promise.all([
        prisma.playerPropOdds.findMany({
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
        }),
        prisma.playerGameContext.findMany({
          where: { gameId: { in: gameIds } },
          include: { player: true },
        }),
        prisma.gameEvent.findMany({
          where: { gameId: { in: gameIds }, type: "outcome" },
          select: { gameId: true, eventKey: true, side: true, meta: true },
        }),
      ]);
      const injuriesByTeam = loadEarlyInjuriesForDate(dateStr, teamAliasLookup, injuryCache, injuryDataDir);

      const propsByGame = new Map<string, PlayerPropRow[]>();
      for (const gid of gameIds) propsByGame.set(gid, []);
      for (const row of playerPropsRaw) propsByGame.get(row.gameId)?.push(row as PlayerPropRow);

      const contextsByGame = new Map<string, typeof gamePlayerContexts>();
      for (const gid of gameIds) contextsByGame.set(gid, []);
      for (const row of gamePlayerContexts) contextsByGame.get(row.gameId)?.push(row);

      const outcomesByGame = new Map<string, Map<string, { meta: unknown }>>();
      for (const row of outcomeRows) {
        const map = outcomesByGame.get(row.gameId) ?? new Map<string, { meta: unknown }>();
        map.set(`${row.eventKey}:${row.side}`, { meta: row.meta });
        outcomesByGame.set(row.gameId, map);
      }

      for (const game of games) {
        const consensus = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;
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
        const contextRows = contextsByGame.get(game.id) ?? [];
        const gamePlayerContext = buildGamePlayerContext(contextRows as any, game.homeTeamId, game.awayTeamId);
        const baseContext = (game.context ?? fallbackGameContext(game)) as GameContext;
        const homeLineupSignals = computeLineupSignalsFromCounts(injuriesByTeam.get(game.homeTeamId));
        const awayLineupSignals = computeLineupSignalsFromCounts(injuriesByTeam.get(game.awayTeamId));
        const effectiveContext: GameContext = {
          ...baseContext,
          homeInjuryOutCount: injuriesByTeam.get(game.homeTeamId)?.out ?? baseContext.homeInjuryOutCount,
          homeInjuryDoubtfulCount: injuriesByTeam.get(game.homeTeamId)?.doubtful ?? baseContext.homeInjuryDoubtfulCount,
          homeInjuryQuestionableCount:
            injuriesByTeam.get(game.homeTeamId)?.questionable ?? baseContext.homeInjuryQuestionableCount,
          homeInjuryProbableCount: injuriesByTeam.get(game.homeTeamId)?.probable ?? baseContext.homeInjuryProbableCount,
          awayInjuryOutCount: injuriesByTeam.get(game.awayTeamId)?.out ?? baseContext.awayInjuryOutCount,
          awayInjuryDoubtfulCount: injuriesByTeam.get(game.awayTeamId)?.doubtful ?? baseContext.awayInjuryDoubtfulCount,
          awayInjuryQuestionableCount:
            injuriesByTeam.get(game.awayTeamId)?.questionable ?? baseContext.awayInjuryQuestionableCount,
          awayInjuryProbableCount: injuriesByTeam.get(game.awayTeamId)?.probable ?? baseContext.awayInjuryProbableCount,
          homeLineupCertainty: homeLineupSignals.certainty ?? baseContext.homeLineupCertainty,
          awayLineupCertainty: awayLineupSignals.certainty ?? baseContext.awayLineupCertainty,
          homeLateScratchRisk: homeLineupSignals.lateScratchRisk ?? baseContext.homeLateScratchRisk,
          awayLateScratchRisk: awayLineupSignals.lateScratchRisk ?? baseContext.awayLateScratchRisk,
        };

        const output = generateGamePredictions({
          season,
          gameContext: effectiveContext,
          odds: engineOdds,
          deployedV2Patterns: selectedPatterns,
          featureBins,
          metaModel,
          gamePlayerContext,
          propsForGame: propsByGame.get(game.id) ?? [],
          maxBetPicksPerGame: Math.max(1, LEDGER_TUNING.maxBetPicksPerGame),
          fallbackAmericanOdds,
          calibrationArtifacts,
          sourceReliabilitySnapshot,
        });

        const gameOutcomeMap = outcomesByGame.get(game.id) ?? new Map<string, { meta: unknown }>();
        const gameLabel = `${game.awayTeam?.code ?? "?"} @ ${game.homeTeam?.code ?? "?"}`;
        for (const play of output.suggestedBetPicks) {
          const hit = evaluateOutcome({
            outcomeType: play.outcomeType,
            target: play.playerTarget,
            game: {
              status: game.status,
              homeScore: game.homeScore,
              awayScore: game.awayScore,
              homeTeamId: game.homeTeamId,
              awayTeamId: game.awayTeamId,
              context: effectiveContext,
              playerStats: game.playerStats,
            },
            consensusOdds: engineOdds,
            gameOutcomeMap,
            gamePlayerContexts: contextRows.map((r) => ({
              playerId: r.playerId,
              teamId: r.teamId,
              ppg: r.ppg,
              rpg: r.rpg,
              apg: r.apg,
            })),
          });

          const mlInvolved =
            evaluatePlayerPointsMlVote({
              outcomeType: play.outcomeType,
              playerTarget: play.playerTarget,
              marketPick: play.marketPick ?? null,
              gameContext: effectiveContext,
              gamePlayerCtx: gamePlayerContext,
            }).decision !== "abstain";

          picks.push({
            date: dateStr,
            season,
            gameId: game.id,
            gameLabel,
            outcomeType: play.outcomeType,
            label: play.displayLabel ?? play.outcomeType,
            odds: play.marketPick?.overPrice ?? fallbackAmericanOdds,
            confidence: play.confidence,
            posterior: play.posteriorHitRate,
            meta: play.metaScore ?? null,
            edge: play.edge,
            mlInvolved,
            hit,
          });

          const current = byDate.get(dateStr) ?? { picks: 0, resolved: 0, hits: 0 };
          current.picks += 1;
          if (hit != null) {
            current.resolved += 1;
            if (hit) current.hits += 1;
          }
          byDate.set(dateStr, current);
        }
      }
    }

    const days = [...byDate.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, s]) => ({
        date,
        picks: s.picks,
        resolved: s.resolved,
        hits: s.hits,
        misses: Math.max(0, s.resolved - s.hits),
        pending: Math.max(0, s.picks - s.resolved),
        hitRate: s.resolved > 0 ? s.hits / s.resolved : null,
      }));

    const totalPicks = picks.length;
    const resolved = picks.filter((p) => p.hit != null).length;
    const hits = picks.filter((p) => p.hit === true).length;

    return NextResponse.json({
      season,
      from,
      to: toCapped,
      modelVersion: modelVersionUsed,
      selectedPatternCount: selectedPatterns.length,
      selectedPatternIds: selectedPatterns.map((p) => p.id),
      summary: {
        days: days.length,
        picks: totalPicks,
        resolved,
        hits,
        hitRate: resolved > 0 ? hits / resolved : null,
      },
      days,
      picks,
    });
  } catch (err) {
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Failed to run pattern simulator" },
      { status: 500 },
    );
  }
}
