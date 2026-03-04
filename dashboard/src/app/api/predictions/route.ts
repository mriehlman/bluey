import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { getEasternDateFromUtc } from "@/lib/format";

export const dynamic = "force-dynamic";

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

type DiscoveryV2Pattern = {
  id: string;
  outcomeType: string;
  conditions: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
};

function getCurrentSeason(): number {
  const now = new Date();
  // Season uses the year it starts (e.g., 2025-26 season = 2025)
  // Oct-Dec belongs to that year's season; Jan-Sep belongs to previous year's season
  return now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
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

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");

  if (!dateStr) {
    const today = new Date().toISOString().slice(0, 10);
    return NextResponse.redirect(new URL(`/api/predictions?date=${today}`, req.url));
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = getCurrentSeason();

  // Query ±1 day to catch games mis-dated during ingestion (e.g. March 2 games stored as March 1)
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
    },
    orderBy: { tipoffTimeUtc: "asc" },
  });

  // Filter to games whose Eastern tipoff date matches requested date (source of truth for game day)
  const gamesForDate = gamesRaw.filter((g) => {
    if (!g.tipoffTimeUtc) {
      // No tipoff time: trust stored date
      const storedDate = g.date instanceof Date ? g.date.toISOString().slice(0, 10) : String(g.date).slice(0, 10);
      return storedDate === dateStr;
    }
    const easternDate = getEasternDateFromUtc(g.tipoffTimeUtc);
    return easternDate === dateStr;
  });

  // Deduplicate by matchup (home/away team codes - handles duplicate team IDs)
  const seenMatchups = new Map<string, (typeof gamesForDate)[0]>();
  for (const g of gamesForDate) {
    const homeCode = g.homeTeam?.code ?? g.homeTeamId.toString();
    const awayCode = g.awayTeam?.code ?? g.awayTeamId.toString();
    const key = `${awayCode}@${homeCode}`;
    const existing = seenMatchups.get(key);
    if (!existing) {
      seenMatchups.set(key, g);
    } else {
      // Prefer game with context and odds
      const better =
        (g.context && !existing.context) || (g.odds?.length && !existing.odds?.length)
          ? g
          : existing;
      seenMatchups.set(key, better);
    }
  }
  const games = [...seenMatchups.values()].sort(
    (a, b) => (a.tipoffTimeUtc?.getTime() ?? 0) - (b.tipoffTimeUtc?.getTime() ?? 0),
  );

  if (games.length === 0) {
    return NextResponse.json({ date: dateStr, games: [], message: "No games found" });
  }

  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
  });

  const discoveryV2ByGame = await getDiscoveryV2MatchesByGame(
    games.map((g) => g.id),
  );

  const teamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const teamSnapshots = await computeTeamSnapshots(season, targetDate, teamIds);

  // For completed games, fetch outcome events to show hit/miss status
  const completedGameIds = games
    .filter((g) => g.status?.includes("Final") && g.homeScore > 0)
    .map((g) => g.id);

  const [gameOutcomes, patternHits] = await Promise.all([
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
    completedGameIds.length > 0
      ? prisma.gamePatternHit.findMany({
          where: { gameId: { in: completedGameIds } },
          select: { gameId: true, patternId: true, hit: true },
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

  // Build lookup: gameId|patternId -> hit (from GamePatternHit - reliable source for completed games)
  const hitByGameAndPattern = new Map<string, boolean>();
  for (const h of patternHits) {
    hitByGameAndPattern.set(`${h.gameId}|${h.patternId}`, h.hit);
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
  const gameIds = games.map((g) => g.id);
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

  type PlayerInfo = { id: number; name: string; stat: number };
  type GamePlayerContext = {
    homeTopScorer: PlayerInfo | null;
    homeTopRebounder: PlayerInfo | null;
    homeTopPlaymaker: PlayerInfo | null;
    awayTopScorer: PlayerInfo | null;
    awayTopRebounder: PlayerInfo | null;
    awayTopPlaymaker: PlayerInfo | null;
  };
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

  // Fetch recent pattern performance (last 30 days)
  const thirtyDaysAgo = new Date(targetDate);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  const patternKeys = patterns.map((p) => p.patternKey);
  const recentHits = await prisma.gamePatternHit.groupBy({
    by: ["patternId", "hit"],
    where: {
      pattern: { patternKey: { in: patternKeys } },
      game: { date: { gte: thirtyDaysAgo, lt: targetDate } },
    },
    _count: true,
  });

  // Build lookup: patternId -> { recentHits, recentTotal }
  const patternIdToKey = new Map(patterns.map((p) => [p.id, p.patternKey]));
  const recentPerfByPattern = new Map<string, { hits: number; total: number }>();
  for (const r of recentHits) {
    const key = patternIdToKey.get(r.patternId);
    if (!key) continue;
    if (!recentPerfByPattern.has(key)) recentPerfByPattern.set(key, { hits: 0, total: 0 });
    const perf = recentPerfByPattern.get(key)!;
    perf.total += r._count;
    if (r.hit) perf.hits += r._count;
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
    const matchingPatterns = findMatchingPatterns(
      game,
      homeSnap,
      awaySnap,
      patterns,
      targetDate,
      consensus ?? undefined,
      gamePlayerContexts,
      game.context ?? undefined,
    );
    const isFinal = game.status?.includes("Final") && game.homeScore > 0;
    const gameOutcomeMap = outcomesByGame.get(game.id);
    const gamePlayerCtx = playerContextByGame.get(game.id);

    // Deduplicate: keep only the best pattern for each unique outcome
    // "Best" = highest hit rate, then largest sample size
    const seenOutcomes = new Set<string>();
    const deduplicatedPatterns = matchingPatterns.filter((pred) => {
      if (seenOutcomes.has(pred.outcome)) return false;
      seenOutcomes.add(pred.outcome);
      return true;
    });

    // Enrich predictions with player info, prop lines, recent performance, and outcome results
    const enrichedPredictions = deduplicatedPatterns.slice(0, 10).map((pred) => {
      // Determine player target for player-specific outcomes
      let playerTarget: { name: string; stat: string; statValue: number } | null = null;
      let propLine: { line: number; market: string } | null = null;

      if (gamePlayerCtx) {
        const outcome = pred.outcome.replace(/:.*/, "");
        if (outcome === "HOME_TOP_SCORER_25_PLUS" || outcome === "HOME_TOP_SCORER_30_PLUS") {
          const p = gamePlayerCtx.homeTopScorer;
          if (p) playerTarget = { name: p.name, stat: "ppg", statValue: p.stat };
        } else if (outcome === "AWAY_TOP_SCORER_25_PLUS" || outcome === "AWAY_TOP_SCORER_30_PLUS") {
          const p = gamePlayerCtx.awayTopScorer;
          if (p) playerTarget = { name: p.name, stat: "ppg", statValue: p.stat };
        } else if (outcome === "HOME_TOP_REBOUNDER_10_PLUS" || outcome === "HOME_TOP_REBOUNDER_12_PLUS") {
          const p = gamePlayerCtx.homeTopRebounder;
          if (p) playerTarget = { name: p.name, stat: "rpg", statValue: p.stat };
        } else if (outcome === "AWAY_TOP_REBOUNDER_10_PLUS" || outcome === "AWAY_TOP_REBOUNDER_12_PLUS") {
          const p = gamePlayerCtx.awayTopRebounder;
          if (p) playerTarget = { name: p.name, stat: "rpg", statValue: p.stat };
        } else if (
          outcome === "HOME_TOP_PLAYMAKER_8_PLUS" || outcome === "HOME_TOP_PLAYMAKER_10_PLUS" ||
          outcome === "HOME_TOP_ASSIST_8_PLUS" || outcome === "HOME_TOP_ASSIST_10_PLUS"
        ) {
          const p = gamePlayerCtx.homeTopPlaymaker;
          if (p) playerTarget = { name: p.name, stat: "apg", statValue: p.stat };
        } else if (
          outcome === "AWAY_TOP_PLAYMAKER_8_PLUS" || outcome === "AWAY_TOP_PLAYMAKER_10_PLUS" ||
          outcome === "AWAY_TOP_ASSIST_8_PLUS" || outcome === "AWAY_TOP_ASSIST_10_PLUS"
        ) {
          const p = gamePlayerCtx.awayTopPlaymaker;
          if (p) playerTarget = { name: p.name, stat: "apg", statValue: p.stat };
        } else if (outcome === "HOME_TOP_ASSIST_EXCEEDS_AVG" || outcome === "AWAY_TOP_ASSIST_EXCEEDS_AVG") {
          const p = outcome.startsWith("HOME_") ? gamePlayerCtx.homeTopPlaymaker : gamePlayerCtx.awayTopPlaymaker;
          if (p) playerTarget = { name: p.name, stat: "apg", statValue: p.stat };
        } else if (outcome === "HOME_TOP_SCORER_EXCEEDS_AVG" || outcome === "AWAY_TOP_SCORER_EXCEEDS_AVG") {
          const p = outcome.startsWith("HOME_") ? gamePlayerCtx.homeTopScorer : gamePlayerCtx.awayTopScorer;
          if (p) playerTarget = { name: p.name, stat: "ppg", statValue: p.stat };
        } else if (outcome === "HOME_TOP_REBOUNDER_EXCEEDS_AVG" || outcome === "AWAY_TOP_REBOUNDER_EXCEEDS_AVG") {
          const p = outcome.startsWith("HOME_") ? gamePlayerCtx.homeTopRebounder : gamePlayerCtx.awayTopRebounder;
          if (p) playerTarget = { name: p.name, stat: "rpg", statValue: p.stat };
        } else if (outcome === "HOME_TOP_SCORER_DOUBLE_DOUBLE" || outcome === "AWAY_TOP_SCORER_DOUBLE_DOUBLE") {
          const p = outcome.startsWith("HOME_") ? gamePlayerCtx.homeTopScorer : gamePlayerCtx.awayTopScorer;
          if (p) {
            playerTarget = { name: p.name, stat: "ppg", statValue: p.stat };
          }
        }
      }

      // Recent performance (last 30 days)
      const recentPerf = recentPerfByPattern.get(pred.patternKey);
      const recent = recentPerf ? { hits: recentPerf.hits, total: recentPerf.total } : null;

      // High-value pattern detection
      const isHighValue = pred.outcome.includes("UNDERDOG_COVERED") || 
                          pred.outcome.includes("FAVORITE_COVERED") ||
                          (pred.hitRate >= 0.65 && pred.sampleSize >= 100);

      // Result for completed games: use GamePatternHit (primary) or GameEvent (fallback)
      // GamePatternHit is populated by search:game-patterns and explicitly tracks hit/miss per (pattern, game)
      let result: { hit: boolean; explanation: string | null } | null = null;
      if (isFinal) {
        let hit: boolean | undefined;
        const patternHit = hitByGameAndPattern.get(`${game.id}|${pred.patternId}`);
        if (patternHit !== undefined) {
          hit = patternHit;
        } else {
          // Fallback: GameEvent outcome lookup (keys stored as eventKey:side, e.g. AWAY_WIN:game)
          const gameOutcomeMap = outcomesByGame.get(game.id);
          if (gameOutcomeMap) {
            const outcomeResult = resolveOutcomeResult(gameOutcomeMap, pred.outcome);
            hit = !!outcomeResult;
          }
        }
        if (hit !== undefined) {
          let explanation: string | null = null;
          if (hit) {
            const gameOutcomeMap = outcomesByGame.get(game.id);
            const outcomeResult = gameOutcomeMap ? resolveOutcomeResult(gameOutcomeMap, pred.outcome) : undefined;
            if (outcomeResult?.meta) {
              const meta = outcomeResult.meta as Record<string, unknown>;
              const parts: string[] = [];
              if (meta.playerId && typeof meta.playerId === "number") {
                parts.push(playerMap.get(meta.playerId) ?? `Player ${meta.playerId}`);
              }
              if (meta.points !== undefined) parts.push(`${meta.points} pts`);
              if (meta.rebounds !== undefined) parts.push(`${meta.rebounds} reb`);
              if (meta.assists !== undefined) parts.push(`${meta.assists} ast`);
              if (meta.fg3m !== undefined) parts.push(`${meta.fg3m} 3PM`);
              if (meta.actual !== undefined && meta.line !== undefined) {
                parts.push(`${meta.actual} total (line: ${meta.line})`);
              }
              if (typeof meta.margin === "number" && typeof meta.spread === "number") {
                parts.push(`margin ${meta.margin > 0 ? "+" : ""}${meta.margin} (spread: ${meta.spread > 0 ? "+" : ""}${meta.spread})`);
              }
              if (parts.length > 0) explanation = parts.join(", ");
            }
          }
          result = { hit, explanation };
        }
      }

      return { ...pred, playerTarget, propLine, recent, isHighValue, result };
    });

    const discoveryV2Matches = discoveryV2ByGame.get(game.id) ?? [];
    const suggestedPlayMap = new Map<
      string,
      { outcomeType: string; scoreSum: number; count: number; bestPosterior: number; bestEdge: number }
    >();
    for (const p of discoveryV2Matches) {
      const existing = suggestedPlayMap.get(p.outcomeType);
      if (!existing) {
        suggestedPlayMap.set(p.outcomeType, {
          outcomeType: p.outcomeType,
          scoreSum: p.score,
          count: 1,
          bestPosterior: p.posteriorHitRate,
          bestEdge: p.edge,
        });
      } else {
        existing.scoreSum += p.score;
        existing.count += 1;
        if (p.posteriorHitRate > existing.bestPosterior) existing.bestPosterior = p.posteriorHitRate;
        if (p.edge > existing.bestEdge) existing.bestEdge = p.edge;
      }
    }
    const suggestedPlays = [...suggestedPlayMap.values()]
      .map((r) => ({
        outcomeType: r.outcomeType,
        confidence: r.scoreSum / r.count,
        posteriorHitRate: r.bestPosterior,
        edge: r.bestEdge,
        votes: r.count,
      }))
      .sort((a, b) => b.confidence - a.confidence)
      .slice(0, 5);

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
      predictions: enrichedPredictions,
      predictionCount: deduplicatedPatterns.length,
      discoveryV2Matches,
      suggestedPlays,
    };
  });

  return NextResponse.json({ date: dateStr, games: result });
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
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

async function getDiscoveryV2MatchesByGame(
  gameIds: string[],
): Promise<Map<string, DiscoveryV2Pattern[]>> {
  const out = new Map<string, DiscoveryV2Pattern[]>();
  for (const id of gameIds) out.set(id, []);
  if (gameIds.length === 0) return out;

  const inClause = gameIds.map((id) => `'${sqlEsc(id)}'`).join(",");
  const tokenRows = await prisma.$queryRawUnsafe<Array<{ gameId: string; tokens: string[] }>>(
    `SELECT "gameId", "tokens" FROM "GameFeatureToken" WHERE "gameId" IN (${inClause})`,
  );
  const patterns = await prisma.$queryRawUnsafe<DiscoveryV2Pattern[]>(
    `SELECT "id","outcomeType","conditions","posteriorHitRate","edge","score","n"
     FROM "PatternV2"
     WHERE "status" = 'deployed'
     ORDER BY "score" DESC`,
  );
  const tokensByGame = new Map(tokenRows.map((r) => [r.gameId, new Set(r.tokens ?? [])]));

  for (const p of patterns) {
    for (const gameId of gameIds) {
      const tokens = tokensByGame.get(gameId);
      if (!tokens) continue;
      if (!matchesConditions(tokens, p.conditions ?? [])) continue;
      out.get(gameId)?.push(p);
    }
  }
  for (const gameId of gameIds) {
    const list = out.get(gameId) ?? [];
    list.sort((a, b) => b.score - a.score || b.edge - a.edge);
    out.set(gameId, list.slice(0, 8));
  }
  return out;
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

type OddsLike = { spreadHome: number | null; totalOver: number | null } | undefined;
type PlayerContextLike = { teamId: number; rankPpg: number | null; rankRpg: number | null; rankApg: number | null }[];
type GameContextLike = {
  homeWins: number; homeLosses: number; homePpg: number; homeOppg: number;
  homeRankOff: number | null; homeRankDef: number | null; homeRankPace: number | null; homeStreak: number | null;
  homeRestDays: number | null; homeIsB2b: boolean;
  awayWins: number; awayLosses: number; awayPpg: number; awayOppg: number;
  awayRankOff: number | null; awayRankDef: number | null; awayRankPace: number | null; awayStreak: number | null;
  awayRestDays: number | null; awayIsB2b: boolean;
} | null;

function findMatchingPatterns(
  game: { id: string; homeTeamId: number; awayTeamId: number },
  homeSnap: TeamSnapshot | undefined,
  awaySnap: TeamSnapshot | undefined,
  patterns: { id: string; patternKey: string; conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; confidenceScore: number | null }[],
  targetDate: Date,
  odds?: OddsLike,
  playerContexts: PlayerContextLike = [],
  gameContext?: GameContextLike,
): { patternKey: string; patternId: string; conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; edge: number }[] {
  // Prefer GameContext when available (matches buildGameEvents / catalog exactly)
  const useCtx = gameContext != null;
  const home = useCtx
    ? {
        wins: gameContext.homeWins,
        losses: gameContext.homeLosses,
        ppg: gameContext.homePpg,
        oppg: gameContext.homeOppg,
        rankOff: gameContext.homeRankOff,
        rankDef: gameContext.homeRankDef,
        rankPace: gameContext.homeRankPace,
        streak: gameContext.homeStreak ?? 0,
        lastGameDate: null as Date | null,
      }
    : homeSnap;
  const away = useCtx
    ? {
        wins: gameContext!.awayWins,
        losses: gameContext!.awayLosses,
        ppg: gameContext!.awayPpg,
        oppg: gameContext!.awayOppg,
        rankOff: gameContext!.awayRankOff,
        rankDef: gameContext!.awayRankDef,
        rankPace: gameContext!.awayRankPace,
        streak: gameContext!.awayStreak ?? 0,
        lastGameDate: null as Date | null,
      }
    : awaySnap;

  if (!home || !away) return [];
  const homeSnapVal = home as TeamSnapshot;
  const awaySnapVal = away as TeamSnapshot;

  const activeConditions = new Set<string>();
  const homePlayerContexts = playerContexts.filter((p) => p.teamId === game.homeTeamId);
  const awayPlayerContexts = playerContexts.filter((p) => p.teamId === game.awayTeamId);

  // Rest/B2B: use GameContext when available (more accurate), else compute from lastGameDate
  const homeRestDays = useCtx && gameContext
    ? gameContext.homeRestDays
    : homeSnapVal.lastGameDate
      ? Math.floor((targetDate.getTime() - homeSnapVal.lastGameDate.getTime()) / 86_400_000) - 1
      : null;
  const awayRestDays = useCtx && gameContext
    ? gameContext.awayRestDays
    : awaySnapVal.lastGameDate
      ? Math.floor((targetDate.getTime() - awaySnapVal.lastGameDate.getTime()) / 86_400_000) - 1
      : null;
  const homeIsB2b = useCtx && gameContext ? gameContext.homeIsB2b : homeRestDays === 0;
  const awayIsB2b = useCtx && gameContext ? gameContext.awayIsB2b : awayRestDays === 0;

  // Offense tiers
  if (homeSnapVal.rankOff != null && homeSnapVal.rankOff <= 5) activeConditions.add("TOP_5_OFF:home");
  if (homeSnapVal.rankOff != null && homeSnapVal.rankOff <= 10) activeConditions.add("TOP_10_OFF:home");
  if (homeSnapVal.rankOff != null && homeSnapVal.rankOff >= 21) activeConditions.add("BOTTOM_10_OFF:home");
  if (homeSnapVal.rankOff != null && homeSnapVal.rankOff >= 26) activeConditions.add("BOTTOM_5_OFF:home");

  if (awaySnapVal.rankOff != null && awaySnapVal.rankOff <= 5) activeConditions.add("TOP_5_OFF:away");
  if (awaySnapVal.rankOff != null && awaySnapVal.rankOff <= 10) activeConditions.add("TOP_10_OFF:away");
  if (awaySnapVal.rankOff != null && awaySnapVal.rankOff >= 21) activeConditions.add("BOTTOM_10_OFF:away");
  if (awaySnapVal.rankOff != null && awaySnapVal.rankOff >= 26) activeConditions.add("BOTTOM_5_OFF:away");

  // Defense tiers
  if (homeSnapVal.rankDef != null && homeSnapVal.rankDef <= 5) activeConditions.add("TOP_5_DEF:home");
  if (homeSnapVal.rankDef != null && homeSnapVal.rankDef <= 10) activeConditions.add("TOP_10_DEF:home");
  if (homeSnapVal.rankDef != null && homeSnapVal.rankDef >= 21) activeConditions.add("BOTTOM_10_DEF:home");
  if (homeSnapVal.rankDef != null && homeSnapVal.rankDef >= 26) activeConditions.add("BOTTOM_5_DEF:home");

  if (awaySnapVal.rankDef != null && awaySnapVal.rankDef <= 5) activeConditions.add("TOP_5_DEF:away");
  if (awaySnapVal.rankDef != null && awaySnapVal.rankDef <= 10) activeConditions.add("TOP_10_DEF:away");
  if (awaySnapVal.rankDef != null && awaySnapVal.rankDef >= 21) activeConditions.add("BOTTOM_10_DEF:away");
  if (awaySnapVal.rankDef != null && awaySnapVal.rankDef >= 26) activeConditions.add("BOTTOM_5_DEF:away");

  // Pace
  if (homeSnapVal.rankPace != null && homeSnapVal.rankPace <= 10 && awaySnapVal.rankPace != null && awaySnapVal.rankPace <= 10) {
    activeConditions.add("BOTH_TOP_10_PACE:game");
  }
  if (homeSnapVal.rankPace != null && homeSnapVal.rankPace >= 21 && awaySnapVal.rankPace != null && awaySnapVal.rankPace >= 21) {
    activeConditions.add("BOTH_BOTTOM_10_PACE:game");
  }

  // Win percentage
  const homeWinPct = homeSnapVal.wins / Math.max(1, homeSnapVal.wins + homeSnapVal.losses);
  const awayWinPct = awaySnapVal.wins / Math.max(1, awaySnapVal.wins + awaySnapVal.losses);

  if (homeWinPct >= 0.7) activeConditions.add("WIN_PCT_OVER_700:home");
  if (homeWinPct >= 0.6) activeConditions.add("WIN_PCT_OVER_600:home");
  if (homeWinPct < 0.4) activeConditions.add("WIN_PCT_UNDER_400:home");
  if (homeSnapVal.wins > homeSnapVal.losses) activeConditions.add("WINNING_RECORD:home");

  if (awayWinPct >= 0.7) activeConditions.add("WIN_PCT_OVER_700:away");
  if (awayWinPct >= 0.6) activeConditions.add("WIN_PCT_OVER_600:away");
  if (awayWinPct < 0.4) activeConditions.add("WIN_PCT_UNDER_400:away");
  if (awaySnapVal.wins > awaySnapVal.losses) activeConditions.add("WINNING_RECORD:away");

  // Streaks
  if (homeSnapVal.streak >= 3) activeConditions.add("WIN_STREAK_3:home");
  if (homeSnapVal.streak >= 5) activeConditions.add("WIN_STREAK_5:home");
  if (homeSnapVal.streak >= 7) activeConditions.add("WIN_STREAK_7:home");
  if (homeSnapVal.streak <= -3) activeConditions.add("LOSING_STREAK_3:home");
  if (homeSnapVal.streak <= -5) activeConditions.add("LOSING_STREAK_5:home");
  if (homeSnapVal.streak <= -7) activeConditions.add("LOSING_STREAK_7:home");

  if (awaySnapVal.streak >= 3) activeConditions.add("WIN_STREAK_3:away");
  if (awaySnapVal.streak >= 5) activeConditions.add("WIN_STREAK_5:away");
  if (awaySnapVal.streak >= 7) activeConditions.add("WIN_STREAK_7:away");
  if (awaySnapVal.streak <= -3) activeConditions.add("LOSING_STREAK_3:away");
  if (awaySnapVal.streak <= -5) activeConditions.add("LOSING_STREAK_5:away");
  if (awaySnapVal.streak <= -7) activeConditions.add("LOSING_STREAK_7:away");

  // Rest days / B2B (homeRestDays, awayRestDays computed above)
  if (homeIsB2b) activeConditions.add("ON_B2B:home");
  if (awayIsB2b) activeConditions.add("ON_B2B:away");
  if (homeIsB2b && awayIsB2b) activeConditions.add("BOTH_ON_B2B:game");
  if (homeRestDays != null && homeRestDays >= 3) activeConditions.add("RESTED_3_PLUS:home");
  if (awayRestDays != null && awayRestDays >= 3) activeConditions.add("RESTED_3_PLUS:away");
  if (homeRestDays != null && homeRestDays >= 4) activeConditions.add("RESTED_4_PLUS:home");
  if (awayRestDays != null && awayRestDays >= 4) activeConditions.add("RESTED_4_PLUS:away");

  // Net rating
  const homeNet = homeSnapVal.ppg - homeSnapVal.oppg;
  const awayNet = awaySnapVal.ppg - awaySnapVal.oppg;
  if (homeNet >= 5) activeConditions.add("NET_RATING_PLUS_5:home");
  if (homeNet >= 10) activeConditions.add("NET_RATING_PLUS_10:home");
  if (homeNet <= -5) activeConditions.add("NET_RATING_MINUS_5:home");
  if (awayNet >= 5) activeConditions.add("NET_RATING_PLUS_5:away");
  if (awayNet >= 10) activeConditions.add("NET_RATING_PLUS_10:away");
  if (awayNet <= -5) activeConditions.add("NET_RATING_MINUS_5:away");

  // Scoring
  if (homeSnapVal.ppg >= 115) activeConditions.add("HIGH_SCORING:home");
  if (homeSnapVal.ppg < 105) activeConditions.add("LOW_SCORING:home");
  if (awaySnapVal.ppg >= 115) activeConditions.add("HIGH_SCORING:away");
  if (awaySnapVal.ppg < 105) activeConditions.add("LOW_SCORING:away");
  if (homeSnapVal.ppg >= 115 && awaySnapVal.ppg >= 115) activeConditions.add("BOTH_HIGH_SCORING:game");
  if (homeSnapVal.ppg < 105 && awaySnapVal.ppg < 105) activeConditions.add("BOTH_LOW_SCORING:game");

  // Defense
  if (homeSnapVal.oppg < 108) activeConditions.add("STINGY_DEF:home");
  if (homeSnapVal.oppg >= 118) activeConditions.add("POROUS_DEF:home");
  if (awaySnapVal.oppg < 108) activeConditions.add("STINGY_DEF:away");
  if (awaySnapVal.oppg >= 118) activeConditions.add("POROUS_DEF:away");
  if (homeSnapVal.oppg < 108 && awaySnapVal.oppg < 108) activeConditions.add("BOTH_STINGY_DEF:game");
  if (homeSnapVal.oppg >= 115 && awaySnapVal.oppg >= 115) activeConditions.add("BOTH_POROUS_DEF:game"); // Catalog uses 115 for both

  // Odds-based conditions (require consensus odds)
  if (odds?.spreadHome != null) {
    const absSpread = Math.abs(odds.spreadHome);
    if (absSpread < 3) activeConditions.add("SPREAD_UNDER_3:game");
    if (absSpread >= 3 && absSpread <= 7) activeConditions.add("SPREAD_3_TO_7:game");
    if (absSpread > 10) activeConditions.add("SPREAD_OVER_10:game");
  }
  if (odds?.totalOver != null) {
    if (odds.totalOver > 230) activeConditions.add("TOTAL_LINE_OVER_230:game");
    if (odds.totalOver > 235) activeConditions.add("TOTAL_LINE_OVER_235:game");
    if (odds.totalOver < 210) activeConditions.add("TOTAL_LINE_UNDER_210:game");
  }

  // Matchup conditions
  const homeTopOffVsAwayBadDef = (homeSnapVal.rankOff ?? 99) <= 10 && (awaySnapVal.rankDef ?? 0) >= 21;
  const awayTopOffVsHomeBadDef = (awaySnapVal.rankOff ?? 99) <= 10 && (homeSnapVal.rankDef ?? 0) >= 21;
  if (homeTopOffVsAwayBadDef || awayTopOffVsHomeBadDef) activeConditions.add("TOP_OFF_VS_BOTTOM_DEF:game");
  if ((homeSnapVal.rankOff ?? 99) <= 10 && (awaySnapVal.rankOff ?? 99) <= 10) activeConditions.add("BOTH_TOP_10_OFF:game");
  if (homeSnapVal.rankPace != null && awaySnapVal.rankPace != null && Math.abs(homeSnapVal.rankPace - awaySnapVal.rankPace) >= 15) {
    activeConditions.add("PACE_MISMATCH:game");
  }
  if (Math.abs(homeWinPct - awayWinPct) >= 0.3) activeConditions.add("BIG_FAVORITE:game");

  // Rest advantage (one team has 2+ more rest days than the other)
  if (homeRestDays != null && awayRestDays != null && homeRestDays - awayRestDays >= 2) activeConditions.add("REST_ADVANTAGE:home");
  if (homeRestDays != null && awayRestDays != null && awayRestDays - homeRestDays >= 2) activeConditions.add("REST_ADVANTAGE:away");

  // Player context conditions (require PlayerGameContext with ranks)
  if (homePlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 10)) activeConditions.add("HAS_TOP_10_SCORER:home");
  if (awayPlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 10)) activeConditions.add("HAS_TOP_10_SCORER:away");
  if (homePlayerContexts.some((p) => p.rankRpg != null && p.rankRpg <= 10)) activeConditions.add("HAS_TOP_10_REBOUNDER:home");
  if (awayPlayerContexts.some((p) => p.rankRpg != null && p.rankRpg <= 10)) activeConditions.add("HAS_TOP_10_REBOUNDER:away");
  if (homePlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 5)) activeConditions.add("HAS_TOP_5_SCORER:home");
  if (awayPlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 5)) activeConditions.add("HAS_TOP_5_SCORER:away");
  if (homePlayerContexts.some((p) => p.rankRpg != null && p.rankRpg <= 5)) activeConditions.add("HAS_TOP_5_REBOUNDER:home");
  if (awayPlayerContexts.some((p) => p.rankRpg != null && p.rankRpg <= 5)) activeConditions.add("HAS_TOP_5_REBOUNDER:away");
  if (homePlayerContexts.some((p) => p.rankApg != null && p.rankApg <= 10)) activeConditions.add("HAS_TOP_10_PLAYMAKER:home");
  if (awayPlayerContexts.some((p) => p.rankApg != null && p.rankApg <= 10)) activeConditions.add("HAS_TOP_10_PLAYMAKER:away");
  const homeHasStar = homePlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 10);
  const awayHasStar = awayPlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 10);
  if (homeHasStar && awayHasStar) activeConditions.add("STAR_MATCHUP:game");

  // TOP_5_SCORER_VS_BOTTOM_10_DEF: any top-5 scorer faces bottom-10 defense
  const homeTop5ScorerVsAwayBadDef = homePlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 5) && (awaySnapVal.rankDef ?? 0) >= 21;
  const awayTop5ScorerVsHomeBadDef = awayPlayerContexts.some((p) => p.rankPpg != null && p.rankPpg <= 5) && (homeSnapVal.rankDef ?? 0) >= 21;
  if (homeTop5ScorerVsAwayBadDef || awayTop5ScorerVsHomeBadDef) activeConditions.add("TOP_5_SCORER_VS_BOTTOM_10_DEF:game");

  // Generic player outcomes are not actionable - filter them out
  // These don't specify which team's player, so you can't actually bet on them
  const NON_ACTIONABLE_OUTCOMES = new Set([
    "PLAYER_10_PLUS_REBOUNDS:game",
    "PLAYER_10_PLUS_ASSISTS:game",
    "PLAYER_5_PLUS_THREES:game",
    "PLAYER_DOUBLE_DOUBLE:game",
    "PLAYER_TRIPLE_DOUBLE:game",
    "PLAYER_30_PLUS:game",
    "PLAYER_40_PLUS:game",
  ]);

  // Match patterns
  const matched: { patternKey: string; patternId: string; conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; edge: number }[] = [];

  for (const pattern of patterns) {
    // Skip non-actionable generic player outcomes
    if (NON_ACTIONABLE_OUTCOMES.has(pattern.outcome)) continue;

    if (pattern.conditions.every((c) => activeConditions.has(c))) {
      matched.push({
        patternKey: pattern.patternKey,
        patternId: pattern.id,
        conditions: pattern.conditions,
        outcome: pattern.outcome,
        hitRate: pattern.hitRate,
        hitCount: pattern.hitCount,
        sampleSize: pattern.sampleSize,
        seasons: pattern.seasons,
        edge: (pattern.hitRate - 0.524) * 100,
      });
    }
  }

  matched.sort((a, b) => (b.hitRate - a.hitRate) || (b.sampleSize - a.sampleSize));
  return matched;
}
