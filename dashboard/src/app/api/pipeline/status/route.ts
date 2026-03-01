import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

async function safeCount(model: { count: () => Promise<number> } | undefined): Promise<number> {
  if (!model) return 0;
  try {
    return await model.count();
  } catch {
    return 0;
  }
}

export async function GET() {
  const p = prisma as Record<string, { count: () => Promise<number> } | undefined>;
  
  const [
    games,
    playerStats,
    teams,
    players,
    gameOdds,
    playerPropOdds,
    nightAggregates,
    nightEvents,
    nights,
    patterns,
    patternHits,
    watchlistItems,
    gameContexts,
    playerGameContexts,
    gameEvents,
    gamePatterns,
    gamePatternHits,
  ] = await Promise.all([
    safeCount(p.game),
    safeCount(p.playerGameStat),
    safeCount(p.team),
    safeCount(p.player),
    safeCount(p.gameOdds),
    safeCount(p.playerPropOdds),
    safeCount(p.nightTeamAggregate),
    safeCount(p.nightEvent),
    safeCount(p.night),
    safeCount(p.pattern),
    safeCount(p.patternHit),
    safeCount(p.patternWatchlist),
    safeCount(p.gameContext),
    safeCount(p.playerGameContext),
    safeCount(p.gameEvent),
    safeCount(p.gamePattern),
    safeCount(p.gamePatternHit),
  ]);

  const latestNight = await prisma.night.findFirst({
    orderBy: { date: "desc" },
    select: { date: true, processedAt: true },
  });

  const latestGame = await prisma.game.findFirst({
    orderBy: { date: "desc" },
    select: { date: true },
  });

  const latestOdds = await prisma.gameOdds.findFirst({
    orderBy: { fetchedAt: "desc" },
    select: { fetchedAt: true },
  });

  const seasons = await prisma.game.findMany({
    select: { season: true },
    distinct: ["season"],
    orderBy: { season: "asc" },
  });

  return NextResponse.json({
    counts: {
      games,
      playerStats,
      teams,
      players,
      gameOdds,
      playerPropOdds,
      nightAggregates,
      nightEvents,
      nights,
      patterns,
      patternHits,
      watchlistItems,
      gameContexts,
      playerGameContexts,
      gameEvents,
      gamePatterns,
      gamePatternHits,
    },
    latestGameDate: latestGame?.date ?? null,
    latestNightDate: latestNight?.date ?? null,
    latestNightProcessedAt: latestNight?.processedAt ?? null,
    latestOddsFetchedAt: latestOdds?.fetchedAt ?? null,
    seasons: seasons.map((s) => s.season),
  });
}
