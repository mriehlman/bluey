import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

export async function GET() {
  const [
    games,
    playerStats,
    teams,
    players,
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
    prisma.game.count(),
    prisma.playerGameStat.count(),
    prisma.team.count(),
    prisma.player.count(),
    prisma.nightTeamAggregate.count(),
    prisma.nightEvent.count(),
    prisma.night.count(),
    prisma.pattern.count(),
    prisma.patternHit.count(),
    prisma.patternWatchlist.count(),
    prisma.gameContext.count(),
    prisma.playerGameContext.count(),
    prisma.gameEvent.count(),
    prisma.gamePattern.count(),
    prisma.gamePatternHit.count(),
  ]);

  const latestNight = await prisma.night.findFirst({
    orderBy: { date: "desc" },
    select: { date: true, processedAt: true },
  });

  const latestGame = await prisma.game.findFirst({
    orderBy: { date: "desc" },
    select: { date: true },
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
    seasons: seasons.map((s) => s.season),
  });
}
