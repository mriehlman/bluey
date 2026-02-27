import { prisma } from "../db/prisma.js";
import type { RollupFilters } from "./filters.js";
import { buildGameWhere, buildStatWhere } from "./filters.js";

export interface RollupResult {
  gamesPlayed: number;
  minutes: number;
  points: number;
  assists: number;
  rebounds: number;
  steals: number;
  blocks: number;
  turnovers: number;
}

export async function getPlayerTotals(
  playerId: number,
  filters: RollupFilters = {}
): Promise<RollupResult> {
  const statWhere = buildStatWhere(filters);

  const teamIds = await prisma.playerGameStat.findMany({
    where: { playerId },
    select: { teamId: true },
    distinct: ["teamId"],
  });

  const gameWhereConditions = teamIds.map((t) =>
    buildGameWhere(filters, t.teamId)
  );

  const baseWhere = {
    playerId,
    ...statWhere,
    game:
      gameWhereConditions.length > 0
        ? { OR: gameWhereConditions }
        : buildGameWhere(filters),
  };

  const agg = await prisma.playerGameStat.aggregate({
    where: baseWhere,
    _sum: {
      minutes: true,
      points: true,
      assists: true,
      rebounds: true,
      steals: true,
      blocks: true,
      turnovers: true,
    },
  });

  const distinctGames = await prisma.playerGameStat.findMany({
    where: baseWhere,
    select: { gameId: true },
    distinct: ["gameId"],
  });

  return {
    gamesPlayed: distinctGames.length,
    minutes: agg._sum.minutes ?? 0,
    points: agg._sum.points ?? 0,
    assists: agg._sum.assists ?? 0,
    rebounds: agg._sum.rebounds ?? 0,
    steals: agg._sum.steals ?? 0,
    blocks: agg._sum.blocks ?? 0,
    turnovers: agg._sum.turnovers ?? 0,
  };
}
