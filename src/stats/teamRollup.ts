import { prisma } from "../db/prisma.js";
import type { RollupFilters } from "./filters.js";
import { buildGameWhere, buildStatWhere } from "./filters.js";
import type { RollupResult } from "./playerRollup.js";

export async function getTeamTotals(
  teamId: number,
  filters: RollupFilters = {}
): Promise<RollupResult> {
  const statWhere = buildStatWhere(filters);
  const gameWhere = buildGameWhere(filters, teamId);

  const baseWhere = {
    teamId,
    ...statWhere,
    game: gameWhere,
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
