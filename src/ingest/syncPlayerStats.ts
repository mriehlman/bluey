import { prisma } from "../db/prisma.js";
import { fetchBoxScores, fetchStatsByDate, type BdlStat } from "../api/balldontlie.js";

function parseMinutes(min: string): number {
  if (!min || min === "" || min === "0" || min === "00:00") return 0;
  const parts = min.split(":");
  if (parts.length === 2) return Number(parts[0]) * 60 + Number(parts[1]);
  const n = Number(min);
  return isNaN(n) ? 0 : Math.round(n * 60);
}

function isDnp(stat: BdlStat): boolean {
  return (
    parseMinutes(stat.min) === 0 &&
    stat.pts === 0 &&
    stat.reb === 0 &&
    stat.ast === 0 &&
    stat.stl === 0 &&
    stat.blk === 0 &&
    stat.turnover === 0
  );
}

async function upsertPlayer(stat: BdlStat): Promise<void> {
  const { player } = stat;
  await prisma.player.upsert({
    where: { id: player.id },
    update: { firstname: player.first_name, lastname: player.last_name },
    create: { id: player.id, firstname: player.first_name, lastname: player.last_name },
  });
  await prisma.externalIdMap.upsert({
    where: {
      entityType_source_sourceId: {
        entityType: "PLAYER",
        source: "balldontlie",
        sourceId: String(player.id),
      },
    },
    update: { internalId: `player:${player.id}` },
    create: {
      entityType: "PLAYER",
      source: "balldontlie",
      sourceId: String(player.id),
      internalId: `player:${player.id}`,
    },
  });
}

async function upsertStat(stat: BdlStat, internalGameId: string): Promise<void> {
  const minutes = parseMinutes(stat.min);
  const data = {
    teamId: stat.team.id,
    minutes,
    points: stat.pts,
    assists: stat.ast,
    rebounds: stat.reb,
    steals: stat.stl,
    blocks: stat.blk,
    turnovers: stat.turnover,
    fgm: stat.fgm,
    fga: stat.fga,
    fg3m: stat.fg3m,
    fg3a: stat.fg3a,
    ftm: stat.ftm,
    fta: stat.fta,
    oreb: stat.oreb,
    dreb: stat.dreb,
    pf: stat.pf,
    plusMinus: null as number | null,
  };

  await prisma.playerGameStat.upsert({
    where: { gameId_playerId: { gameId: internalGameId, playerId: stat.player.id } },
    update: data,
    create: { gameId: internalGameId, playerId: stat.player.id, ...data },
  });
}

/** Resolve BallDontLie game ID from GameExternalId or legacy sourceGameId. */
async function getBallDontLieGameId(gameId: string): Promise<number | null> {
  const ext = await prisma.gameExternalId.findUnique({
    where: { gameId_source: { gameId, source: "balldontlie" } },
    select: { sourceId: true },
  });
  if (ext) {
    const n = Number(ext.sourceId);
    return Number.isInteger(n) && n > 0 ? n : null;
  }
  const game = await prisma.game.findUnique({
    where: { id: gameId },
    select: { sourceGameId: true },
  });
  if (game && game.sourceGameId > 0) return game.sourceGameId;
  return null;
}

export async function syncPlayerStatsForDate(date: string): Promise<number> {
  console.log(`Syncing player stats for ${date}...`);

  const dbGames = await prisma.game.findMany({
    where: { date: new Date(date + "T00:00:00Z") },
    select: { id: true },
  });

  if (dbGames.length === 0) {
    console.log("  No games found in DB for this date. Run sync:games first.");
    return 0;
  }

  const gameIdToBdlId = new Map<string, number>();
  for (const g of dbGames) {
    const bdlId = await getBallDontLieGameId(g.id);
    if (bdlId != null) gameIdToBdlId.set(g.id, bdlId);
  }

  const validGames = dbGames.filter((g) => gameIdToBdlId.has(g.id));
  const skipped = dbGames.length - validGames.length;
  if (skipped > 0) {
    console.log(`  Skipping ${skipped} games without BallDontLie IDs`);
  }
  if (validGames.length === 0) {
    console.log("  No games with BallDontLie IDs to sync. Run sync:games first.");
    return 0;
  }

  console.log(`  Found ${validGames.length} games to sync`);

  let totalStats = 0;

  for (const dbGame of validGames) {
    const bdlId = gameIdToBdlId.get(dbGame.id)!;
    const stats = await fetchBoxScores(bdlId);

    for (const stat of stats) {
      if (isDnp(stat)) continue;
      await upsertPlayer(stat);
      await upsertStat(stat, dbGame.id);
      totalStats++;
    }

    process.stdout.write(`\r  Stats upserted: ${totalStats}`);
  }

  console.log(`\n  Synced ${totalStats} player stat rows across ${validGames.length} games`);
  return totalStats;
}

export async function syncPlayerStatsForGames(internalGameIds: string[]): Promise<number> {
  let totalStats = 0;

  for (const gameId of internalGameIds) {
    const bdlId = await getBallDontLieGameId(gameId);
    if (!bdlId) continue;

    const stats = await fetchBoxScores(bdlId);
    for (const stat of stats) {
      if (isDnp(stat)) continue;
      await upsertPlayer(stat);
      await upsertStat(stat, gameId);
      totalStats++;
    }
  }

  return totalStats;
}
