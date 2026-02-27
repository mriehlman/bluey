import { join } from "path";
import { prisma } from "../db/prisma.js";
import { readJson, parseMinutesToSeconds, chunkArray } from "./utils.js";

interface RawPlayerStat {
  player: { id: number | null; firstname?: string; lastname?: string };
  team: { id: number; name?: string; code?: string; nickname?: string };
  game: { id: number };
  min?: string | null;
  points?: number;
  assists?: number;
  totReb?: number;
  steals?: number;
  blocks?: number;
  turnovers?: number;
  comment?: string | null;
}

interface PerGameWrapper {
  response: RawPlayerStat[];
  [key: string]: unknown;
}

function isDnp(row: RawPlayerStat): boolean {
  const comment = (row.comment ?? "").toLowerCase();
  if (comment.includes("dnp") || comment.includes("did not play")) return true;

  const mins = parseMinutesToSeconds(row.min);
  const pts = row.points ?? 0;
  const reb = row.totReb ?? 0;
  const ast = row.assists ?? 0;
  const stl = row.steals ?? 0;
  const blk = row.blocks ?? 0;
  const tov = row.turnovers ?? 0;

  return mins === 0 && pts === 0 && reb === 0 && ast === 0 && stl === 0 && blk === 0 && tov === 0;
}

export async function ingestPlayerStats(): Promise<void> {
  const dataDir = process.env.DATA_DIR ?? "./data";
  const raw = await readJson<PerGameWrapper[] | { response: RawPlayerStat[] }>(
    join(dataDir, "playerstats.json")
  );

  let rows: RawPlayerStat[];
  if (Array.isArray(raw)) {
    rows = raw.flatMap((wrapper) => wrapper.response ?? []);
  } else {
    rows = raw.response;
  }

  console.log(`Loaded ${rows.length} player stat rows`);

  const allGames = await prisma.game.findMany({ select: { id: true, sourceGameId: true } });
  const gameMap = new Map<number, string>();
  for (const g of allGames) gameMap.set(g.sourceGameId, g.id);
  console.log(`Game map: ${gameMap.size} games in DB`);

  let skipNull = 0;
  let skipDnp = 0;
  let skipOrphan = 0;

  const playersToUpsert = new Map<number, { firstname?: string; lastname?: string }>();
  const teamsToUpsert = new Map<number, { name?: string; code?: string }>();
  const orphanRows: {
    sourceGameId: number;
    teamId: number;
    playerId: number | null;
    reason: string;
  }[] = [];
  const statRows: {
    gameId: string;
    playerId: number;
    teamId: number;
    minutes: number;
    points: number;
    assists: number;
    rebounds: number;
    steals: number;
    blocks: number;
    turnovers: number;
  }[] = [];

  for (const row of rows) {
    if (row.player.id == null) {
      skipNull++;
      continue;
    }

    if (isDnp(row)) {
      skipDnp++;
      continue;
    }

    const internalGameId = gameMap.get(row.game.id);
    if (!internalGameId) {
      skipOrphan++;
      orphanRows.push({
        sourceGameId: row.game.id,
        teamId: row.team.id,
        playerId: row.player.id,
        reason: "NO_GAME_MATCH",
      });
      continue;
    }

    const playerId = row.player.id;
    if (!playersToUpsert.has(playerId)) {
      playersToUpsert.set(playerId, {
        firstname: row.player.firstname,
        lastname: row.player.lastname,
      });
    }

    if (!teamsToUpsert.has(row.team.id)) {
      teamsToUpsert.set(row.team.id, {
        name: row.team.name ?? row.team.nickname,
        code: row.team.code,
      });
    }

    statRows.push({
      gameId: internalGameId,
      playerId,
      teamId: row.team.id,
      minutes: parseMinutesToSeconds(row.min),
      points: row.points ?? 0,
      assists: row.assists ?? 0,
      rebounds: row.totReb ?? 0,
      steals: row.steals ?? 0,
      blocks: row.blocks ?? 0,
      turnovers: row.turnovers ?? 0,
    });
  }

  console.log(
    `Parsed ${rows.length} rows: ${statRows.length} valid, ` +
      `${skipNull} null player, ${skipDnp} DNP, ${skipOrphan} orphan game`
  );

  console.log(`Upserting ${playersToUpsert.size} players...`);
  let playerCount = 0;
  for (const [id, p] of playersToUpsert) {
    await prisma.player.upsert({
      where: { id },
      update: { firstname: p.firstname ?? null, lastname: p.lastname ?? null },
      create: { id, firstname: p.firstname ?? null, lastname: p.lastname ?? null },
    });
    playerCount++;
    if (playerCount % 200 === 0) process.stdout.write(`\r  Players: ${playerCount}/${playersToUpsert.size}`);
  }
  console.log(`\n  Upserted ${playersToUpsert.size} players`);

  for (const [id, t] of teamsToUpsert) {
    await prisma.team.upsert({
      where: { id },
      update: { name: t.name ?? null, code: t.code ?? null },
      create: { id, name: t.name ?? null, code: t.code ?? null },
    });
  }
  console.log(`  Upserted ${teamsToUpsert.size} teams`);

  let inserted = 0;
  const chunks = chunkArray(statRows, 500);
  for (let i = 0; i < chunks.length; i++) {
    const result = await prisma.playerGameStat.createMany({
      data: chunks[i],
      skipDuplicates: true,
    });
    inserted += result.count;
    if ((i + 1) % 20 === 0) process.stdout.write(`\r  Stats chunks: ${i + 1}/${chunks.length}`);
  }

  console.log(`\nInserted ${inserted} PlayerGameStat rows (${statRows.length - inserted} duplicates skipped)`);

  if (orphanRows.length > 0) {
    console.log(`\nLogging ${orphanRows.length} orphan player stat rows...`);
    const orphanChunks = chunkArray(orphanRows, 500);
    let orphanInserted = 0;
    for (const chunk of orphanChunks) {
      const result = await prisma.orphanPlayerStat.createMany({
        data: chunk.map((o) => ({
          sourceGameId: o.sourceGameId,
          teamId: o.teamId,
          playerId: o.playerId,
          reason: o.reason,
        })),
        skipDuplicates: true,
      });
      orphanInserted += result.count;
    }
    console.log(`  Logged ${orphanInserted} orphan rows to OrphanPlayerStat`);
  }
}
