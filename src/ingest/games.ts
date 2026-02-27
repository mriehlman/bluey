import { join } from "path";
import { prisma } from "../db/prisma.js";
import { readJson, parseDateOnly, isRegularSeasonGame, chunkArray } from "./utils.js";

interface RawTeam {
  id: number;
  name?: string;
  code?: string;
  nickname?: string;
}

interface RawGame {
  id: number;
  date: { start: string; end?: string; duration?: string };
  stage?: string | number;
  league?: string;
  season: number;
  teams: {
    visitors: RawTeam;
    home: RawTeam;
  };
  scores: {
    visitors: { points: number | null };
    home: { points: number | null };
  };
}

export async function ingestGames(): Promise<void> {
  const dataDir = process.env.DATA_DIR ?? "./data";
  const raw = await readJson<{ games: RawGame[] } | RawGame[] | { response: RawGame[] }>(
    join(dataDir, "games.json")
  );

  let allGames: RawGame[];
  if (Array.isArray(raw)) {
    allGames = raw;
  } else if ("games" in raw) {
    allGames = raw.games;
  } else {
    allGames = raw.response;
  }

  const regularSeason = allGames.filter(isRegularSeasonGame);
  const skipped = allGames.length - regularSeason.length;

  console.log(`Loaded ${allGames.length} games, ${regularSeason.length} regular season, ${skipped} skipped`);

  const teamMap = new Map<number, RawTeam>();
  for (const g of regularSeason) {
    teamMap.set(g.teams.home.id, g.teams.home);
    teamMap.set(g.teams.visitors.id, g.teams.visitors);
  }

  let teamsUpserted = 0;
  for (const t of teamMap.values()) {
    await prisma.team.upsert({
      where: { id: t.id },
      update: { name: t.name ?? t.nickname ?? null, code: t.code ?? null },
      create: { id: t.id, name: t.name ?? t.nickname ?? null, code: t.code ?? null },
    });
    teamsUpserted++;
  }

  let gamesUpserted = 0;
  const chunks = chunkArray(regularSeason, 100);
  for (const chunk of chunks) {
    for (const g of chunk) {
      const date = parseDateOnly(g.date.start);
      const stage = Number(g.stage) || 2;
      const league = g.league ?? "standard";
      await prisma.game.upsert({
        where: { sourceGameId: g.id },
        update: {
          date,
          season: g.season,
          stage,
          league,
          homeTeamId: g.teams.home.id,
          awayTeamId: g.teams.visitors.id,
          homeScore: g.scores.home.points ?? 0,
          awayScore: g.scores.visitors.points ?? 0,
        },
        create: {
          sourceGameId: g.id,
          date,
          season: g.season,
          stage,
          league,
          homeTeamId: g.teams.home.id,
          awayTeamId: g.teams.visitors.id,
          homeScore: g.scores.home.points ?? 0,
          awayScore: g.scores.visitors.points ?? 0,
        },
      });
      gamesUpserted++;
    }
    process.stdout.write(`\r  Games upserted: ${gamesUpserted}/${regularSeason.length}`);
  }

  console.log(`\nUpserted ${teamsUpserted} teams, ${gamesUpserted} games (${skipped} skipped preseason)`);
}
