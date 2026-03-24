import { prisma } from "@bluey/db";
import {
  fetchGames,
  fetchGamesByDateRange,
  fetchSeasonGames,
  type BdlGame,
  type BdlTeam,
} from "../api/balldontlie";

async function rehomeOddsFromShadowGames(args: {
  canonicalGameId: string;
  canonicalSourceGameId: number;
  date: Date;
  homeTeamId: number;
  awayTeamId: number;
}): Promise<number> {
  const shadows = await prisma.game.findMany({
    where: {
      id: { not: args.canonicalGameId },
      sourceGameId: { lt: 0 },
      date: args.date,
      homeTeamId: args.homeTeamId,
      awayTeamId: args.awayTeamId,
    },
    select: { id: true },
  });
  if (shadows.length === 0) return 0;

  let moved = 0;
  for (const shadow of shadows) {
    const oddsRows = await prisma.gameOdds.findMany({
      where: { gameId: shadow.id },
      select: {
        source: true,
        spreadHome: true,
        spreadAway: true,
        totalOver: true,
        totalUnder: true,
        mlHome: true,
        mlAway: true,
        fetchedAt: true,
      },
    });
    for (const row of oddsRows) {
      await prisma.gameOdds.upsert({
        where: {
          gameId_source: {
            gameId: args.canonicalGameId,
            source: row.source,
          },
        },
        update: {
          spreadHome: row.spreadHome,
          spreadAway: row.spreadAway,
          totalOver: row.totalOver,
          totalUnder: row.totalUnder,
          mlHome: row.mlHome,
          mlAway: row.mlAway,
          fetchedAt: row.fetchedAt,
        },
        create: {
          gameId: args.canonicalGameId,
          source: row.source,
          spreadHome: row.spreadHome,
          spreadAway: row.spreadAway,
          totalOver: row.totalOver,
          totalUnder: row.totalUnder,
          mlHome: row.mlHome,
          mlAway: row.mlAway,
          fetchedAt: row.fetchedAt,
        },
      });
      moved++;
    }
  }
  if (moved > 0) {
    console.log(
      `  Rehomed ${moved} GameOdds rows onto canonical game ${args.canonicalSourceGameId} (${args.canonicalGameId})`,
    );
  }
  return moved;
}

async function upsertTeam(team: BdlTeam): Promise<void> {
  await prisma.team.upsert({
    where: { id: team.id },
    update: { name: team.full_name, code: team.abbreviation, city: team.city },
    create: { id: team.id, name: team.full_name, code: team.abbreviation, city: team.city },
  });

  await prisma.externalIdMap.upsert({
    where: {
      entityType_source_sourceId: {
        entityType: "TEAM",
        source: "balldontlie",
        sourceId: String(team.id),
      },
    },
    update: { internalId: `team:${team.id}` },
    create: {
      entityType: "TEAM",
      source: "balldontlie",
      sourceId: String(team.id),
      internalId: `team:${team.id}`,
    },
  });
}

async function upsertGame(game: BdlGame, includeScheduled = false): Promise<string> {
  // Skip non-final games unless includeScheduled is true
  if (game.status !== "Final" && !includeScheduled) return "";
  // Always skip in-progress games
  if (game.status !== "Final" && game.status !== "Scheduled" && !game.status.includes("scheduled")) return "";

  await upsertTeam(game.home_team);
  await upsertTeam(game.visitor_team);

  const dateMatch = game.date.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (!dateMatch) throw new Error(`Cannot parse date: ${game.date}`);
  const date = new Date(
    Date.UTC(Number(dateMatch[1]), Number(dateMatch[2]) - 1, Number(dateMatch[3])),
  );

  const existingBySource = await prisma.game.findUnique({
    where: { sourceGameId: game.id },
    select: { id: true },
  });

  if (!existingBySource) {
    // If this game was previously created from odds ingest with a synthetic negative sourceGameId,
    // claim that row instead of creating a duplicate canonical game row.
    const shadow = await prisma.game.findFirst({
      where: {
        sourceGameId: { lt: 0 },
        date,
        homeTeamId: game.home_team.id,
        awayTeamId: game.visitor_team.id,
      },
      select: { id: true },
    });
    if (shadow) {
      const claimed = await prisma.game.update({
        where: { id: shadow.id },
        data: {
          sourceGameId: game.id,
          date,
          season: game.season,
          stage: game.postseason ? 4 : 2,
          league: "standard",
          homeTeamId: game.home_team.id,
          awayTeamId: game.visitor_team.id,
          homeScore: game.home_team_score,
          awayScore: game.visitor_team_score,
          homeTeamNameSnapshot: game.home_team.full_name,
          awayTeamNameSnapshot: game.visitor_team.full_name,
          seasonType: game.postseason ? "POST" : "REG",
        },
      });

      await prisma.externalIdMap.upsert({
        where: {
          entityType_source_sourceId: {
            entityType: "GAME",
            source: "balldontlie",
            sourceId: String(game.id),
          },
        },
        update: { internalId: `game:${claimed.id}` },
        create: {
          entityType: "GAME",
          source: "balldontlie",
          sourceId: String(game.id),
          internalId: `game:${claimed.id}`,
        },
      });

      await prisma.gameExternalId.upsert({
        where: {
          gameId_source: { gameId: claimed.id, source: "balldontlie" },
        },
        update: { sourceId: String(game.id) },
        create: { gameId: claimed.id, source: "balldontlie", sourceId: String(game.id) },
      });

      return claimed.id;
    }
  }

  const result = await prisma.game.upsert({
    where: { sourceGameId: game.id },
    update: {
      date,
      season: game.season,
      stage: game.postseason ? 4 : 2,
      league: "standard",
      homeTeamId: game.home_team.id,
      awayTeamId: game.visitor_team.id,
      homeScore: game.home_team_score,
      awayScore: game.visitor_team_score,
      homeTeamNameSnapshot: game.home_team.full_name,
      awayTeamNameSnapshot: game.visitor_team.full_name,
      seasonType: game.postseason ? "POST" : "REG",
    },
    create: {
      sourceGameId: game.id,
      date,
      season: game.season,
      stage: game.postseason ? 4 : 2,
      league: "standard",
      homeTeamId: game.home_team.id,
      awayTeamId: game.visitor_team.id,
      homeScore: game.home_team_score,
      awayScore: game.visitor_team_score,
      homeTeamNameSnapshot: game.home_team.full_name,
      awayTeamNameSnapshot: game.visitor_team.full_name,
      seasonType: game.postseason ? "POST" : "REG",
    },
  });

  await prisma.externalIdMap.upsert({
    where: {
      entityType_source_sourceId: {
        entityType: "GAME",
        source: "balldontlie",
        sourceId: String(game.id),
      },
    },
    update: { internalId: `game:${result.id}` },
    create: {
      entityType: "GAME",
      source: "balldontlie",
      sourceId: String(game.id),
      internalId: `game:${result.id}`,
    },
  });

  await prisma.gameExternalId.upsert({
    where: {
      gameId_source: { gameId: result.id, source: "balldontlie" },
    },
    update: { sourceId: String(game.id) },
    create: { gameId: result.id, source: "balldontlie", sourceId: String(game.id) },
  });

  await rehomeOddsFromShadowGames({
    canonicalGameId: result.id,
    canonicalSourceGameId: game.id,
    date,
    homeTeamId: game.home_team.id,
    awayTeamId: game.visitor_team.id,
  });

  return result.id;
}

export async function syncGamesForDate(date: string): Promise<number> {
  console.log(`Fetching games for ${date}...`);
  const games = await fetchGames(date);
  console.log(`  Found ${games.length} games from balldontlie`);

  let upserted = 0;
  for (const game of games) {
    if (await upsertGame(game)) upserted++;
  }
  console.log(`  Upserted ${upserted} games`);
  return upserted;
}

export async function syncGamesForRange(startDate: string, endDate: string): Promise<number> {
  console.log(`Fetching games from ${startDate} to ${endDate}...`);
  const games = await fetchGamesByDateRange(startDate, endDate);
  console.log(`  Found ${games.length} games from balldontlie`);

  let upserted = 0;
  for (const game of games) {
    if (await upsertGame(game)) {
      upserted++;
      if (upserted % 50 === 0) process.stdout.write(`\r  Games upserted: ${upserted}/${games.length}`);
    }
  }
  console.log(`\n  Upserted ${upserted} games`);
  return upserted;
}

export async function syncGamesForSeason(season: number): Promise<BdlGame[]> {
  console.log(`Fetching all games for season ${season}...`);
  const games = await fetchSeasonGames(season);
  console.log(`  Found ${games.length} games from balldontlie`);

  let upserted = 0;
  for (const game of games) {
    if (await upsertGame(game)) {
      upserted++;
      if (upserted % 50 === 0) process.stdout.write(`\r  Games upserted: ${upserted}/${games.length}`);
    }
  }
  console.log(`\n  Upserted ${upserted} games for season ${season}`);
  return games;
}

/** Backfill GameExternalId for games that have sourceGameId > 0 (BallDontLie) but no external ID row. */
export async function backfillGameExternalIds(): Promise<number> {
  const gamesWithBdlSourceId = await prisma.game.findMany({
    where: { sourceGameId: { gt: 0 } },
    select: { id: true, sourceGameId: true },
  });

  const withExtId = await prisma.gameExternalId.findMany({
    where: { source: "balldontlie", gameId: { in: gamesWithBdlSourceId.map((g) => g.id) } },
    select: { gameId: true },
  });
  const hasExtId = new Set(withExtId.map((e) => e.gameId));

  let created = 0;
  for (const g of gamesWithBdlSourceId) {
    if (hasExtId.has(g.id)) continue;
    await prisma.gameExternalId.create({
      data: { gameId: g.id, source: "balldontlie", sourceId: String(g.sourceGameId) },
    });
    created++;
  }
  console.log(`Backfilled ${created} GameExternalId rows for balldontlie`);
  return created;
}
