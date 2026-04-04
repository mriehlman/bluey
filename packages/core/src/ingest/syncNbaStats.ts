import { prisma } from "@bluey/db";
import { fetchBoxScoresForDate, fetchGamesForDate } from "../api/nbaStats";
import { dateStringToUtcMidday } from "./utils";

function parseMinutes(min: string): number {
  if (!min || min === "" || min === "0" || min === "0:00") return 0;
  const parts = min.split(":");
  if (parts.length === 2) return Number(parts[0]) * 60 + Number(parts[1]);
  const n = Number(min);
  return isNaN(n) ? 0 : Math.round(n * 60);
}

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash);
}

function getSeasonFromDate(dateStr: string): number {
  const [y] = dateStr.split("-").map(Number);
  const m = new Date(dateStr).getMonth();
  return m >= 9 ? y : y - 1; // Oct+ = current season start year
}

// NBA API team ID to balldontlie team ID mapping
const NBA_TO_BDL_TEAM: Record<number, number> = {
  1610612737: 1,  // Atlanta Hawks
  1610612738: 2,  // Boston Celtics
  1610612751: 3,  // Brooklyn Nets
  1610612766: 4,  // Charlotte Hornets
  1610612741: 5,  // Chicago Bulls
  1610612739: 6,  // Cleveland Cavaliers
  1610612742: 7,  // Dallas Mavericks
  1610612743: 8,  // Denver Nuggets
  1610612765: 9,  // Detroit Pistons
  1610612744: 10, // Golden State Warriors
  1610612745: 11, // Houston Rockets
  1610612754: 12, // Indiana Pacers
  1610612746: 13, // LA Clippers
  1610612747: 14, // Los Angeles Lakers
  1610612763: 15, // Memphis Grizzlies
  1610612748: 16, // Miami Heat
  1610612749: 17, // Milwaukee Bucks
  1610612750: 18, // Minnesota Timberwolves
  1610612740: 19, // New Orleans Pelicans
  1610612752: 20, // New York Knicks
  1610612760: 21, // Oklahoma City Thunder
  1610612753: 22, // Orlando Magic
  1610612755: 23, // Philadelphia 76ers
  1610612756: 24, // Phoenix Suns
  1610612757: 25, // Portland Trail Blazers
  1610612758: 26, // Sacramento Kings
  1610612759: 27, // San Antonio Spurs
  1610612761: 28, // Toronto Raptors
  1610612762: 29, // Utah Jazz
  1610612764: 30, // Washington Wizards
};

export async function syncNbaStatsForDate(date: string): Promise<number> {
  console.log(`\nSyncing NBA stats for ${date} using Python nba_api...\n`);

  // Fetch games and box scores
  const boxScores = await fetchBoxScoresForDate(date);
  console.log(`  Fetched ${boxScores.length} games with box scores`);

  let totalStats = 0;

  for (const box of boxScores) {
    // Convert NBA API team IDs to balldontlie IDs
    const homeTeamBdl = NBA_TO_BDL_TEAM[box.homeTeamId ?? 0];
    const awayTeamBdl = NBA_TO_BDL_TEAM[box.awayTeamId ?? 0];
    
    if (!homeTeamBdl || !awayTeamBdl) {
      console.log(`  Unknown team IDs: home=${box.homeTeamId}, away=${box.awayTeamId}`);
      continue;
    }

    // Find the game in our DB by matching teams and date (use same format as syncOdds)
    const gameDate = dateStringToUtcMidday(box.date ?? date);
    
    let game = await prisma.game.findFirst({
      where: {
        date: gameDate,
        OR: [
          { homeTeamId: homeTeamBdl, awayTeamId: awayTeamBdl },
          { homeTeamId: awayTeamBdl, awayTeamId: homeTeamBdl },
        ],
      },
    });

    if (!game) {
      // Create game from nba_api data (no BallDontLie required)
      const nbaGameIdNum = parseInt(box.gameId, 10) || Math.abs(hashString(box.gameId));
      const existingBySourceId = await prisma.game.findUnique({ where: { sourceGameId: nbaGameIdNum } });
      if (existingBySourceId) {
        game = existingBySourceId;
        await prisma.gameExternalId.upsert({
          where: { gameId_source: { gameId: game.id, source: "nba_stats" } },
          update: { sourceId: box.gameId },
          create: { gameId: game.id, source: "nba_stats", sourceId: box.gameId },
        });
      } else {
        game = await prisma.game.create({
          data: {
            sourceGameId: nbaGameIdNum,
            date: gameDate,
            season: getSeasonFromDate(box.date ?? date),
            stage: 2,
            league: "standard",
            homeTeamId: homeTeamBdl,
            awayTeamId: awayTeamBdl,
            homeScore: box.homeScore ?? 0,
            awayScore: box.awayScore ?? 0,
            status: (box as any).status ?? "Final",
            tipoffTimeUtc: gameDate,
          },
        });
        await prisma.gameExternalId.create({
          data: { gameId: game.id, source: "nba_stats", sourceId: box.gameId },
        });
        console.log(`  Created game ${box.gameId} (${game.id})`);
      }
    } else {
      const statusIsNotFinal = !game.status?.trim().includes("Final");
      const newHasScores = (box.homeScore ?? 0) > 0 || (box.awayScore ?? 0) > 0;
      if (statusIsNotFinal && newHasScores) {
        await prisma.game.update({
          where: { id: game.id },
          data: {
            homeScore: box.homeScore ?? 0,
            awayScore: box.awayScore ?? 0,
            status: (box as any).status ?? "Final",
          },
        });
        console.log(`  Updated game ${game.id} to final: ${box.homeScore}-${box.awayScore}`);
      }
    }

    // Upsert player stats
    for (const stat of box.playerStats) {
      const s = stat as any;
      // Convert NBA team ID to balldontlie team ID
      const teamIdBdl = NBA_TO_BDL_TEAM[stat.teamId] ?? stat.teamId;
      
      const playerName = stat.playerName ?? ([s.firstName, s.familyName].filter(Boolean).join(" ").trim() || "Unknown");
      const minutes = (stat as { minutesPlayed?: number }).minutesPlayed != null
        ? Math.round(Number((stat as { minutesPlayed?: number }).minutesPlayed) * 60)
        : parseMinutes(stat.minutes ?? "");
      
      // Ensure player exists
      const [first, ...rest] = playerName.split(" ");
      await prisma.player.upsert({
        where: { id: stat.playerId },
        update: { firstname: first ?? "", lastname: rest.join(" ") },
        create: { id: stat.playerId, firstname: first ?? "", lastname: rest.join(" ") },
      });

      await prisma.playerGameStat.upsert({
        where: { gameId_playerId: { gameId: game.id, playerId: stat.playerId } },
        update: {
          teamId: teamIdBdl,
          minutes,
          points: stat.points,
          assists: stat.assists,
          rebounds: stat.rebounds,
          steals: stat.steals,
          blocks: stat.blocks,
          turnovers: stat.turnovers,
          fgm: stat.fgm,
          fga: stat.fga,
          fg3m: stat.fg3m,
          fg3a: stat.fg3a,
          ftm: stat.ftm,
          fta: stat.fta,
          oreb: stat.oreb,
          dreb: stat.dreb,
          pf: stat.pf,
          plusMinus: stat.plusMinus,
        },
        create: {
          gameId: game.id,
          playerId: stat.playerId,
          teamId: teamIdBdl,
          minutes,
          points: stat.points,
          assists: stat.assists,
          rebounds: stat.rebounds,
          steals: stat.steals,
          blocks: stat.blocks,
          turnovers: stat.turnovers,
          fgm: stat.fgm,
          fga: stat.fga,
          fg3m: stat.fg3m,
          fg3a: stat.fg3a,
          ftm: stat.ftm,
          fta: stat.fta,
          oreb: stat.oreb,
          dreb: stat.dreb,
          pf: stat.pf,
          plusMinus: stat.plusMinus,
        },
      });

      totalStats++;
    }

    const homeLabel = game.homeTeamNameSnapshot ?? `Team ${homeTeamBdl}`;
    const awayLabel = game.awayTeamNameSnapshot ?? `Team ${awayTeamBdl}`;
    console.log(
      `  ${homeLabel} vs ${awayLabel} (${box.gameId}): ${box.playerStats.length} player stats`,
    );
  }

  console.log(`\n  Total: ${totalStats} player stat rows synced`);
  return totalStats;
}

export async function syncNbaStatsForDateRange(startDate: string, endDate: string): Promise<number> {
  console.log(`\nSyncing NBA stats from ${startDate} to ${endDate}...\n`);

  const start = new Date(startDate);
  const end = new Date(endDate);
  let totalStats = 0;
  let currentDate = start;

  while (currentDate <= end) {
    const dateStr = currentDate.toISOString().slice(0, 10);
    
    try {
      const count = await syncNbaStatsForDate(dateStr);
      totalStats += count;
    } catch (err) {
      console.error(`  Error syncing ${dateStr}:`, (err as Error).message);
    }

    currentDate.setDate(currentDate.getDate() + 1);
  }

  console.log(`\n=== Total: ${totalStats} player stat rows synced ===`);
  return totalStats;
}

/** Sync upcoming/scheduled games for a date using nba_api (no BallDontLie). */
export async function syncUpcomingFromNba(date: string): Promise<number> {
  await catchUpUnfinishedGames();

  console.log(`Fetching games for ${date} from nba_api...`);
  const games = await fetchGamesForDate(date);
  console.log(`  Found ${games.length} games`);

  let upserted = 0;
  let updated = 0;
  for (const g of games) {
    const homeTeamBdl = NBA_TO_BDL_TEAM[g.homeTeamId] ?? g.homeTeamId;
    const awayTeamBdl = NBA_TO_BDL_TEAM[g.awayTeamId] ?? g.awayTeamId;
    if (!homeTeamBdl || !awayTeamBdl) continue;

    const gameDate = dateStringToUtcMidday(g.date);
    const existing = await prisma.game.findFirst({
      where: {
        date: gameDate,
        OR: [
          { homeTeamId: homeTeamBdl, awayTeamId: awayTeamBdl },
          { homeTeamId: awayTeamBdl, awayTeamId: homeTeamBdl },
        ],
      },
    });
    if (existing) {
      const statusIsNotFinal = !existing.status?.trim().includes("Final");
      const newHasScores = (g.homeScore ?? 0) > 0 || (g.awayScore ?? 0) > 0;
      if (statusIsNotFinal && newHasScores) {
        await prisma.game.update({
          where: { id: existing.id },
          data: {
            homeScore: g.homeScore ?? 0,
            awayScore: g.awayScore ?? 0,
            status: g.status ?? "Final",
          },
        });
        updated++;
      }
      continue;
    }

    const nbaGameIdNum = parseInt(g.gameId, 10) || Math.abs(hashString(g.gameId));
    const game = await prisma.game.create({
      data: {
        sourceGameId: nbaGameIdNum,
        date: gameDate,
        season: getSeasonFromDate(g.date),
        stage: 2,
        league: "standard",
        homeTeamId: homeTeamBdl,
        awayTeamId: awayTeamBdl,
        homeScore: g.homeScore ?? 0,
        awayScore: g.awayScore ?? 0,
        status: g.status ?? "Scheduled",
        tipoffTimeUtc: gameDate,
      },
    });
    await prisma.gameExternalId.create({
      data: { gameId: game.id, source: "nba_stats", sourceId: g.gameId },
    });
    upserted++;
  }
  console.log(`  Upserted ${upserted} new games, updated ${updated} existing games`);
  return upserted + updated;
}

async function catchUpUnfinishedGames(): Promise<void> {
  const threeDaysAgo = new Date(Date.now() - 3 * 86_400_000);
  const unfinished = await prisma.game.findMany({
    where: {
      date: { gte: threeDaysAgo },
      NOT: { status: { contains: "Final" } },
    },
    include: { homeTeam: true, awayTeam: true },
  });
  if (unfinished.length === 0) return;

  console.log(`Catching up ${unfinished.length} unfinished games from recent days...`);
  const dateSet = new Set(unfinished.map((g) => g.date.toISOString().slice(0, 10)));
  let caught = 0;

  for (const dateStr of dateSet) {
    const apiGames = await fetchGamesForDate(dateStr);
    for (const ag of apiGames) {
      if ((ag.homeScore ?? 0) === 0 && (ag.awayScore ?? 0) === 0) continue;
      const homeTeamBdl = NBA_TO_BDL_TEAM[ag.homeTeamId] ?? ag.homeTeamId;
      const awayTeamBdl = NBA_TO_BDL_TEAM[ag.awayTeamId] ?? ag.awayTeamId;
      const gameDate = dateStringToUtcMidday(ag.date);
      const match = await prisma.game.findFirst({
        where: {
          date: gameDate,
          NOT: { status: { contains: "Final" } },
          OR: [
            { homeTeamId: homeTeamBdl, awayTeamId: awayTeamBdl },
            { homeTeamId: awayTeamBdl, awayTeamId: homeTeamBdl },
          ],
        },
      });
      if (match) {
        await prisma.game.update({
          where: { id: match.id },
          data: {
            homeScore: ag.homeScore ?? 0,
            awayScore: ag.awayScore ?? 0,
            status: ag.status ?? "Final",
          },
        });
        caught++;
      }
    }
  }
  if (caught > 0) console.log(`  Updated ${caught} previously-unfinished games to final`);
}
