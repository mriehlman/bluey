import { prisma } from "../db/prisma.js";
import { fetchBoxScoresForDate, fetchGamesForDate } from "../api/nbaStats.js";

function parseMinutes(min: string): number {
  if (!min || min === "" || min === "0" || min === "0:00") return 0;
  const parts = min.split(":");
  if (parts.length === 2) return Number(parts[0]) * 60 + Number(parts[1]);
  const n = Number(min);
  return isNaN(n) ? 0 : Math.round(n * 60);
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

    // Find the game in our DB by matching teams and date
    const gameDate = new Date(box.date + "T00:00:00Z");
    
    const game = await prisma.game.findFirst({
      where: {
        date: gameDate,
        OR: [
          { homeTeamId: homeTeamBdl, awayTeamId: awayTeamBdl },
          { homeTeamId: awayTeamBdl, awayTeamId: homeTeamBdl },
        ],
      },
    });

    if (!game) {
      console.log(`  Game not found in DB for teams ${homeTeamBdl} vs ${awayTeamBdl} on ${date}`);
      continue;
    }

    // Upsert player stats
    for (const stat of box.playerStats) {
      // Convert NBA team ID to balldontlie team ID
      const teamIdBdl = NBA_TO_BDL_TEAM[stat.teamId] ?? stat.teamId;
      
      // Ensure player exists
      await prisma.player.upsert({
        where: { id: stat.playerId },
        update: { firstname: stat.playerName.split(" ")[0], lastname: stat.playerName.split(" ").slice(1).join(" ") },
        create: { id: stat.playerId, firstname: stat.playerName.split(" ")[0], lastname: stat.playerName.split(" ").slice(1).join(" ") },
      });

      const minutes = parseMinutes(stat.minutes);

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

    console.log(`  ${game.homeTeamNameSnapshot} vs ${game.awayTeamNameSnapshot}: ${box.playerStats.length} player stats`);
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
