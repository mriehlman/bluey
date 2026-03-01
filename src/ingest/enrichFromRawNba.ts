/**
 * Enrich existing games in the database with detailed stats from raw NBA JSON files.
 * 
 * This matches existing games (from old data source) by date + team IDs and
 * updates them with richer stats from the NBA.com API data.
 */

import { prisma } from "../db/prisma.js";
import * as fs from "fs";
import * as path from "path";

// NBA API team ID to internal team ID mapping
const NBA_TO_INTERNAL_TEAM: Record<number, number> = {
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

// Parse minutes string to integer minutes
function parseMinutes(min: string | null | undefined): number {
  if (!min || min === "" || min === "0" || min === "0:00") return 0;
  if (min.includes(":")) {
    // Only take first valid MM:SS pattern
    const match = min.match(/^(\d+):(\d+)/);
    if (match) return Number(match[1]);
    return 0;
  }
  const n = Number(min);
  return isNaN(n) || n < 0 ? 0 : Math.floor(n);
}

interface RawPlayerStat {
  gameId: string;
  teamId: number;
  personId: number;
  firstName: string;
  familyName: string;
  position?: string;
  minutes?: string;
  points?: number;
  assists?: number;
  reboundsTotal?: number;
  reboundsOffensive?: number;
  reboundsDefensive?: number;
  steals?: number;
  blocks?: number;
  turnovers?: number;
  fieldGoalsMade?: number;
  fieldGoalsAttempted?: number;
  fieldGoalsPercentage?: number;
  threePointersMade?: number;
  threePointersAttempted?: number;
  threePointersPercentage?: number;
  freeThrowsMade?: number;
  freeThrowsAttempted?: number;
  freeThrowsPercentage?: number;
  foulsPersonal?: number;
  plusMinusPoints?: number;
  comment?: string;
  playerSlug?: string;
  jerseyNum?: string;
}

interface RawGameFile {
  gameId: string;
  date: string;
  homeTeamId: number;
  awayTeamId: number;
  homeScore: number;
  awayScore: number;
  homeTricode: string;
  awayTricode: string;
  status?: string;
  period?: number;
  playerStats: RawPlayerStat[];
}

/**
 * Find existing game in DB by date and team matchup.
 */
async function findExistingGame(date: string, homeTeamId: number, awayTeamId: number) {
  const gameDate = new Date(date + "T00:00:00Z");
  
  // Try exact match first
  let game = await prisma.game.findFirst({
    where: {
      date: gameDate,
      homeTeamId,
      awayTeamId,
    },
  });
  
  // If not found, try swapped (in case home/away are flipped)
  if (!game) {
    game = await prisma.game.findFirst({
      where: {
        date: gameDate,
        homeTeamId: awayTeamId,
        awayTeamId: homeTeamId,
      },
    });
  }
  
  return game;
}

/**
 * Enrich a single game with detailed stats from raw JSON.
 */
async function enrichGameFile(filePath: string): Promise<{
  matched: boolean;
  statsUpdated: number;
  playersUpdated: number;
} | null> {
  const content = fs.readFileSync(filePath, "utf-8");
  const data: RawGameFile = JSON.parse(content);

  if (!data.gameId || !data.date || !data.playerStats) {
    return null;
  }

  const homeTeamId = NBA_TO_INTERNAL_TEAM[data.homeTeamId];
  const awayTeamId = NBA_TO_INTERNAL_TEAM[data.awayTeamId];

  if (!homeTeamId || !awayTeamId) {
    return null;
  }

  // Find matching game in DB
  const game = await findExistingGame(data.date, homeTeamId, awayTeamId);
  
  if (!game) {
    return { matched: false, statsUpdated: 0, playersUpdated: 0 };
  }

  // Update game with nbaGameId if not set
  if (!game.nbaGameId) {
    await prisma.game.update({
      where: { id: game.id },
      data: {
        nbaGameId: data.gameId,
        homeTeamNameSnapshot: data.homeTricode,
        awayTeamNameSnapshot: data.awayTricode,
        status: data.status ?? game.status,
        periods: data.period ?? game.periods,
      },
    });
  }

  let statsUpdated = 0;
  let playersUpdated = 0;

  for (const stat of data.playerStats) {
    const playerTeamId = NBA_TO_INTERNAL_TEAM[stat.teamId];
    if (!playerTeamId) continue;

    // Update player with richer info
    const playerUpdate = await prisma.player.upsert({
      where: { id: stat.personId },
      update: {
        firstname: stat.firstName,
        lastname: stat.familyName,
        slug: stat.playerSlug,
        jerseyNum: stat.jerseyNum,
      },
      create: {
        id: stat.personId,
        firstname: stat.firstName,
        lastname: stat.familyName,
        slug: stat.playerSlug,
        jerseyNum: stat.jerseyNum,
      },
    });
    playersUpdated++;

    // Find existing player stat for this game
    const existingStat = await prisma.playerGameStat.findUnique({
      where: { gameId_playerId: { gameId: game.id, playerId: stat.personId } },
    });

    if (existingStat) {
      // Update with enriched data
      await prisma.playerGameStat.update({
        where: { id: existingStat.id },
        data: {
          minutesRaw: stat.minutes,
          fgm: stat.fieldGoalsMade,
          fga: stat.fieldGoalsAttempted,
          fgPct: stat.fieldGoalsPercentage,
          fg3m: stat.threePointersMade,
          fg3a: stat.threePointersAttempted,
          fg3Pct: stat.threePointersPercentage,
          ftm: stat.freeThrowsMade,
          fta: stat.freeThrowsAttempted,
          ftPct: stat.freeThrowsPercentage,
          oreb: stat.reboundsOffensive,
          dreb: stat.reboundsDefensive,
          plusMinus: stat.plusMinusPoints,
          position: stat.position,
          comment: stat.comment,
        },
      });
      statsUpdated++;
    } else {
      // Create new stat if doesn't exist
      await prisma.playerGameStat.create({
        data: {
          gameId: game.id,
          playerId: stat.personId,
          teamId: playerTeamId,
          minutes: parseMinutes(stat.minutes),
          minutesRaw: stat.minutes,
          points: stat.points ?? 0,
          assists: stat.assists ?? 0,
          rebounds: stat.reboundsTotal ?? 0,
          steals: stat.steals ?? 0,
          blocks: stat.blocks ?? 0,
          turnovers: stat.turnovers ?? 0,
          fgm: stat.fieldGoalsMade,
          fga: stat.fieldGoalsAttempted,
          fgPct: stat.fieldGoalsPercentage,
          fg3m: stat.threePointersMade,
          fg3a: stat.threePointersAttempted,
          fg3Pct: stat.threePointersPercentage,
          ftm: stat.freeThrowsMade,
          fta: stat.freeThrowsAttempted,
          ftPct: stat.freeThrowsPercentage,
          oreb: stat.reboundsOffensive,
          dreb: stat.reboundsDefensive,
          pf: stat.foulsPersonal,
          plusMinus: stat.plusMinusPoints,
          position: stat.position,
          comment: stat.comment,
        },
      });
      statsUpdated++;
    }
  }

  return { matched: true, statsUpdated, playersUpdated };
}

/**
 * Enrich all games for a season from raw JSON files.
 */
export async function enrichSeason(season: string): Promise<{
  gamesMatched: number;
  gamesUnmatched: number;
  statsUpdated: number;
}> {
  const seasonDir = path.join(process.cwd(), "data", "raw", "seasons", season);
  
  if (!fs.existsSync(seasonDir)) {
    throw new Error(`Season directory not found: ${seasonDir}`);
  }

  console.log(`\n=== Enriching season ${season} ===`);

  // Find all game files
  const files = fs.readdirSync(seasonDir).filter(f => 
    f.endsWith(".json") && !f.startsWith("_")
  );

  console.log(`Found ${files.length} game files`);

  let gamesMatched = 0;
  let gamesUnmatched = 0;
  let statsUpdated = 0;

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const filePath = path.join(seasonDir, file);

    try {
      const result = await enrichGameFile(filePath);
      
      if (result) {
        if (result.matched) {
          gamesMatched++;
          statsUpdated += result.statsUpdated;
        } else {
          gamesUnmatched++;
        }
      }

      if ((i + 1) % 100 === 0) {
        console.log(`  Progress: ${i + 1}/${files.length} (${gamesMatched} matched, ${gamesUnmatched} unmatched)`);
      }
    } catch (err) {
      console.error(`  Error enriching ${file}:`, (err as Error).message);
    }
  }

  console.log(`\n=== Season ${season} enrichment complete ===`);
  console.log(`  Games matched: ${gamesMatched}`);
  console.log(`  Games unmatched: ${gamesUnmatched}`);
  console.log(`  Stats updated: ${statsUpdated}`);

  return { gamesMatched, gamesUnmatched, statsUpdated };
}

/**
 * Enrich all available seasons.
 */
export async function enrichAllSeasons(): Promise<void> {
  const seasonsDir = path.join(process.cwd(), "data", "raw", "seasons");
  
  if (!fs.existsSync(seasonsDir)) {
    throw new Error(`Seasons directory not found: ${seasonsDir}`);
  }

  const seasons = fs.readdirSync(seasonsDir).filter(d => 
    fs.statSync(path.join(seasonsDir, d)).isDirectory()
  ).sort();

  console.log(`Found ${seasons.length} seasons: ${seasons.join(", ")}`);

  let totalMatched = 0;
  let totalUnmatched = 0;
  let totalStats = 0;

  for (const season of seasons) {
    const result = await enrichSeason(season);
    totalMatched += result.gamesMatched;
    totalUnmatched += result.gamesUnmatched;
    totalStats += result.statsUpdated;
  }

  console.log(`\n=== All seasons enrichment complete ===`);
  console.log(`  Total games matched: ${totalMatched}`);
  console.log(`  Total games unmatched: ${totalUnmatched}`);
  console.log(`  Total stats updated: ${totalStats}`);
}

/**
 * Show enrichment status.
 */
export async function getEnrichmentStatus(): Promise<void> {
  const withDetailedStats = await prisma.playerGameStat.count({
    where: { fgm: { not: null } }
  });
  const withoutDetailedStats = await prisma.playerGameStat.count({
    where: { fgm: null }
  });
  const gamesWithNbaId = await prisma.game.count({
    where: { nbaGameId: { not: null } }
  });
  const gamesWithoutNbaId = await prisma.game.count({
    where: { nbaGameId: null }
  });

  console.log("\n=== Enrichment Status ===");
  console.log(`Games with nbaGameId: ${gamesWithNbaId}`);
  console.log(`Games without nbaGameId: ${gamesWithoutNbaId}`);
  console.log(`Player stats with detailed shooting: ${withDetailedStats}`);
  console.log(`Player stats basic only: ${withoutDetailedStats}`);
  console.log(`Enrichment coverage: ${((withDetailedStats / (withDetailedStats + withoutDetailedStats)) * 100).toFixed(1)}%`);
}
