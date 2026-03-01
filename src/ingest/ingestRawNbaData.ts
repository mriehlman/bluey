/**
 * Ingest raw NBA JSON files from data/raw/seasons/ into PostgreSQL.
 * 
 * This reads from local files (not API) for bulk historical imports.
 * Designed for efficiency with batch upserts and progress tracking.
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

// Parse season string to start year (e.g., "2024-25" -> 2024)
function parseSeasonYear(season: string): number {
  return parseInt(season.split("-")[0], 10);
}

// Parse minutes string to integer seconds
function parseMinutesToSeconds(min: string | null | undefined): number {
  if (!min || min === "" || min === "0" || min === "0:00") return 0;

  // Handle "MM:SS" format - but validate the format first
  if (min.includes(":")) {
    // Only take first colon-separated part (handles malformed like "-1:0-57")
    const match = min.match(/^(\d+):(\d+)/);
    if (match) {
      return Number(match[1]) * 60 + Number(match[2]);
    }
    // Invalid format, return 0
    return 0;
  }

  const n = Number(min);
  return isNaN(n) || n < 0 ? 0 : Math.round(n * 60);
}

// Parse minutes string to integer minutes (for DB storage)
function parseMinutes(min: string | null | undefined): number {
  const seconds = parseMinutesToSeconds(min);
  const result = Math.floor(seconds / 60);
  // Ensure we never return NaN
  return isNaN(result) || result < 0 ? 0 : result;
}

// Determine season type from game ID
function getSeasonType(gameId: string): string {
  const prefix = gameId.substring(0, 3);
  switch (prefix) {
    case "001": return "PRE"; // Preseason
    case "002": return "REG"; // Regular season
    case "003": return "ASG"; // All-Star
    case "004": return "POST"; // Playoffs
    case "005": return "PIT"; // Play-in tournament
    default: return "REG";
  }
}

interface RawGameFile {
  gameId: string;
  date: string;
  season: string;
  homeTeamId: number;
  awayTeamId: number;
  homeScore: number;
  awayScore: number;
  homeTricode: string;
  awayTricode: string;
  homeWins?: number;
  homeLosses?: number;
  awayWins?: number;
  awayLosses?: number;
  status?: string;
  gameStatus?: number;
  period?: number;
  regulationPeriods?: number;
  gameCode?: string;
  gameTimeUTC?: string;
  isNeutral?: boolean;
  seriesText?: string;
  poRoundDesc?: string;
  broadcasts?: any[];
  playerStats: RawPlayerStat[];
  teamTotals?: any[];
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

interface IngestProgress {
  season: string;
  gamesIngested: string[];
  lastUpdated: string;
}

function getProgressFilePath(season: string): string {
  return path.join(process.cwd(), "data", "raw", "seasons", season, "_ingest_progress.json");
}

function loadProgress(season: string): IngestProgress {
  const filePath = getProgressFilePath(season);
  if (fs.existsSync(filePath)) {
    const data = JSON.parse(fs.readFileSync(filePath, "utf-8"));
    return data;
  }
  return { season, gamesIngested: [], lastUpdated: new Date().toISOString() };
}

function saveProgress(progress: IngestProgress): void {
  const filePath = getProgressFilePath(progress.season);
  progress.lastUpdated = new Date().toISOString();
  fs.writeFileSync(filePath, JSON.stringify(progress, null, 2));
}

/**
 * Ensure all 30 NBA teams exist in the database.
 */
export async function ensureTeams(): Promise<void> {
  const teams = [
    { id: 1, name: "Hawks", code: "ATL", city: "Atlanta" },
    { id: 2, name: "Celtics", code: "BOS", city: "Boston" },
    { id: 3, name: "Nets", code: "BKN", city: "Brooklyn" },
    { id: 4, name: "Hornets", code: "CHA", city: "Charlotte" },
    { id: 5, name: "Bulls", code: "CHI", city: "Chicago" },
    { id: 6, name: "Cavaliers", code: "CLE", city: "Cleveland" },
    { id: 7, name: "Mavericks", code: "DAL", city: "Dallas" },
    { id: 8, name: "Nuggets", code: "DEN", city: "Denver" },
    { id: 9, name: "Pistons", code: "DET", city: "Detroit" },
    { id: 10, name: "Warriors", code: "GSW", city: "Golden State" },
    { id: 11, name: "Rockets", code: "HOU", city: "Houston" },
    { id: 12, name: "Pacers", code: "IND", city: "Indiana" },
    { id: 13, name: "Clippers", code: "LAC", city: "Los Angeles" },
    { id: 14, name: "Lakers", code: "LAL", city: "Los Angeles" },
    { id: 15, name: "Grizzlies", code: "MEM", city: "Memphis" },
    { id: 16, name: "Heat", code: "MIA", city: "Miami" },
    { id: 17, name: "Bucks", code: "MIL", city: "Milwaukee" },
    { id: 18, name: "Timberwolves", code: "MIN", city: "Minnesota" },
    { id: 19, name: "Pelicans", code: "NOP", city: "New Orleans" },
    { id: 20, name: "Knicks", code: "NYK", city: "New York" },
    { id: 21, name: "Thunder", code: "OKC", city: "Oklahoma City" },
    { id: 22, name: "Magic", code: "ORL", city: "Orlando" },
    { id: 23, name: "76ers", code: "PHI", city: "Philadelphia" },
    { id: 24, name: "Suns", code: "PHX", city: "Phoenix" },
    { id: 25, name: "Trail Blazers", code: "POR", city: "Portland" },
    { id: 26, name: "Kings", code: "SAC", city: "Sacramento" },
    { id: 27, name: "Spurs", code: "SAS", city: "San Antonio" },
    { id: 28, name: "Raptors", code: "TOR", city: "Toronto" },
    { id: 29, name: "Jazz", code: "UTA", city: "Utah" },
    { id: 30, name: "Wizards", code: "WAS", city: "Washington" },
  ];

  for (const team of teams) {
    await prisma.team.upsert({
      where: { id: team.id },
      update: { name: team.name, code: team.code, city: team.city },
      create: team,
    });
  }
  console.log("✓ Ensured all 30 teams exist");
}

/**
 * Ingest a single game file into the database.
 */
async function ingestGameFile(filePath: string, season: string): Promise<{ gameId: string; statsCount: number } | null> {
  const content = fs.readFileSync(filePath, "utf-8");
  const data: RawGameFile = JSON.parse(content);

  if (!data.gameId || !data.date) {
    console.warn(`  Skipping ${filePath}: missing gameId or date`);
    return null;
  }

  const homeTeamId = NBA_TO_INTERNAL_TEAM[data.homeTeamId];
  const awayTeamId = NBA_TO_INTERNAL_TEAM[data.awayTeamId];

  if (!homeTeamId || !awayTeamId) {
    console.warn(`  Skipping ${data.gameId}: unknown team IDs (${data.homeTeamId}, ${data.awayTeamId})`);
    return null;
  }

  const seasonYear = parseSeasonYear(season);
  const seasonType = getSeasonType(data.gameId);
  const gameDate = new Date(data.date + "T00:00:00Z");

  // Generate a stable sourceGameId from the NBA game ID
  const sourceGameId = parseInt(data.gameId, 10);

  // Upsert the game
  const game = await prisma.game.upsert({
    where: { nbaGameId: data.gameId },
    update: {
      homeScore: data.homeScore ?? 0,
      awayScore: data.awayScore ?? 0,
      status: data.status ?? "Final",
      periods: data.period ?? 4,
      homeWinsPreGame: data.homeWins,
      homeLossesPreGame: data.homeLosses,
      awayWinsPreGame: data.awayWins,
      awayLossesPreGame: data.awayLosses,
      isNeutralSite: data.isNeutral ?? false,
      seriesText: data.seriesText,
      poRoundDesc: data.poRoundDesc,
      broadcasts: data.broadcasts,
      tipoffTimeUtc: data.gameTimeUTC ? new Date(data.gameTimeUTC) : null,
    },
    create: {
      sourceGameId,
      nbaGameId: data.gameId,
      date: gameDate,
      season: seasonYear,
      stage: seasonType === "POST" ? 2 : 1,
      league: "NBA",
      homeTeamId,
      awayTeamId,
      homeScore: data.homeScore ?? 0,
      awayScore: data.awayScore ?? 0,
      status: data.status ?? "Final",
      periods: data.period ?? 4,
      gameCode: data.gameCode,
      seasonType,
      homeTeamNameSnapshot: data.homeTricode,
      awayTeamNameSnapshot: data.awayTricode,
      homeWinsPreGame: data.homeWins,
      homeLossesPreGame: data.homeLosses,
      awayWinsPreGame: data.awayWins,
      awayLossesPreGame: data.awayLosses,
      isNeutralSite: data.isNeutral ?? false,
      seriesText: data.seriesText,
      poRoundDesc: data.poRoundDesc,
      broadcasts: data.broadcasts,
      tipoffTimeUtc: data.gameTimeUTC ? new Date(data.gameTimeUTC) : null,
    },
  });

  // Batch player upserts - collect all players first
  const playerStats = data.playerStats || [];
  const validStats = playerStats.filter(stat => NBA_TO_INTERNAL_TEAM[stat.teamId]);
  
  // Batch upsert all players in parallel (5 at a time to avoid connection limits)
  const BATCH_SIZE = 10;
  for (let i = 0; i < validStats.length; i += BATCH_SIZE) {
    const batch = validStats.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(stat => 
      prisma.player.upsert({
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
      })
    ));
  }

  // Batch upsert all player stats in parallel
  for (let i = 0; i < validStats.length; i += BATCH_SIZE) {
    const batch = validStats.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(stat => {
      const playerTeamId = NBA_TO_INTERNAL_TEAM[stat.teamId];
      return prisma.playerGameStat.upsert({
        where: { gameId_playerId: { gameId: game.id, playerId: stat.personId } },
        update: {
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
        create: {
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
    }));
  }

  return { gameId: data.gameId, statsCount: validStats.length };
}

/**
 * Ingest all games for a season from raw JSON files.
 */
export async function ingestSeason(season: string, options: { force?: boolean } = {}): Promise<{
  gamesIngested: number;
  statsIngested: number;
  skipped: number;
  errors: number;
}> {
  const seasonDir = path.join(process.cwd(), "data", "raw", "seasons", season);
  
  if (!fs.existsSync(seasonDir)) {
    throw new Error(`Season directory not found: ${seasonDir}`);
  }

  console.log(`\n=== Ingesting season ${season} ===`);

  // Load progress
  const progress = options.force ? { season, gamesIngested: [], lastUpdated: new Date().toISOString() } : loadProgress(season);
  const alreadyIngested = new Set(progress.gamesIngested);

  // Ensure teams exist
  await ensureTeams();

  // Find all game files (exclude scoreboards and progress files)
  const files = fs.readdirSync(seasonDir).filter(f => 
    f.endsWith(".json") && 
    !f.startsWith("_")
  );

  console.log(`Found ${files.length} game files, ${alreadyIngested.size} already ingested`);

  let gamesIngested = 0;
  let statsIngested = 0;
  let skipped = 0;
  let errors = 0;

  // Filter to games that need processing
  const gamesToProcess = files
    .map(file => ({
      file,
      gameId: file.replace(".json", ""),
      filePath: path.join(seasonDir, file),
    }))
    .filter(({ gameId }) => {
      if (alreadyIngested.has(gameId)) {
        skipped++;
        return false;
      }
      return true;
    });

  // Process games in parallel batches
  const GAME_BATCH_SIZE = 5;
  for (let i = 0; i < gamesToProcess.length; i += GAME_BATCH_SIZE) {
    const batch = gamesToProcess.slice(i, i + GAME_BATCH_SIZE);
    
    const results = await Promise.allSettled(
      batch.map(({ filePath, file }) => 
        ingestGameFile(filePath, season).catch(err => {
          console.error(`  Error ingesting ${file}:`, (err as Error).message);
          throw err;
        })
      )
    );

    for (const result of results) {
      if (result.status === "fulfilled" && result.value) {
        gamesIngested++;
        statsIngested += result.value.statsCount;
        progress.gamesIngested.push(result.value.gameId);
      } else if (result.status === "rejected") {
        errors++;
      }
    }

    // Save progress every batch
    if (gamesIngested > 0 && gamesIngested % 25 === 0) {
      saveProgress(progress);
      const total = gamesIngested + skipped + errors;
      const pct = Math.round((total / files.length) * 100);
      console.log(`  Progress: ${total}/${files.length} (${pct}%) - ${gamesIngested} new, ${skipped} skipped, ${errors} errors`);
    }
  }

  // Final progress save
  saveProgress(progress);

  console.log(`\n=== Season ${season} complete ===`);
  console.log(`  Games ingested: ${gamesIngested}`);
  console.log(`  Player stats: ${statsIngested}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);

  return { gamesIngested, statsIngested, skipped, errors };
}

/**
 * Ingest all available seasons.
 */
export async function ingestAllSeasons(options: { force?: boolean } = {}): Promise<void> {
  const seasonsDir = path.join(process.cwd(), "data", "raw", "seasons");
  
  if (!fs.existsSync(seasonsDir)) {
    throw new Error(`Seasons directory not found: ${seasonsDir}`);
  }

  const seasons = fs.readdirSync(seasonsDir).filter(d => 
    fs.statSync(path.join(seasonsDir, d)).isDirectory()
  ).sort();

  console.log(`Found ${seasons.length} seasons: ${seasons.join(", ")}`);

  let totalGames = 0;
  let totalStats = 0;

  for (const season of seasons) {
    const result = await ingestSeason(season, options);
    totalGames += result.gamesIngested;
    totalStats += result.statsIngested;
  }

  console.log(`\n=== All seasons complete ===`);
  console.log(`  Total games: ${totalGames}`);
  console.log(`  Total player stats: ${totalStats}`);
}

/**
 * Get ingestion status for all seasons.
 */
export async function getIngestionStatus(): Promise<Record<string, { 
  rawGames: number; 
  ingestedGames: number; 
  dbGames: number;
  percentComplete: number;
}>> {
  const seasonsDir = path.join(process.cwd(), "data", "raw", "seasons");
  const status: Record<string, any> = {};

  if (!fs.existsSync(seasonsDir)) {
    return status;
  }

  const seasons = fs.readdirSync(seasonsDir).filter(d => 
    fs.statSync(path.join(seasonsDir, d)).isDirectory()
  );

  for (const season of seasons) {
    const seasonDir = path.join(seasonsDir, season);
    
    // Count raw game files
    const rawGames = fs.readdirSync(seasonDir).filter(f => 
      f.endsWith(".json") && !f.startsWith("_")
    ).length;

    // Count ingested games from progress
    const progress = loadProgress(season);
    const ingestedGames = progress.gamesIngested.length;

    // Count games in DB for this season
    const seasonYear = parseSeasonYear(season);
    const dbGames = await prisma.game.count({
      where: { season: seasonYear }
    });

    status[season] = {
      rawGames,
      ingestedGames,
      dbGames,
      percentComplete: rawGames > 0 ? Math.round((ingestedGames / rawGames) * 100) : 0,
    };
  }

  return status;
}
