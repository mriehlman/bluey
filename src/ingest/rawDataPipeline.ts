/**
 * Raw Data Pipeline
 * 
 * Two-stage data ingestion:
 * 1. fetch:season - Download raw JSON from NBA API, save locally
 * 2. process:season - Read local JSON, write to database
 * 
 * This allows re-processing without re-fetching from APIs.
 */

import { spawn } from "child_process";
import { join } from "path";
import { readdir, readFile } from "fs/promises";
import { existsSync } from "fs";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();
const SCRIPT_PATH = join(process.cwd(), "scripts", "nba_fetch.py");
const DATA_DIR = join(process.cwd(), "data", "raw", "seasons");

// Python executable path - use full path on Windows to avoid PATH issues
const PYTHON_PATH = process.platform === "win32" 
  ? "C:\\Users\\Michael Riehlman\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
  : "python3";

// NBA.com team info - will use these IDs directly in our database
// Format: NBA team ID -> [dbId, name, code, city]
const NBA_TEAMS: Record<number, [number, string, string, string]> = {
  1610612737: [1, "Atlanta Hawks", "ATL", "Atlanta"],
  1610612738: [2, "Boston Celtics", "BOS", "Boston"],
  1610612751: [3, "Brooklyn Nets", "BKN", "Brooklyn"],
  1610612766: [4, "Charlotte Hornets", "CHA", "Charlotte"],
  1610612741: [5, "Chicago Bulls", "CHI", "Chicago"],
  1610612739: [6, "Cleveland Cavaliers", "CLE", "Cleveland"],
  1610612742: [7, "Dallas Mavericks", "DAL", "Dallas"],
  1610612743: [8, "Denver Nuggets", "DEN", "Denver"],
  1610612765: [9, "Detroit Pistons", "DET", "Detroit"],
  1610612744: [10, "Golden State Warriors", "GSW", "Golden State"],
  1610612745: [11, "Houston Rockets", "HOU", "Houston"],
  1610612754: [12, "Indiana Pacers", "IND", "Indiana"],
  1610612746: [13, "LA Clippers", "LAC", "Los Angeles"],
  1610612747: [14, "Los Angeles Lakers", "LAL", "Los Angeles"],
  1610612763: [15, "Memphis Grizzlies", "MEM", "Memphis"],
  1610612748: [16, "Miami Heat", "MIA", "Miami"],
  1610612749: [17, "Milwaukee Bucks", "MIL", "Milwaukee"],
  1610612750: [18, "Minnesota Timberwolves", "MIN", "Minnesota"],
  1610612740: [19, "New Orleans Pelicans", "NOP", "New Orleans"],
  1610612752: [20, "New York Knicks", "NYK", "New York"],
  1610612760: [21, "Oklahoma City Thunder", "OKC", "Oklahoma City"],
  1610612753: [22, "Orlando Magic", "ORL", "Orlando"],
  1610612755: [23, "Philadelphia 76ers", "PHI", "Philadelphia"],
  1610612756: [24, "Phoenix Suns", "PHX", "Phoenix"],
  1610612757: [25, "Portland Trail Blazers", "POR", "Portland"],
  1610612758: [26, "Sacramento Kings", "SAC", "Sacramento"],
  1610612759: [27, "San Antonio Spurs", "SAS", "San Antonio"],
  1610612761: [28, "Toronto Raptors", "TOR", "Toronto"],
  1610612762: [29, "Utah Jazz", "UTA", "Utah"],
  1610612764: [30, "Washington Wizards", "WAS", "Washington"],
};

// Map NBA team ID to our standard database ID (1-30)
function nbaTeamToDbId(nbaTeamId: number): number | null {
  const team = NBA_TEAMS[nbaTeamId];
  return team ? team[0] : null;
}

interface RawBoxScore {
  gameId: string;
  date: string;
  homeTeamId: number;
  awayTeamId: number;
  homeScore: number;
  awayScore: number;
  status: string;
  gameStatus?: number;
  period: number;
  regulationPeriods?: number;
  season?: string;
  gameCode?: string;
  gameTimeUTC?: string;
  isNeutral?: boolean;
  seriesText?: string;
  poRoundDesc?: string;
  homeWins?: number;
  homeLosses?: number;
  awayWins?: number;
  awayLosses?: number;
  homeTricode?: string;
  awayTricode?: string;
  broadcasts?: Array<Record<string, unknown>>;
  gameLeaders?: Array<Record<string, unknown>>;
  seasonLeaders?: Array<Record<string, unknown>>;
  playerStats: RawPlayerStat[];
  teamTotals?: Array<Record<string, unknown>>;
  teamStartersBench?: Array<Record<string, unknown>>;
}

interface RawPlayerStat {
  playerId?: number;
  personId?: number;
  firstName?: string;
  familyName?: string;
  nameI?: string;
  playerSlug?: string;
  teamId: number;
  teamTricode?: string;
  position?: string;
  starter?: boolean;
  comment?: string;
  jerseyNum?: string;
  minutes?: string;
  minutesPlayed?: number;
  points?: number;
  rebounds?: number;
  reboundsTotal?: number;
  assists?: number;
  steals?: number;
  blocks?: number;
  turnovers?: number;
  fgm?: number;
  fga?: number;
  fgPct?: number;
  fieldGoalsMade?: number;
  fieldGoalsAttempted?: number;
  fieldGoalsPercentage?: number;
  fg3m?: number;
  fg3a?: number;
  fg3Pct?: number;
  threePointersMade?: number;
  threePointersAttempted?: number;
  threePointersPercentage?: number;
  ftm?: number;
  fta?: number;
  ftPct?: number;
  freeThrowsMade?: number;
  freeThrowsAttempted?: number;
  freeThrowsPercentage?: number;
  oreb?: number;
  dreb?: number;
  reboundsOffensive?: number;
  reboundsDefensive?: number;
  pf?: number;
  foulsPersonal?: number;
  plusMinus?: number;
  plusMinusPoints?: number;
}

/**
 * Stage 1: Fetch season data from NBA API and save as local JSON files
 */
export async function fetchSeasonToJson(
  season: string,
  startDate?: string,
  endDate?: string
): Promise<void> {
  console.log(`\n=== Fetching Season ${season} to Local JSON ===\n`);
  
  const args = ["backfill-season", season];
  if (startDate) args.push(startDate);
  if (endDate) args.push(endDate);
  
  return new Promise((resolve, reject) => {
    const proc = spawn(PYTHON_PATH, [SCRIPT_PATH, ...args], {
      stdio: ["pipe", "pipe", "inherit"], // stderr goes to console
    });
    
    let stdout = "";
    proc.stdout.on("data", (data) => {
      stdout += data.toString();
    });
    
    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}`));
        return;
      }
      
      try {
        const result = JSON.parse(stdout);
        console.log(`\nFetch complete: ${result.gamesProcessed} games saved`);
        resolve();
      } catch (e) {
        console.log("\nFetch complete");
        resolve();
      }
    });
    
    proc.on("error", reject);
  });
}

/**
 * Ensure all NBA teams exist in the database with consistent IDs 1-30
 */
async function ensureTeamsExist(): Promise<void> {
  console.log("Ensuring all NBA teams exist in database...");
  
  for (const [nbaId, [dbId, name, code, city]] of Object.entries(NBA_TEAMS)) {
    await prisma.team.upsert({
      where: { id: dbId },
      create: { id: dbId, name, code, city },
      update: { name, code, city },
    });
  }
  
  console.log("All 30 NBA teams ready.\n");
}

/**
 * Stage 2: Process local JSON files into database
 */
export async function processSeasonFromJson(season: string): Promise<void> {
  const seasonDir = join(DATA_DIR, season);
  
  if (!existsSync(seasonDir)) {
    console.error(`Season directory not found: ${seasonDir}`);
    console.error(`Run "fetch:season --season ${season}" first`);
    process.exit(1);
  }
  
  console.log(`\n=== Processing Season ${season} from Local JSON ===\n`);
  
  // Ensure teams exist first
  await ensureTeamsExist();
  
  // Read all JSON files in the season directory
  const files = await readdir(seasonDir);
  const jsonFiles = files.filter(f => f.endsWith(".json") && !f.startsWith("_"));
  
  console.log(`Found ${jsonFiles.length} game files to process\n`);
  
  let gamesProcessed = 0;
  let playersUpserted = 0;
  let statsUpserted = 0;
  let errors = 0;
  
  // Parse season year for DB
  const seasonYear = parseInt(season.split("-")[0]);
  
  for (const file of jsonFiles) {
    try {
      const filepath = join(seasonDir, file);
      const content = await readFile(filepath, "utf-8");
      const boxScore: RawBoxScore = JSON.parse(content);
      
      const result = await processBoxScore(boxScore, seasonYear);
      
      if (result.gameId) {
        gamesProcessed++;
        playersUpserted += result.playersUpserted;
        statsUpserted += result.statsUpserted;
      }
      
      if (gamesProcessed % 50 === 0) {
        console.log(`Progress: ${gamesProcessed}/${jsonFiles.length} games, ${statsUpserted} stats`);
      }
      
    } catch (err) {
      errors++;
      console.error(`Error processing ${file}:`, (err as Error).message);
    }
  }
  
  console.log(`\n=== Season ${season} Processing Complete ===`);
  console.log(`Games: ${gamesProcessed}`);
  console.log(`Players: ${playersUpserted}`);
  console.log(`Stats: ${statsUpserted}`);
  if (errors > 0) console.log(`Errors: ${errors}`);
}

function parseMinutes(raw: string | undefined): number {
  if (!raw) return 0;
  const s = String(raw);
  
  // Handle ISO duration format PT##M##.##S
  if (s.startsWith("PT")) {
    try {
      let mins = 0, secs = 0;
      let part = s.slice(2);
      if (part.includes("M")) {
        const [m, rest] = part.split("M");
        mins = parseInt(m) || 0;
        part = rest;
      }
      if (part.includes("S")) {
        secs = parseFloat(part.replace("S", "")) || 0;
      }
      return mins + Math.floor(secs / 60);
    } catch {
      return 0;
    }
  }
  
  // Handle MM:SS format
  if (s.includes(":")) {
    return parseInt(s.split(":")[0]) || 0;
  }
  
  return parseInt(s) || 0;
}

async function processBoxScore(
  boxScore: RawBoxScore,
  seasonYear: number
): Promise<{ gameId: string | null; playersUpserted: number; statsUpserted: number }> {
  const result = { gameId: null as string | null, playersUpserted: 0, statsUpserted: 0 };
  
  // Convert NBA team IDs to our database IDs
  const homeTeamId = nbaTeamToDbId(boxScore.homeTeamId);
  const awayTeamId = nbaTeamToDbId(boxScore.awayTeamId);
  
  if (!homeTeamId || !awayTeamId) {
    console.warn(`Unknown team IDs: home=${boxScore.homeTeamId}, away=${boxScore.awayTeamId}`);
    return result;
  }
  
  // Find or create the game
  const gameDate = new Date(boxScore.date);
  
  // Parse tipoff time if available
  let tipoffTimeUtc: Date | null = null;
  if (boxScore.gameTimeUTC) {
    try {
      tipoffTimeUtc = new Date(boxScore.gameTimeUTC);
    } catch {}
  }
  
  // Look for existing game by teams and date
  let game = await prisma.game.findFirst({
    where: {
      homeTeamId,
      awayTeamId,
      date: gameDate,
    },
  });
  
  // Build game data with all available fields
  const gameData = {
    nbaGameId: boxScore.gameId,
    homeScore: boxScore.homeScore,
    awayScore: boxScore.awayScore,
    periods: boxScore.period,
    status: boxScore.status,
    gameCode: boxScore.gameCode || null,
    tipoffTimeUtc,
    isNeutralSite: boxScore.isNeutral || false,
    seriesText: boxScore.seriesText || null,
    poRoundDesc: boxScore.poRoundDesc || null,
    homeWinsPreGame: boxScore.homeWins ?? null,
    homeLossesPreGame: boxScore.homeLosses ?? null,
    awayWinsPreGame: boxScore.awayWins ?? null,
    awayLossesPreGame: boxScore.awayLosses ?? null,
    broadcasts: boxScore.broadcasts ? JSON.stringify(boxScore.broadcasts) : null,
  };
  
  if (!game) {
    // Create the game - use NBA game ID as a pseudo sourceGameId
    const sourceId = parseInt(boxScore.gameId.replace(/^00/, "")) || Math.floor(Math.random() * 1000000000);
    
    game = await prisma.game.upsert({
      where: { sourceGameId: sourceId },
      create: {
        sourceGameId: sourceId,
        date: gameDate,
        season: seasonYear,
        stage: 2, // Regular season
        league: "NBA",
        homeTeamId,
        awayTeamId,
        ...gameData,
      },
      update: gameData,
    });
  } else {
    await prisma.game.update({
      where: { id: game.id },
      data: gameData,
    });
  }
  
  result.gameId = game.id;
  
  // Process player stats
  for (const stat of boxScore.playerStats) {
    // Convert NBA team ID to database ID
    const playerTeamId = nbaTeamToDbId(stat.teamId);
    if (!playerTeamId) continue;
    
    // Handle both old and new field names
    const playerId = stat.playerId ?? stat.personId;
    if (!playerId) continue;
    
    const firstName = stat.firstName ?? "";
    const lastName = stat.familyName ?? "";
    const jerseyNum = stat.jerseyNum ?? null;
    const playerSlug = stat.playerSlug ?? null;
    
    // Upsert player with all available info
    await prisma.player.upsert({
      where: { id: playerId },
      create: {
        id: playerId,
        firstname: firstName || null,
        lastname: lastName || null,
        jerseyNum,
        slug: playerSlug,
      },
      update: {
        firstname: firstName || null,
        lastname: lastName || null,
        jerseyNum,
        slug: playerSlug,
      },
    });
    result.playersUpserted++;
    
    // Extract stats with fallbacks for different field names
    const minutes = stat.minutesPlayed ?? parseMinutes(stat.minutes);
    const minutesRaw = stat.minutes ?? null;
    const points = stat.points ?? 0;
    const rebounds = stat.rebounds ?? stat.reboundsTotal ?? 0;
    const assists = stat.assists ?? 0;
    const steals = stat.steals ?? 0;
    const blocks = stat.blocks ?? 0;
    const turnovers = stat.turnovers ?? 0;
    const fgm = stat.fgm ?? stat.fieldGoalsMade ?? null;
    const fga = stat.fga ?? stat.fieldGoalsAttempted ?? null;
    const fgPct = stat.fgPct ?? stat.fieldGoalsPercentage ?? null;
    const fg3m = stat.fg3m ?? stat.threePointersMade ?? null;
    const fg3a = stat.fg3a ?? stat.threePointersAttempted ?? null;
    const fg3Pct = stat.fg3Pct ?? stat.threePointersPercentage ?? null;
    const ftm = stat.ftm ?? stat.freeThrowsMade ?? null;
    const fta = stat.fta ?? stat.freeThrowsAttempted ?? null;
    const ftPct = stat.ftPct ?? stat.freeThrowsPercentage ?? null;
    const oreb = stat.oreb ?? stat.reboundsOffensive ?? null;
    const dreb = stat.dreb ?? stat.reboundsDefensive ?? null;
    const pf = stat.pf ?? stat.foulsPersonal ?? null;
    const plusMinus = stat.plusMinus ?? stat.plusMinusPoints ?? null;
    const position = stat.position || null;
    const starter = stat.starter ?? null;
    const comment = stat.comment || null;
    
    // Upsert player game stat with all fields
    await prisma.playerGameStat.upsert({
      where: {
        gameId_playerId: {
          gameId: game.id,
          playerId,
        },
      },
      create: {
        gameId: game.id,
        playerId,
        teamId: playerTeamId,
        minutes,
        minutesRaw,
        points,
        assists,
        rebounds,
        steals,
        blocks,
        turnovers,
        fgm,
        fga,
        fgPct,
        fg3m,
        fg3a,
        fg3Pct,
        ftm,
        fta,
        ftPct,
        oreb,
        dreb,
        pf,
        plusMinus,
        position,
        starter,
        comment,
      },
      update: {
        minutes,
        minutesRaw,
        points,
        assists,
        rebounds,
        steals,
        blocks,
        turnovers,
        fgm,
        fga,
        fgPct,
        fg3m,
        fg3a,
        fg3Pct,
        ftm,
        fta,
        ftPct,
        oreb,
        dreb,
        pf,
        plusMinus,
        position,
        starter,
        comment,
      },
    });
    result.statsUpserted++;
  }
  
  return result;
}
