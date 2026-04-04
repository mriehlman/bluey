/**
 * Sync Player Prop Odds from The Odds API
 * 
 * Requires paid API subscription for player props.
 * Player props are fetched per-event using /events/{eventId}/odds endpoint.
 */

import { prisma } from "@bluey/db";
import { getEasternDateFromUtc, dateStringToUtcMidday } from "./utils";
import * as fs from "fs";
import * as path from "path";
import { getDataDir } from "../config/paths";

function saveRawPlayerProps(date: string, eventId: string, data: EventOddsResponse): void {
  const dir = path.join(getDataDir(), "raw", "odds", "player-props", date);
  fs.mkdirSync(dir, { recursive: true });
  
  const filename = `${eventId}.json`;
  const filepath = path.join(dir, filename);
  
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
}

const ODDS_API_KEY = process.env.ODDS_API_KEY;
const BASE_URL = "https://api.the-odds-api.com/v4";

// Player prop markets we want to capture
const PLAYER_PROP_MARKETS = [
  "player_points",
  "player_rebounds", 
  "player_assists",
  "player_threes",
  "player_blocks",
  "player_steals",
  "player_turnovers",
  "player_points_rebounds_assists",
  "player_points_rebounds",
  "player_points_assists",
  "player_rebounds_assists",
  "player_double_double",
  "player_triple_double",
];

interface PlayerPropOutcome {
  name: string;           // "Over" or "Under"
  description: string;    // Player name
  price: number;          // American odds
  point?: number;         // The line (e.g., 24.5)
}

interface PlayerPropMarket {
  key: string;            // e.g., "player_points"
  last_update: string;
  outcomes: PlayerPropOutcome[];
}

interface PlayerPropBookmaker {
  key: string;            // e.g., "fanduel"
  title: string;
  markets: PlayerPropMarket[];
}

interface EventOddsResponse {
  id: string;
  sport_key: string;
  sport_title?: string;
  home_team: string;
  away_team: string;
  commence_time: string;
  bookmakers: PlayerPropBookmaker[];
}

interface PlayerPropsDayBundle {
  date: string;
  generatedAt: string;
  source: "player-props-day-bundle-v1";
  eventCount: number;
  events: EventOddsResponse[];
}

/**
 * Fetch player props for a specific event
 */
async function fetchPlayerPropsForEvent(eventId: string): Promise<EventOddsResponse | null> {
  if (!ODDS_API_KEY) {
    throw new Error("ODDS_API_KEY not set");
  }

  const markets = PLAYER_PROP_MARKETS.join(",");
  const url = `${BASE_URL}/sports/basketball_nba/events/${eventId}/odds?apiKey=${ODDS_API_KEY}&regions=us&markets=${markets}&oddsFormat=american`;

  try {
    const res = await fetch(url);
    if (!res.ok) {
      if (res.status === 404) {
        return null; // Event not found or no props available
      }
      throw new Error(`Odds API error: ${res.status}`);
    }
    return res.json();
  } catch (err) {
    console.warn(`  Failed to fetch props for event ${eventId}:`, (err as Error).message);
    return null;
  }
}

/**
 * Get upcoming NBA events from The Odds API
 */
async function fetchUpcomingEvents(): Promise<Array<{ id: string; home_team: string; away_team: string; commence_time: string }>> {
  if (!ODDS_API_KEY) {
    throw new Error("ODDS_API_KEY not set");
  }

  const url = `${BASE_URL}/sports/basketball_nba/events?apiKey=${ODDS_API_KEY}`;
  const res = await fetch(url);
  
  if (!res.ok) {
    throw new Error(`Odds API error: ${res.status}`);
  }
  
  return res.json();
}

/**
 * Match player name from odds API to our database
 */
async function findPlayerId(playerName: string): Promise<number | null> {
  // Normalize the name
  const nameParts = playerName.toLowerCase().trim().split(" ");
  if (nameParts.length < 2) return null;

  const firstName = nameParts[0];
  const lastName = nameParts.slice(1).join(" ");

  // Try exact match first
  const player = await prisma.player.findFirst({
    where: {
      OR: [
        {
          firstname: { contains: firstName, mode: "insensitive" },
          lastname: { contains: lastName, mode: "insensitive" },
        },
        // Handle "Jr." suffix variations
        {
          firstname: { contains: firstName, mode: "insensitive" },
          lastname: { startsWith: lastName.replace(/\s*jr\.?$/i, ""), mode: "insensitive" },
        },
      ],
    },
  });

  return player?.id ?? null;
}

/**
 * NBA team name to abbreviation mapping for The Odds API
 */
const TEAM_NAME_MAP: Record<string, { abbr: string; city: string; name: string }> = {
  "atlanta hawks": { abbr: "ATL", city: "Atlanta", name: "Hawks" },
  "boston celtics": { abbr: "BOS", city: "Boston", name: "Celtics" },
  "brooklyn nets": { abbr: "BKN", city: "Brooklyn", name: "Nets" },
  "charlotte hornets": { abbr: "CHA", city: "Charlotte", name: "Hornets" },
  "chicago bulls": { abbr: "CHI", city: "Chicago", name: "Bulls" },
  "cleveland cavaliers": { abbr: "CLE", city: "Cleveland", name: "Cavaliers" },
  "dallas mavericks": { abbr: "DAL", city: "Dallas", name: "Mavericks" },
  "denver nuggets": { abbr: "DEN", city: "Denver", name: "Nuggets" },
  "detroit pistons": { abbr: "DET", city: "Detroit", name: "Pistons" },
  "golden state warriors": { abbr: "GSW", city: "Golden State", name: "Warriors" },
  "houston rockets": { abbr: "HOU", city: "Houston", name: "Rockets" },
  "indiana pacers": { abbr: "IND", city: "Indiana", name: "Pacers" },
  "los angeles clippers": { abbr: "LAC", city: "Los Angeles", name: "Clippers" },
  "los angeles lakers": { abbr: "LAL", city: "Los Angeles", name: "Lakers" },
  "memphis grizzlies": { abbr: "MEM", city: "Memphis", name: "Grizzlies" },
  "miami heat": { abbr: "MIA", city: "Miami", name: "Heat" },
  "milwaukee bucks": { abbr: "MIL", city: "Milwaukee", name: "Bucks" },
  "minnesota timberwolves": { abbr: "MIN", city: "Minnesota", name: "Timberwolves" },
  "new orleans pelicans": { abbr: "NOP", city: "New Orleans", name: "Pelicans" },
  "new york knicks": { abbr: "NYK", city: "New York", name: "Knicks" },
  "oklahoma city thunder": { abbr: "OKC", city: "Oklahoma City", name: "Thunder" },
  "orlando magic": { abbr: "ORL", city: "Orlando", name: "Magic" },
  "philadelphia 76ers": { abbr: "PHI", city: "Philadelphia", name: "76ers" },
  "phoenix suns": { abbr: "PHX", city: "Phoenix", name: "Suns" },
  "portland trail blazers": { abbr: "POR", city: "Portland", name: "Trail Blazers" },
  "sacramento kings": { abbr: "SAC", city: "Sacramento", name: "Kings" },
  "san antonio spurs": { abbr: "SAS", city: "San Antonio", name: "Spurs" },
  "toronto raptors": { abbr: "TOR", city: "Toronto", name: "Raptors" },
  "utah jazz": { abbr: "UTA", city: "Utah", name: "Jazz" },
  "washington wizards": { abbr: "WAS", city: "Washington", name: "Wizards" },
};

/**
 * Find team from Odds API name using existing database teams
 */
async function findTeamId(teamName: string): Promise<number | null> {
  const normalized = teamName.toLowerCase().trim();
  const teamInfo = TEAM_NAME_MAP[normalized];
  
  if (!teamInfo) {
    console.warn(`    Unknown team: ${teamName}`);
    return null;
  }
  
  // Try to find existing team by code, name, or city
  const existing = await prisma.team.findFirst({
    where: {
      OR: [
        { code: teamInfo.abbr },
        { name: { contains: teamInfo.name, mode: "insensitive" } },
        { city: { contains: teamInfo.city, mode: "insensitive" } },
      ],
    },
  });
  
  if (existing) return existing.id;
  
  // If not found, list available teams for debugging
  console.warn(`    Team not found for: ${teamName} (${teamInfo.abbr})`);
  return null;
}

/**
 * Match game from odds API to our database, or create if missing
 */
async function findOrCreateGame(
  homeTeam: string,
  awayTeam: string,
  commenceTime: string,
  eventId: string
): Promise<string | null> {
  const commenceDate = new Date(commenceTime);
  const easternDateStr = getEasternDateFromUtc(commenceDate);
  const gameDateForDb = dateStringToUtcMidday(easternDateStr);
  
  // First, check if we already created this game by sourceGameId
  const sourceGameId = -Math.abs(hashString(eventId));
  const existingBySource = await prisma.game.findUnique({
    where: { sourceGameId },
  });
  if (existingBySource) {
    return existingBySource.id;
  }
  
  const normalizeTeam = (name: string) => name.toLowerCase().replace(/[^a-z]/g, "");
  const homeNorm = normalizeTeam(homeTeam);
  const awayNorm = normalizeTeam(awayTeam);

  const games = await prisma.game.findMany({
    where: { date: gameDateForDb },
    include: { homeTeam: true, awayTeam: true },
  });

  // Try to find existing match by team names
  for (const game of games) {
    const dbHome = normalizeTeam(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
    const dbAway = normalizeTeam(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");

    if (
      (dbHome.includes(homeNorm) || homeNorm.includes(dbHome)) &&
      (dbAway.includes(awayNorm) || awayNorm.includes(dbAway))
    ) {
      return game.id;
    }
  }

  // Game not found - create it
  console.log(`    Creating game: ${awayTeam} @ ${homeTeam} (${easternDateStr})`);
  
  const homeTeamId = await findTeamId(homeTeam);
  const awayTeamId = await findTeamId(awayTeam);
  
  if (!homeTeamId || !awayTeamId) {
    console.warn(`    Could not resolve teams for game`);
    return null;
  }
  
  const [y, m] = easternDateStr.split("-").map(Number);
  const season = m >= 10 ? y : y - 1;
  
  const game = await prisma.game.create({
    data: {
      sourceGameId,
      date: gameDateForDb,
      homeTeamId,
      awayTeamId,
      homeTeamNameSnapshot: homeTeam,
      awayTeamNameSnapshot: awayTeam,
      season,
      stage: 0, // Regular season placeholder
      league: "NBA",
      homeScore: 0,
      awayScore: 0,
      status: "Scheduled",
      tipoffTimeUtc: commenceDate,
    },
  });
  
  console.log(`    Created game id: ${game.id}`);
  return game.id;
}

/**
 * Simple hash function to generate numeric ID from string
 */
function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash);
}

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (!args[i].startsWith("--")) continue;
    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

function listRawPropDates(rawRoot: string): string[] {
  if (!fs.existsSync(rawRoot)) return [];
  return fs
    .readdirSync(rawRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(entry.name))
    .map((entry) => entry.name)
    .sort();
}

function dateRange(from: string, to: string): string[] {
  const out: string[] = [];
  const cur = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  while (cur <= end) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

async function upsertPropsForGame(gameId: string, propsData: EventOddsResponse): Promise<number> {
  let upserted = 0;
  for (const bookmaker of propsData.bookmakers) {
    for (const market of bookmaker.markets) {
      const playerOutcomes = new Map<string, { over?: PlayerPropOutcome; under?: PlayerPropOutcome }>();

      for (const outcome of market.outcomes) {
        const playerName = outcome.description;
        if (!playerName) continue;
        const existing = playerOutcomes.get(playerName) || {};
        if (outcome.name === "Over" || outcome.name === "Yes") {
          existing.over = outcome;
        } else if (outcome.name === "Under" || outcome.name === "No") {
          existing.under = outcome;
        }
        playerOutcomes.set(playerName, existing);
      }

      for (const [playerName, outcomes] of playerOutcomes) {
        const playerId = await findPlayerId(playerName);
        if (!playerId) continue;

        const line = outcomes.over?.point ?? outcomes.under?.point ?? null;
        if (line == null || !Number.isFinite(line)) continue;

        await prisma.playerPropOdds.upsert({
          where: {
            gameId_playerId_source_market: {
              gameId,
              playerId,
              source: bookmaker.key,
              market: market.key,
            },
          },
          create: {
            gameId,
            playerId,
            source: bookmaker.key,
            market: market.key,
            line,
            overPrice: outcomes.over?.price ?? null,
            underPrice: outcomes.under?.price ?? null,
          },
          update: {
            line,
            overPrice: outcomes.over?.price ?? null,
            underPrice: outcomes.under?.price ?? null,
            fetchedAt: new Date(),
          },
        });
        upserted++;
      }
    }
  }
  return upserted;
}

/**
 * Sync player props for upcoming games
 */
export async function syncPlayerPropsLive(): Promise<number> {
  console.log("Fetching upcoming NBA events for player props...");
  
  const events = await fetchUpcomingEvents();
  console.log(`  Found ${events.length} upcoming events`);
  
  let totalUpserted = 0;
  
  for (const event of events) {
    // Find or create matching game in our database
    const gameId = await findOrCreateGame(event.home_team, event.away_team, event.commence_time, event.id);
    if (!gameId) {
      console.log(`  Could not create/find game for ${event.away_team} @ ${event.home_team}`);
      continue;
    }
    
    console.log(`  Fetching props for ${event.away_team} @ ${event.home_team}...`);
    
    const propsData = await fetchPlayerPropsForEvent(event.id);
    if (!propsData || !propsData.bookmakers.length) {
      console.log(`    No props available`);
      continue;
    }
    
    const eventDate = event.commence_time.slice(0, 10);
    saveRawPlayerProps(eventDate, event.id, propsData);
    
    totalUpserted += await upsertPropsForGame(gameId, propsData);
  }
  
  console.log(`  Upserted ${totalUpserted} player prop odds`);
  return totalUpserted;
}

/**
 * Sync player props for a specific date (historical)
 */
export async function syncPlayerPropsForDate(date: string): Promise<number> {
  console.log(`Fetching player props for ${date}...`);
  
  // For historical, we need to use the historical odds endpoint
  // This requires the eventId which we'd need to look up or store
  // Implementation depends on how The Odds API structures historical player props
  
  console.log("  Historical player props sync not yet implemented");
  console.log("  (Requires additional API endpoint investigation)");
  
  return 0;
}

export async function buildPlayerPropsDayFiles(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const dataDir = getDataDir();
  const rawRoot = path.join(dataDir, "raw", "odds", "player-props");
  const outDir = path.join(dataDir, "raw", "odds", "player-props-day");
  fs.mkdirSync(outDir, { recursive: true });

  const discovered = listRawPropDates(rawRoot);
  if (discovered.length === 0) {
    throw new Error(`No raw player prop folders found at ${rawRoot}`);
  }

  const from = flags.from ?? discovered[0];
  const to = flags.to ?? discovered[discovered.length - 1];
  const targets = dateRange(from, to);

  console.log(`Building day bundle files for ${from} -> ${to} (${targets.length} dates)...`);

  for (const date of targets) {
    const dateDir = path.join(rawRoot, date);
    if (!fs.existsSync(dateDir)) continue;

    const files = fs.readdirSync(dateDir).filter((f) => f.endsWith(".json")).sort();
    const events: EventOddsResponse[] = [];
    for (const file of files) {
      const filePath = path.join(dateDir, file);
      try {
        const raw = fs.readFileSync(filePath, "utf-8");
        const parsed = JSON.parse(raw) as EventOddsResponse;
        if (
          parsed &&
          typeof parsed.id === "string" &&
          typeof parsed.home_team === "string" &&
          typeof parsed.away_team === "string" &&
          Array.isArray(parsed.bookmakers)
        ) {
          events.push(parsed);
        }
      } catch (err) {
        console.warn(`  Skipping unreadable props file ${filePath}: ${(err as Error).message}`);
      }
    }

    events.sort((a, b) => {
      const at = Date.parse(a.commence_time || "");
      const bt = Date.parse(b.commence_time || "");
      if (Number.isFinite(at) && Number.isFinite(bt) && at !== bt) return at - bt;
      return a.id.localeCompare(b.id);
    });

    const bundle: PlayerPropsDayBundle = {
      date,
      generatedAt: new Date().toISOString(),
      source: "player-props-day-bundle-v1",
      eventCount: events.length,
      events,
    };

    const outPath = path.join(outDir, `${date}.json`);
    fs.writeFileSync(outPath, JSON.stringify(bundle, null, 2));
    console.log(`  ${date}: ${events.length} events -> ${outPath}`);
  }
}

function loadDayBundle(date: string): EventOddsResponse[] {
  const filePath = path.join(getDataDir(), "raw", "odds", "player-props-day", `${date}.json`);
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf-8");
  const parsed = JSON.parse(raw) as PlayerPropsDayBundle | EventOddsResponse[];
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.events)) return parsed.events;
  return [];
}

function loadRawPropEventsForDate(date: string): EventOddsResponse[] {
  const dateDir = path.join(getDataDir(), "raw", "odds", "player-props", date);
  if (!fs.existsSync(dateDir)) return [];
  const files = fs.readdirSync(dateDir).filter((f) => f.endsWith(".json")).sort();
  const events: EventOddsResponse[] = [];
  for (const file of files) {
    try {
      const raw = fs.readFileSync(path.join(dateDir, file), "utf-8");
      const parsed = JSON.parse(raw) as EventOddsResponse;
      if (parsed && typeof parsed.id === "string" && Array.isArray(parsed.bookmakers)) {
        events.push(parsed);
      }
    } catch {
      // Best-effort ingest: skip malformed files.
    }
  }
  return events;
}

export async function ingestPlayerPropsRaw(args: string[] = []): Promise<number> {
  const flags = parseFlags(args);
  const preferDayFiles = (flags["prefer-day-files"] ?? "false") === "true";
  const rawRoot = path.join(getDataDir(), "raw", "odds", "player-props");
  const discovered = listRawPropDates(rawRoot);
  if (discovered.length === 0) {
    throw new Error(`No raw player prop date folders found at ${rawRoot}`);
  }

  const from = flags.from ?? discovered[0];
  const to = flags.to ?? discovered[discovered.length - 1];
  const targets = dateRange(from, to);

  console.log(`Ingesting raw player props for ${from} -> ${to} (${targets.length} dates)...`);
  let totalUpserted = 0;
  let totalEvents = 0;

  for (const date of targets) {
    const events = preferDayFiles ? loadDayBundle(date) : loadRawPropEventsForDate(date);
    if (events.length === 0) continue;

    let dateUpserted = 0;
    for (const event of events) {
      const gameId = await findOrCreateGame(event.home_team, event.away_team, event.commence_time, event.id);
      if (!gameId) continue;
      dateUpserted += await upsertPropsForGame(gameId, event);
    }

    totalEvents += events.length;
    totalUpserted += dateUpserted;
    console.log(`  ${date}: ${events.length} events, ${dateUpserted} prop rows upserted`);
  }

  console.log(`Raw props ingest complete: ${totalEvents} events, ${totalUpserted} prop rows upserted`);
  return totalUpserted;
}
