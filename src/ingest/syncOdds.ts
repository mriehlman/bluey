import { prisma } from "../db/prisma.js";
import { fetchNbaOdds, fetchHistoricalOdds, fetchHistoricalEvents, fetchHistoricalEventOdds, type OddsEvent } from "../api/oddsApi.js";
import { getEasternDateFromUtc, dateStringToUtcMidday } from "./utils.js";
import * as fs from "fs";
import * as path from "path";

const PLAYER_PROP_MARKETS = [
  "player_points",
  "player_rebounds", 
  "player_assists",
  "player_threes",
  "player_points_rebounds_assists",
].join(",");

const PRIORITY_BOOKS = ["fanduel", "draftkings", "betmgm", "pointsbet", "caesars"];

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function saveRawOdds(date: string, events: OddsEvent[], type: "live" | "historical"): void {
  const dir = path.join(getDataDir(), "raw", "odds", type);
  fs.mkdirSync(dir, { recursive: true });
  
  const filename = `${date}.json`;
  const filepath = path.join(dir, filename);
  
  fs.writeFileSync(filepath, JSON.stringify(events, null, 2));
  console.log(`  Saved raw JSON: ${filepath}`);
}

function normalizeTeamName(name: string): string {
  return name.toLowerCase().replace(/[^a-z]/g, "");
}

const TEAM_CANONICAL: Record<string, string> = {
  "atlantahawks": "hawks", "hawks": "hawks", "atl": "hawks",
  "bostonceltics": "celtics", "celtics": "celtics", "bos": "celtics",
  "brooklynnets": "nets", "nets": "nets", "bkn": "nets",
  "charlottehornets": "hornets", "hornets": "hornets", "cha": "hornets",
  "chicagobulls": "bulls", "bulls": "bulls", "chi": "bulls",
  "clevelandcavaliers": "cavaliers", "cavaliers": "cavaliers", "cavs": "cavaliers", "cle": "cavaliers",
  "dallasmavericks": "mavericks", "mavericks": "mavericks", "mavs": "mavericks", "dal": "mavericks",
  "denvernuggets": "nuggets", "nuggets": "nuggets", "den": "nuggets",
  "detroitpistons": "pistons", "pistons": "pistons", "det": "pistons",
  "goldenstatewarriors": "warriors", "warriors": "warriors", "gsw": "warriors",
  "houstonrockets": "rockets", "rockets": "rockets", "hou": "rockets",
  "indianapacers": "pacers", "pacers": "pacers", "ind": "pacers",
  "losangelesclippers": "clippers", "clippers": "clippers", "lac": "clippers", "laclippers": "clippers",
  "losangeleslakers": "lakers", "lakers": "lakers", "lal": "lakers", "lalakers": "lakers",
  "memphisgrizzlies": "grizzlies", "grizzlies": "grizzlies", "mem": "grizzlies",
  "miamiheat": "heat", "heat": "heat", "mia": "heat",
  "milwaukeebucks": "bucks", "bucks": "bucks", "mil": "bucks",
  "minnesotatimberwolves": "timberwolves", "timberwolves": "timberwolves", "wolves": "timberwolves", "min": "timberwolves",
  "neworleanspelicans": "pelicans", "pelicans": "pelicans", "nop": "pelicans",
  "newyorkknicks": "knicks", "knicks": "knicks", "nyk": "knicks",
  "oklahomacitythunder": "thunder", "thunder": "thunder", "okc": "thunder",
  "orlandomagic": "magic", "magic": "magic", "orl": "magic",
  "philadelphia76ers": "76ers", "76ers": "76ers", "sixers": "76ers", "phi": "76ers",
  "phoenixsuns": "suns", "suns": "suns", "phx": "suns",
  "portlandtrailblazers": "blazers", "trailblazers": "blazers", "blazers": "blazers", "por": "blazers",
  "sacramentokings": "kings", "kings": "kings", "sac": "kings",
  "sanantoniospurs": "spurs", "spurs": "spurs", "sas": "spurs",
  "torontoraptors": "raptors", "raptors": "raptors", "tor": "raptors",
  "utahjazz": "jazz", "jazz": "jazz", "uta": "jazz",
  "washingtonwizards": "wizards", "wizards": "wizards", "was": "wizards",
};

function getCanonicalTeam(name: string): string {
  const norm = normalizeTeamName(name);
  // Direct lookup
  if (TEAM_CANONICAL[norm]) return TEAM_CANONICAL[norm];
  // Try finding a key that's a substring
  for (const [key, canonical] of Object.entries(TEAM_CANONICAL)) {
    if (norm.includes(key) || key.includes(norm)) return canonical;
  }
  return norm;
}

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash);
}

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

async function findTeamId(teamName: string): Promise<number | null> {
  const normalized = teamName.toLowerCase().trim();
  const teamInfo = TEAM_NAME_MAP[normalized];
  
  if (!teamInfo) return null;
  
  const existing = await prisma.team.findFirst({
    where: {
      OR: [
        { code: teamInfo.abbr },
        { name: { contains: teamInfo.name, mode: "insensitive" } },
        { city: { contains: teamInfo.city, mode: "insensitive" } },
      ],
    },
  });
  
  return existing?.id ?? null;
}

async function findOrCreateGame(event: OddsEvent): Promise<string | null> {
  const commenceDate = new Date(event.commence_time);
  const easternDateStr = getEasternDateFromUtc(commenceDate);
  const gameDate = dateStringToUtcMidday(easternDateStr);

  // First check by odds_api external ID (games we've seen from Odds API before)
  const existingByOddsId = await prisma.gameExternalId.findUnique({
    where: { source_sourceId: { source: "odds_api", sourceId: event.id } },
    select: { gameId: true },
  });
  if (existingByOddsId) return existingByOddsId.gameId;

  // Fallback: check by sourceGameId (legacy for games we created from odds)
  const sourceGameId = -Math.abs(hashString(event.id));
  const existingBySource = await prisma.game.findUnique({
    where: { sourceGameId },
  });
  if (existingBySource) {
    await prisma.gameExternalId.upsert({
      where: { gameId_source: { gameId: existingBySource.id, source: "odds_api" } },
      update: { sourceId: event.id },
      create: { gameId: existingBySource.id, source: "odds_api", sourceId: event.id },
    });
    return existingBySource.id;
  }

  // Get canonical team names for matching
  const homeCanon = getCanonicalTeam(event.home_team);
  const awayCanon = getCanonicalTeam(event.away_team);
  
  // Use Eastern date (game day in North America)
  const games = await prisma.game.findMany({
    where: { 
      date: gameDate,
      // Prefer games with context (real games from stats ingest)
      context: { isNot: null },
    },
    include: { homeTeam: true, awayTeam: true },
  });

  for (const game of games) {
    const dbHome = getCanonicalTeam(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
    const dbAway = getCanonicalTeam(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");

    if (
      (dbHome === homeCanon && dbAway === awayCanon) ||
      (dbHome === awayCanon && dbAway === homeCanon)
    ) {
      await prisma.gameExternalId.upsert({
        where: { gameId_source: { gameId: game.id, source: "odds_api" } },
        update: { sourceId: event.id },
        create: { gameId: game.id, source: "odds_api", sourceId: event.id },
      });
      return game.id;
    }
  }

  // Also check games without context (fallback)
  const gamesNoContext = await prisma.game.findMany({
    where: { 
      date: gameDate,
    },
    include: { homeTeam: true, awayTeam: true },
  });

  for (const game of gamesNoContext) {
    const dbHome = getCanonicalTeam(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
    const dbAway = getCanonicalTeam(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");

    if (
      (dbHome === homeCanon && dbAway === awayCanon) ||
      (dbHome === awayCanon && dbAway === homeCanon)
    ) {
      await prisma.gameExternalId.upsert({
        where: { gameId_source: { gameId: game.id, source: "odds_api" } },
        update: { sourceId: event.id },
        create: { gameId: game.id, source: "odds_api", sourceId: event.id },
      });
      return game.id;
    }
  }

  // Create game if not found
  const homeTeamId = await findTeamId(event.home_team);
  const awayTeamId = await findTeamId(event.away_team);
  
  if (!homeTeamId || !awayTeamId) return null;

  const [y, m] = easternDateStr.split("-").map(Number);
  const season = m >= 10 ? y : y - 1;

  const game = await prisma.game.create({
    data: {
      sourceGameId,
      date: gameDate,
      homeTeamId,
      awayTeamId,
      homeTeamNameSnapshot: event.home_team,
      awayTeamNameSnapshot: event.away_team,
      season,
      stage: 0,
      league: "NBA",
      homeScore: 0,
      awayScore: 0,
      status: "Scheduled",
      tipoffTimeUtc: commenceDate,
    },
  });

  await prisma.gameExternalId.create({
    data: { gameId: game.id, source: "odds_api", sourceId: event.id },
  });

  console.log(`    Created game: ${event.away_team} @ ${event.home_team} (${game.id})`);
  return game.id;
}

function extractOddsFromBookmaker(event: OddsEvent, bookmakerKey: string) {
  const bookmaker = event.bookmakers.find((b) => b.key === bookmakerKey);
  if (!bookmaker) return null;

  let spreadHome: number | null = null;
  let spreadAway: number | null = null;
  let totalOver: number | null = null;
  let totalUnder: number | null = null;
  let mlHome: number | null = null;
  let mlAway: number | null = null;

  const homeCanon = getCanonicalTeam(event.home_team);
  const awayCanon = getCanonicalTeam(event.away_team);

  for (const market of bookmaker.markets) {
    if (market.key === "spreads") {
      for (const o of market.outcomes) {
        const oCanon = getCanonicalTeam(o.name);
        if (oCanon === homeCanon) spreadHome = o.point ?? null;
        if (oCanon === awayCanon) spreadAway = o.point ?? null;
      }
    }
    if (market.key === "totals") {
      for (const o of market.outcomes) {
        if (o.name === "Over") totalOver = o.point ?? null;
        if (o.name === "Under") totalUnder = o.point ?? null;
      }
    }
    if (market.key === "h2h") {
      for (const o of market.outcomes) {
        const oCanon = getCanonicalTeam(o.name);
        if (oCanon === homeCanon) mlHome = o.price;
        if (oCanon === awayCanon) mlAway = o.price;
      }
    }
  }

  return { spreadHome, spreadAway, totalOver, totalUnder, mlHome, mlAway };
}

async function processOddsEvents(events: OddsEvent[], debug = false): Promise<number> {
  let upserted = 0;

  for (const event of events) {
    const gameId = await findOrCreateGame(event);
    if (!gameId) {
      if (debug) console.log(`    Could not find/create game for ${event.away_team} @ ${event.home_team}`);
      continue;
    }

    for (const bookmaker of event.bookmakers) {
      const odds = extractOddsFromBookmaker(event, bookmaker.key);
      if (!odds) continue;

      await prisma.gameOdds.upsert({
        where: { gameId_source: { gameId, source: bookmaker.key } },
        update: { ...odds, fetchedAt: new Date() },
        create: { gameId, source: bookmaker.key, ...odds },
      });
      upserted++;
    }

    for (const bookKey of PRIORITY_BOOKS) {
      const odds = extractOddsFromBookmaker(event, bookKey);
      if (odds && (odds.spreadHome != null || odds.totalOver != null)) {
        await prisma.gameOdds.upsert({
          where: { gameId_source: { gameId, source: "consensus" } },
          update: { ...odds, fetchedAt: new Date() },
          create: { gameId, source: "consensus", ...odds },
        });
        upserted++;
        break;
      }
    }
  }

  console.log(`  Upserted ${upserted} odds rows`);
  return upserted;
}

export async function syncOddsLive(debug = false): Promise<number> {
  console.log("Fetching live NBA odds...");
  const events = await fetchNbaOdds();
  console.log(`  Found ${events.length} events with odds`);
  
  if (events.length > 0) {
    const today = new Date().toISOString().slice(0, 10);
    saveRawOdds(today, events, "live");
  }
  
  return processOddsEvents(events, debug);
}

export async function syncOddsForDate(date: string): Promise<number> {
  console.log(`Fetching historical odds for ${date}...`);
  const events = await fetchHistoricalOdds(date);
  console.log(`  Found ${events.length} events with odds`);
  
  if (events.length > 0) {
    saveRawOdds(date, events, "historical");
  }
  
  return processOddsEvents(events);
}

export async function syncPlayerPropsForDate(date: string): Promise<number> {
  console.log(`Fetching historical player props for ${date}...`);
  
  const events = await fetchHistoricalEvents(date);
  if (events.length === 0) {
    console.log(`  No events found`);
    return 0;
  }
  
  console.log(`  Found ${events.length} events`);
  let totalUpserted = 0;
  
  const propsDir = path.join(getDataDir(), "raw", "odds", "player-props-historical", date);
  fs.mkdirSync(propsDir, { recursive: true });
  
  for (const event of events) {
    const propsData = await fetchHistoricalEventOdds(event.id, date, PLAYER_PROP_MARKETS);
    if (!propsData || !propsData.bookmakers?.length) {
      continue;
    }
    
    fs.writeFileSync(
      path.join(propsDir, `${event.id}.json`),
      JSON.stringify(propsData, null, 2)
    );
    
    const gameId = await findOrCreateGame(propsData);
    if (!gameId) continue;
    
    for (const bookmaker of propsData.bookmakers) {
      for (const market of bookmaker.markets) {
        const playerOutcomes = new Map<string, { over?: { price: number; point?: number }; under?: { price: number; point?: number } }>();
        
        for (const outcome of market.outcomes) {
          const playerName = (outcome as { description?: string }).description;
          if (!playerName) continue;
          
          const existing = playerOutcomes.get(playerName) || {};
          if (outcome.name === "Over") {
            existing.over = { price: outcome.price, point: outcome.point };
          } else if (outcome.name === "Under") {
            existing.under = { price: outcome.price, point: outcome.point };
          }
          playerOutcomes.set(playerName, existing);
        }
        
        for (const [playerName, outcomes] of playerOutcomes) {
          const playerId = await findPlayerId(playerName);
          if (!playerId) continue;
          
          const line = outcomes.over?.point ?? outcomes.under?.point ?? null;
          if (line === null) continue;
          
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
          totalUpserted++;
        }
      }
    }
  }
  
  console.log(`  Upserted ${totalUpserted} player prop odds`);
  return totalUpserted;
}

async function findPlayerId(playerName: string): Promise<number | null> {
  const nameParts = playerName.toLowerCase().trim().split(" ");
  if (nameParts.length < 2) return null;

  const firstName = nameParts[0];
  const lastName = nameParts.slice(1).join(" ");

  const player = await prisma.player.findFirst({
    where: {
      OR: [
        {
          firstname: { contains: firstName, mode: "insensitive" },
          lastname: { contains: lastName, mode: "insensitive" },
        },
        {
          firstname: { contains: firstName, mode: "insensitive" },
          lastname: { startsWith: lastName.replace(/\s*jr\.?$/i, ""), mode: "insensitive" },
        },
      ],
    },
  });

  return player?.id ?? null;
}
