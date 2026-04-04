import { prisma } from "@bluey/db";
import { fetchNbaOdds, fetchHistoricalOdds, fetchLiveEvents, fetchLiveEventOdds, fetchHistoricalEvents, fetchHistoricalEventOdds, getHistoricalSnapshotIsoUtc, type OddsEvent } from "../api/oddsApi";
import { getEasternDateFromUtc, dateStringToUtcMidday } from "./utils";
import * as fs from "fs";
import * as path from "path";
import { getDataDir } from "../config/paths";

const PLAYER_PROP_MARKETS = [
  "player_points",
  "player_rebounds", 
  "player_assists",
  "player_threes",
  "player_points_rebounds_assists",
].join(",");

const PRIORITY_BOOKS = ["fanduel", "draftkings", "betmgm", "pointsbet", "caesars"];

function saveRawOdds(date: string, events: OddsEvent[], type: "live" | "historical"): void {
  const dir = path.join(getDataDir(), "raw", "odds", type);
  fs.mkdirSync(dir, { recursive: true });
  
  const filename = `${date}.json`;
  const filepath = path.join(dir, filename);
  
  fs.writeFileSync(filepath, JSON.stringify(events, null, 2));
  console.log(`  Saved raw JSON: ${filepath}`);
}

function normalizeTeamName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]/g, "");
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

  const homeCanon = getCanonicalTeam(event.home_team);
  const awayCanon = getCanonicalTeam(event.away_team);

  const dayStart = new Date(Date.UTC(gameDate.getUTCFullYear(), gameDate.getUTCMonth(), gameDate.getUTCDate()));
  const dayEnd = new Date(dayStart.getTime() + 86_400_000);
  const sourceGameId = -Math.abs(hashString(event.id));

  // Helper: find the canonical game (from NBA stats) for the same matchup
  const findNbaStatsGame = async (): Promise<string | null> => {
    const candidates = await prisma.game.findMany({
      where: {
        date: { gte: dayStart, lt: dayEnd },
        externalIds: { some: { source: "nba_stats" } },
      },
      include: { homeTeam: true, awayTeam: true },
    });
    for (const g of candidates) {
      const h = getCanonicalTeam(g.homeTeam?.name ?? g.homeTeamNameSnapshot ?? "");
      const a = getCanonicalTeam(g.awayTeam?.name ?? g.awayTeamNameSnapshot ?? "");
      if ((h === homeCanon && a === awayCanon) || (h === awayCanon && a === homeCanon)) return g.id;
    }
    return null;
  };

  // Helper: link external ID and migrate odds from orphan if needed
  const linkAndMigrate = async (targetId: string, orphanId?: string) => {
    // Delete any stale external ID for this source+sourceId combo before creating
    await prisma.gameExternalId.deleteMany({
      where: { source: "odds_api", sourceId: event.id, gameId: { not: targetId } },
    });
    await prisma.gameExternalId.upsert({
      where: { gameId_source: { gameId: targetId, source: "odds_api" } },
      update: { sourceId: event.id },
      create: { gameId: targetId, source: "odds_api", sourceId: event.id },
    });
    if (orphanId && orphanId !== targetId) {
      const orphanOdds = await prisma.gameOdds.findMany({ where: { gameId: orphanId } });
      for (const o of orphanOdds) {
        await prisma.gameOdds.upsert({
          where: { gameId_source: { gameId: targetId, source: o.source } },
          update: { mlHome: o.mlHome, mlAway: o.mlAway, spreadHome: o.spreadHome, spreadAway: o.spreadAway, totalOver: o.totalOver, totalUnder: o.totalUnder, fetchedAt: o.fetchedAt },
          create: { gameId: targetId, source: o.source, mlHome: o.mlHome, mlAway: o.mlAway, spreadHome: o.spreadHome, spreadAway: o.spreadAway, totalOver: o.totalOver, totalUnder: o.totalUnder },
        });
      }
      await prisma.gameOdds.deleteMany({ where: { gameId: orphanId } });
      console.log(`    Migrated odds from orphan game ${orphanId} → ${targetId}`);
    }
  };

  // First check by odds_api external ID
  const existingByOddsId = await prisma.gameExternalId.findUnique({
    where: { source_sourceId: { source: "odds_api", sourceId: event.id } },
    select: { gameId: true },
  });
  if (existingByOddsId) {
    const nbaGameId = await findNbaStatsGame();
    if (nbaGameId && nbaGameId !== existingByOddsId.gameId) {
      await linkAndMigrate(nbaGameId, existingByOddsId.gameId);
      return nbaGameId;
    }
    return existingByOddsId.gameId;
  }

  // Fallback: check by sourceGameId (legacy for games we created from odds)
  const existingBySource = await prisma.game.findUnique({ where: { sourceGameId } });
  if (existingBySource) {
    const nbaGameId = await findNbaStatsGame();
    if (nbaGameId && nbaGameId !== existingBySource.id) {
      await linkAndMigrate(nbaGameId, existingBySource.id);
      return nbaGameId;
    }
    await linkAndMigrate(existingBySource.id);
    return existingBySource.id;
  }
  
  // Search by teams on same calendar day — prefer games with context
  const games = await prisma.game.findMany({
    where: { date: { gte: dayStart, lt: dayEnd }, context: { isNot: null } },
    include: { homeTeam: true, awayTeam: true },
  });
  for (const game of games) {
    const dbHome = getCanonicalTeam(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
    const dbAway = getCanonicalTeam(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");
    if ((dbHome === homeCanon && dbAway === awayCanon) || (dbHome === awayCanon && dbAway === homeCanon)) {
      await linkAndMigrate(game.id);
      return game.id;
    }
  }

  // Broaden: games without context on same date, then +/- 1 day
  const dayMs = 86_400_000;
  const nearbyStarts = [dayStart, new Date(dayStart.getTime() - dayMs), new Date(dayStart.getTime() + dayMs)];
  for (const searchStart of nearbyStarts) {
    const searchEnd = new Date(searchStart.getTime() + dayMs);
    const gamesNearby = await prisma.game.findMany({
      where: { date: { gte: searchStart, lt: searchEnd } },
      include: { homeTeam: true, awayTeam: true },
    });
    for (const game of gamesNearby) {
      const dbHome = getCanonicalTeam(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
      const dbAway = getCanonicalTeam(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");
      if ((dbHome === homeCanon && dbAway === awayCanon) || (dbHome === awayCanon && dbAway === homeCanon)) {
        await linkAndMigrate(game.id);
        return game.id;
      }
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
      stage: 2,
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

function extractOddsFromBookmaker(
  event: OddsEvent,
  bookmakerKey: string,
  dbHomeCanon: string,
  dbAwayCanon: string,
) {
  const bookmaker = event.bookmakers.find((b) => b.key === bookmakerKey);
  if (!bookmaker) return null;

  let spreadHome: number | null = null;
  let spreadAway: number | null = null;
  let spreadHomePrice: number | null = null;
  let spreadAwayPrice: number | null = null;
  let totalOver: number | null = null;
  let totalUnder: number | null = null;
  let totalOverPrice: number | null = null;
  let totalUnderPrice: number | null = null;
  let mlHome: number | null = null;
  let mlAway: number | null = null;

  for (const market of bookmaker.markets) {
    if (market.key === "spreads") {
      for (const o of market.outcomes) {
        const oCanon = getCanonicalTeam(o.name);
        if (oCanon === dbHomeCanon) { spreadHome = o.point ?? null; spreadHomePrice = o.price; }
        if (oCanon === dbAwayCanon) { spreadAway = o.point ?? null; spreadAwayPrice = o.price; }
      }
    }
    if (market.key === "totals") {
      for (const o of market.outcomes) {
        if (o.name === "Over") { totalOver = o.point ?? null; totalOverPrice = o.price; }
        if (o.name === "Under") { totalUnder = o.point ?? null; totalUnderPrice = o.price; }
      }
    }
    if (market.key === "h2h") {
      for (const o of market.outcomes) {
        const oCanon = getCanonicalTeam(o.name);
        if (oCanon === dbHomeCanon) mlHome = o.price;
        if (oCanon === dbAwayCanon) mlAway = o.price;
      }
    }
  }

  return {
    spreadHome, spreadAway, spreadHomePrice, spreadAwayPrice,
    totalOver, totalUnder, totalOverPrice, totalUnderPrice,
    mlHome, mlAway,
  };
}


async function processOddsEvents(events: OddsEvent[], debug = false): Promise<number> {
  let upserted = 0;

  for (const event of events) {
    const gameId = await findOrCreateGame(event);
    if (!gameId) {
      if (debug) console.log(`    Could not find/create game for ${event.away_team} @ ${event.home_team}`);
      continue;
    }

    const dbGame = await prisma.game.findUnique({
      where: { id: gameId },
      include: { homeTeam: true, awayTeam: true },
    });
    if (!dbGame) continue;

    const dbHomeCanon = getCanonicalTeam(dbGame.homeTeam?.name ?? dbGame.homeTeamNameSnapshot ?? "");
    const dbAwayCanon = getCanonicalTeam(dbGame.awayTeam?.name ?? dbGame.awayTeamNameSnapshot ?? "");

    if (debug) {
      console.log(`    Game ${gameId}: DB home="${dbGame.homeTeam?.name}" (${dbHomeCanon}) away="${dbGame.awayTeam?.name}" (${dbAwayCanon})`);
      console.log(`      API home="${event.home_team}" away="${event.away_team}"`);
    }

    for (const bookmaker of event.bookmakers) {
      const odds = extractOddsFromBookmaker(event, bookmaker.key, dbHomeCanon, dbAwayCanon);
      if (!odds) continue;

      const { spreadHomePrice: shp, spreadAwayPrice: sap, totalOverPrice: top_, totalUnderPrice: tup, ...prismaOdds } = odds;
      await prisma.$transaction(async (tx) => {
        await tx.gameOdds.upsert({
          where: { gameId_source: { gameId, source: bookmaker.key } },
          update: { ...prismaOdds, fetchedAt: new Date() },
          create: { gameId, source: bookmaker.key, ...prismaOdds },
        });
        await tx.$executeRawUnsafe(
          `UPDATE "GameOdds" SET "spreadHomePrice" = $1, "spreadAwayPrice" = $2, "totalOverPrice" = $3, "totalUnderPrice" = $4 WHERE "gameId" = $5 AND "source" = $6`,
          shp, sap, top_, tup, gameId, bookmaker.key,
        );
      });
      upserted++;
    }

    const merged: NonNullable<ReturnType<typeof extractOddsFromBookmaker>> = {
      spreadHome: null, spreadAway: null, spreadHomePrice: null, spreadAwayPrice: null,
      totalOver: null, totalUnder: null, totalOverPrice: null, totalUnderPrice: null,
      mlHome: null, mlAway: null,
    };
    for (const bookKey of PRIORITY_BOOKS) {
      const odds = extractOddsFromBookmaker(event, bookKey, dbHomeCanon, dbAwayCanon);
      if (!odds) continue;
      if (merged.spreadHome == null && odds.spreadHome != null) {
        merged.spreadHome = odds.spreadHome; merged.spreadAway = odds.spreadAway;
        merged.spreadHomePrice = odds.spreadHomePrice; merged.spreadAwayPrice = odds.spreadAwayPrice;
      }
      if (merged.totalOver == null && odds.totalOver != null) {
        merged.totalOver = odds.totalOver; merged.totalUnder = odds.totalUnder;
        merged.totalOverPrice = odds.totalOverPrice; merged.totalUnderPrice = odds.totalUnderPrice;
      }
      if (merged.mlHome == null && odds.mlHome != null) {
        merged.mlHome = odds.mlHome; merged.mlAway = odds.mlAway;
      }
      if (merged.spreadHome != null && merged.totalOver != null && merged.mlHome != null) break;
    }
    if (merged.spreadHome != null || merged.totalOver != null || merged.mlHome != null) {
      const { spreadHomePrice: shp, spreadAwayPrice: sap, totalOverPrice: top_, totalUnderPrice: tup, ...prismaOdds } = merged;
      await prisma.$transaction(async (tx) => {
        await tx.gameOdds.upsert({
          where: { gameId_source: { gameId, source: "consensus" } },
          update: { ...prismaOdds, fetchedAt: new Date() },
          create: { gameId, source: "consensus", ...prismaOdds },
        });
        await tx.$executeRawUnsafe(
          `UPDATE "GameOdds" SET "spreadHomePrice" = $1, "spreadAwayPrice" = $2, "totalOverPrice" = $3, "totalUnderPrice" = $4 WHERE "gameId" = $5 AND "source" = $6`,
          shp, sap, top_, tup, gameId, "consensus",
        );
      });
      upserted++;
    }
  }

  console.log(`  Upserted ${upserted} odds rows`);
  return upserted;
}

export async function syncOddsLive(debug = false): Promise<number> {
  const today = getEasternDateFromUtc(new Date());
  console.log(`Fetching NBA odds snapshot for ${today} at ${getHistoricalSnapshotIsoUtc(today)} (target: 7am ET window)...`);
  const events = await fetchHistoricalOdds(today);
  console.log(`  Found ${events.length} events with odds`);
  
  if (events.length > 0) {
    saveRawOdds(today, events, "live");
  }
  
  return processOddsEvents(events, debug);
}

export async function syncOddsForDate(date: string): Promise<number> {
  const today = getEasternDateFromUtc(new Date());
  const isTodayOrFuture = date >= today;

  if (isTodayOrFuture) {
    console.log(`Fetching live odds for ${date}...`);
    const liveEvents = await fetchNbaOdds();
    const dateEvents = liveEvents.filter((ev) => {
      const evDate = getEasternDateFromUtc(new Date(ev.commence_time));
      return evDate === date;
    });
    console.log(`  Found ${dateEvents.length} events for ${date} (${liveEvents.length} total live)`);
    if (dateEvents.length > 0) {
      saveRawOdds(date, dateEvents, "live");
      return processOddsEvents(dateEvents);
    }
    console.log(`  No live odds found for ${date}, falling back to historical snapshot...`);
  }

  console.log(`Fetching historical odds for ${date} at ${getHistoricalSnapshotIsoUtc(date)} (target: 7am ET window)...`);
  const events = await fetchHistoricalOdds(date);
  console.log(`  Found ${events.length} events with odds`);
  
  if (events.length > 0) {
    saveRawOdds(date, events, "historical");
  }
  
  return processOddsEvents(events);
}

export async function syncPlayerPropsForDate(date: string): Promise<number> {
  const today = getEasternDateFromUtc(new Date());
  const isToday = date === today;

  let events: Array<{ id: string; home_team: string; away_team: string; commence_time: string }>;
  const fetchEventOdds = isToday
    ? (eventId: string) => fetchLiveEventOdds(eventId, PLAYER_PROP_MARKETS)
    : (eventId: string) => fetchHistoricalEventOdds(eventId, date, PLAYER_PROP_MARKETS);

  if (isToday) {
    console.log(`Fetching live player props for today (${date})...`);
    events = await fetchLiveEvents();
  } else {
    console.log(`Fetching historical player props for ${date} at ${getHistoricalSnapshotIsoUtc(date)} (target: 7am ET window)...`);
    events = await fetchHistoricalEvents(date);
  }

  if (events.length === 0) {
    console.log(`  No events found`);
    return 0;
  }
  
  console.log(`  Found ${events.length} events`);
  let totalUpserted = 0;
  
  const propsDir = path.join(getDataDir(), "raw", "odds", "player-props", date);
  fs.mkdirSync(propsDir, { recursive: true });
  
  for (const event of events) {
    const propsData = await fetchEventOdds(event.id);
    if (!propsData || !propsData.bookmakers?.length) {
      continue;
    }
    
    const payload = JSON.stringify(propsData, null, 2);
    fs.writeFileSync(path.join(propsDir, `${event.id}.json`), payload);
    
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
