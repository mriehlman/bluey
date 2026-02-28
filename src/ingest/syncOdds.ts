import { prisma } from "../db/prisma.js";
import { fetchNbaOdds, fetchHistoricalOdds, type OddsEvent } from "../api/oddsApi.js";

const PRIORITY_BOOKS = ["fanduel", "draftkings", "betmgm", "pointsbet", "caesars"];

function normalizeTeamName(name: string): string {
  return name.toLowerCase().replace(/[^a-z]/g, "");
}

async function matchGameToOdds(event: OddsEvent): Promise<string | null> {
  const commenceDate = event.commence_time.slice(0, 10);
  const games = await prisma.game.findMany({
    where: { date: new Date(commenceDate + "T00:00:00Z") },
    include: { homeTeam: true, awayTeam: true },
  });

  const homeNorm = normalizeTeamName(event.home_team);
  const awayNorm = normalizeTeamName(event.away_team);

  for (const game of games) {
    const dbHome = normalizeTeamName(game.homeTeam?.name ?? game.homeTeamNameSnapshot ?? "");
    const dbAway = normalizeTeamName(game.awayTeam?.name ?? game.awayTeamNameSnapshot ?? "");

    if (
      (dbHome.includes(homeNorm) || homeNorm.includes(dbHome)) &&
      (dbAway.includes(awayNorm) || awayNorm.includes(dbAway))
    ) {
      return game.id;
    }
  }

  return null;
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

  for (const market of bookmaker.markets) {
    if (market.key === "spreads") {
      for (const o of market.outcomes) {
        if (o.name === event.home_team) spreadHome = o.point ?? null;
        if (o.name === event.away_team) spreadAway = o.point ?? null;
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
        if (o.name === event.home_team) mlHome = o.price;
        if (o.name === event.away_team) mlAway = o.price;
      }
    }
  }

  return { spreadHome, spreadAway, totalOver, totalUnder, mlHome, mlAway };
}

async function processOddsEvents(events: OddsEvent[]): Promise<number> {
  let upserted = 0;

  for (const event of events) {
    const gameId = await matchGameToOdds(event);
    if (!gameId) continue;

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

export async function syncOddsLive(): Promise<number> {
  console.log("Fetching live NBA odds...");
  const events = await fetchNbaOdds();
  console.log(`  Found ${events.length} events with odds`);
  return processOddsEvents(events);
}

export async function syncOddsForDate(date: string): Promise<number> {
  console.log(`Fetching historical odds for ${date}...`);
  const events = await fetchHistoricalOdds(date);
  console.log(`  Found ${events.length} events with odds`);
  return processOddsEvents(events);
}
