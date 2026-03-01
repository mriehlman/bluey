import { RateLimiter } from "./rateLimiter.js";

const BASE_URL = "https://api.the-odds-api.com/v4";

// ── Response types ──

export interface OddsOutcome {
  name: string;
  price: number;
  point?: number;
}

export interface OddsMarket {
  key: string;
  last_update: string;
  outcomes: OddsOutcome[];
}

export interface OddsBookmaker {
  key: string;
  title: string;
  last_update: string;
  markets: OddsMarket[];
}

export interface OddsEvent {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: OddsBookmaker[];
}

// ── Client internals ──

const limiter = new RateLimiter(300); // allow up to 300 requests/minute for fast backfill

function getApiKey(): string {
  const key = process.env.ODDS_API_KEY;
  if (!key) throw new Error("ODDS_API_KEY environment variable is required");
  return key;
}

async function fetchJson<T>(url: string): Promise<T> {
  await limiter.acquire();
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Odds API ${res.status} ${res.statusText} — ${url}`);
  }
  return res.json() as Promise<T>;
}

// ── Public API ──

export async function fetchNbaOdds(): Promise<OddsEvent[]> {
  const params = new URLSearchParams({
    apiKey: getApiKey(),
    regions: "us",
    markets: "spreads,totals,h2h",
    oddsFormat: "american",
  });
  return fetchJson<OddsEvent[]>(
    `${BASE_URL}/sports/basketball_nba/odds?${params.toString()}`,
  );
}

export async function fetchHistoricalOdds(date: string): Promise<OddsEvent[]> {
  const params = new URLSearchParams({
    apiKey: getApiKey(),
    regions: "us",
    markets: "spreads,totals,h2h",
    oddsFormat: "american",
    date: `${date}T12:00:00Z`,
  });
  const res = await fetchJson<{ data: OddsEvent[] }>(
    `${BASE_URL}/historical/sports/basketball_nba/odds?${params.toString()}`,
  );
  return res.data;
}

export async function fetchHistoricalEvents(date: string): Promise<Array<{ id: string; home_team: string; away_team: string; commence_time: string }>> {
  const params = new URLSearchParams({
    apiKey: getApiKey(),
    date: `${date}T12:00:00Z`,
  });
  const res = await fetchJson<{ data: Array<{ id: string; home_team: string; away_team: string; commence_time: string }> }>(
    `${BASE_URL}/historical/sports/basketball_nba/events?${params.toString()}`,
  );
  return res.data;
}

export async function fetchHistoricalEventOdds(
  eventId: string,
  date: string,
  markets: string,
): Promise<OddsEvent | null> {
  const params = new URLSearchParams({
    apiKey: getApiKey(),
    regions: "us",
    markets,
    oddsFormat: "american",
    date: `${date}T12:00:00Z`,
  });
  try {
    const res = await fetchJson<{ data: OddsEvent }>(
      `${BASE_URL}/historical/sports/basketball_nba/events/${eventId}/odds?${params.toString()}`,
    );
    return res.data;
  } catch {
    return null;
  }
}
