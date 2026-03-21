import { RateLimiter } from "./rateLimiter";

const BASE_URL = "https://api.the-odds-api.com/v4";
const ODDS_SNAPSHOT_TIME_ET = process.env.ODDS_SNAPSHOT_TIME_ET || "07:30";

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

function parseEasternUtcOffsetMinutes(utcDate: Date): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "shortOffset",
  }).formatToParts(utcDate);
  const tzPart = parts.find((p) => p.type === "timeZoneName")?.value ?? "GMT-5";
  const match = tzPart.match(/^GMT([+-]\d{1,2})(?::?(\d{2}))?$/);
  if (!match) return -5 * 60;
  const signHours = Number(match[1]);
  const minsPart = Number(match[2] ?? "0");
  return signHours * 60 + (signHours >= 0 ? minsPart : -minsPart);
}

export function getHistoricalSnapshotIsoUtc(date: string, timeEt = ODDS_SNAPSHOT_TIME_ET): string {
  const [year, month, day] = date.split("-").map(Number);
  const [hourEt, minuteEt] = timeEt.split(":").map(Number);
  const offsetMinutes = parseEasternUtcOffsetMinutes(new Date(`${date}T12:00:00Z`));
  const utcMs = Date.UTC(year, month - 1, day, hourEt, minuteEt) - offsetMinutes * 60_000;
  return new Date(utcMs).toISOString().replace(".000Z", "Z");
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
    date: getHistoricalSnapshotIsoUtc(date),
  });
  const res = await fetchJson<{ data: OddsEvent[] }>(
    `${BASE_URL}/historical/sports/basketball_nba/odds?${params.toString()}`,
  );
  return res.data;
}

export async function fetchHistoricalEvents(date: string): Promise<Array<{ id: string; home_team: string; away_team: string; commence_time: string }>> {
  const params = new URLSearchParams({
    apiKey: getApiKey(),
    date: getHistoricalSnapshotIsoUtc(date),
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
    date: getHistoricalSnapshotIsoUtc(date),
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
