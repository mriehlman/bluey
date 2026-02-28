import { RateLimiter } from "./rateLimiter.js";

const BASE_URL = "https://api.balldontlie.io/v1";

// ── Response types ──

export interface BdlTeam {
  id: number;
  conference: string;
  division: string;
  city: string;
  name: string;
  full_name: string;
  abbreviation: string;
}

export interface BdlGame {
  id: number;
  date: string;
  season: number;
  status: string;
  period: number;
  time: string;
  postseason: boolean;
  home_team_score: number;
  visitor_team_score: number;
  home_team: BdlTeam;
  visitor_team: BdlTeam;
}

export interface BdlPlayer {
  id: number;
  first_name: string;
  last_name: string;
  position: string;
  height: string;
  weight: string;
  jersey_number: string;
  college: string;
  country: string;
  draft_year: number | null;
  draft_round: number | null;
  draft_number: number | null;
  team: BdlTeam;
}

export interface BdlStat {
  id: number;
  min: string;
  fgm: number;
  fga: number;
  fg_pct: number;
  fg3m: number;
  fg3a: number;
  fg3_pct: number;
  ftm: number;
  fta: number;
  ft_pct: number;
  oreb: number;
  dreb: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  turnover: number;
  pf: number;
  pts: number;
  player: BdlPlayer;
  team: BdlTeam;
  game: {
    id: number;
    date: string;
    season: number;
    status: string;
    home_team_score: number;
    visitor_team_score: number;
  };
}

interface BdlPaginatedResponse<T> {
  data: T[];
  meta: { next_cursor: number | null; per_page: number };
}

// ── Client internals ──

const limiter = new RateLimiter(30);

function getApiKey(): string {
  const key = process.env.BALLDONTLIE_API_KEY;
  if (!key) throw new Error("BALLDONTLIE_API_KEY environment variable is required");
  return key;
}

async function fetchJson<T>(url: string): Promise<T> {
  await limiter.acquire();
  const res = await fetch(url, { headers: { Authorization: getApiKey() } });
  if (!res.ok) {
    throw new Error(`balldontlie ${res.status} ${res.statusText} — ${url}`);
  }
  return res.json() as Promise<T>;
}

async function fetchAllPages<T>(baseUrl: string, params: URLSearchParams): Promise<T[]> {
  const all: T[] = [];
  let cursor: number | null = null;

  do {
    const p = new URLSearchParams(params);
    if (cursor) p.set("cursor", String(cursor));
    const url = `${baseUrl}?${p.toString()}`;
    const res = await fetchJson<BdlPaginatedResponse<T>>(url);
    all.push(...res.data);
    cursor = res.meta.next_cursor;
  } while (cursor);

  return all;
}

// ── Public API ──

export async function fetchGames(date: string): Promise<BdlGame[]> {
  const params = new URLSearchParams({ "dates[]": date, per_page: "100" });
  return fetchAllPages<BdlGame>(`${BASE_URL}/games`, params);
}

export async function fetchGamesByDateRange(startDate: string, endDate: string): Promise<BdlGame[]> {
  const params = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    per_page: "100",
  });
  return fetchAllPages<BdlGame>(`${BASE_URL}/games`, params);
}

export async function fetchSeasonGames(season: number): Promise<BdlGame[]> {
  const params = new URLSearchParams({ "seasons[]": String(season), per_page: "100" });
  return fetchAllPages<BdlGame>(`${BASE_URL}/games`, params);
}

export async function fetchBoxScores(gameId: number): Promise<BdlStat[]> {
  const params = new URLSearchParams({ "game_ids[]": String(gameId), per_page: "100" });
  return fetchAllPages<BdlStat>(`${BASE_URL}/stats`, params);
}

export async function fetchStatsByDate(date: string): Promise<BdlStat[]> {
  const params = new URLSearchParams({ "dates[]": date, per_page: "100" });
  return fetchAllPages<BdlStat>(`${BASE_URL}/stats`, params);
}
