import * as fs from "fs/promises";
import * as fssync from "fs";
import * as path from "path";
import { createHash } from "crypto";
import { prisma } from "../db/prisma.js";
import { dateStringToUtcMidday } from "./utils.js";

type JsonObject = Record<string, unknown>;

type DayBundleGame = {
  season: string;
  file: string;
  data: {
    gameId?: string;
    date?: string;
    homeTeamId?: number;
    awayTeamId?: number;
    homeScore?: number;
    awayScore?: number;
    status?: string;
    period?: number;
    gameCode?: string;
    gameTimeUTC?: string;
    isNeutral?: boolean;
    seriesText?: string;
    poRoundDesc?: string;
    homeWins?: number;
    homeLosses?: number;
    awayWins?: number;
    awayLosses?: number;
    broadcasts?: unknown;
    playerStats?: Array<{
      personId?: number;
      firstName?: string;
      familyName?: string;
      playerSlug?: string;
      jerseyNum?: string;
      teamId?: number;
      position?: string;
      starter?: boolean;
      comment?: string;
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
    }>;
  };
};

type OddsEvent = {
  id?: string;
  home_team?: string;
  away_team?: string;
  commence_time?: string;
  bookmakers?: Array<{
    key?: string;
    markets?: Array<{
      key?: string;
      outcomes?: Array<{ name?: string; price?: number; point?: number; description?: string }>;
    }>;
  }>;
};

type DayBundle = {
  date: string;
  games?: DayBundleGame[];
  odds?: {
    historicalSameDay?: OddsEvent[];
    live?: OddsEvent[];
  };
  playerProps?: {
    events?: OddsEvent[];
  };
};

const NBA_TO_INTERNAL_TEAM: Record<number, number> = {
  1610612737: 1,
  1610612738: 2,
  1610612751: 3,
  1610612766: 4,
  1610612741: 5,
  1610612739: 6,
  1610612742: 7,
  1610612743: 8,
  1610612765: 9,
  1610612744: 10,
  1610612745: 11,
  1610612754: 12,
  1610612746: 13,
  1610612747: 14,
  1610612763: 15,
  1610612748: 16,
  1610612749: 17,
  1610612750: 18,
  1610612740: 19,
  1610612752: 20,
  1610612760: 21,
  1610612753: 22,
  1610612755: 23,
  1610612756: 24,
  1610612757: 25,
  1610612758: 26,
  1610612759: 27,
  1610612761: 28,
  1610612762: 29,
  1610612764: 30,
};

const TEAM_NAME_TO_ID: Record<string, number> = {
  "atlanta hawks": 1,
  "boston celtics": 2,
  "brooklyn nets": 3,
  "charlotte hornets": 4,
  "chicago bulls": 5,
  "cleveland cavaliers": 6,
  "dallas mavericks": 7,
  "denver nuggets": 8,
  "detroit pistons": 9,
  "golden state warriors": 10,
  "houston rockets": 11,
  "indiana pacers": 12,
  "los angeles clippers": 13,
  "la clippers": 13,
  "los angeles lakers": 14,
  "memphis grizzlies": 15,
  "miami heat": 16,
  "milwaukee bucks": 17,
  "minnesota timberwolves": 18,
  "new orleans pelicans": 19,
  "new york knicks": 20,
  "oklahoma city thunder": 21,
  "orlando magic": 22,
  "philadelphia 76ers": 23,
  "phoenix suns": 24,
  "portland trail blazers": 25,
  "sacramento kings": 26,
  "san antonio spurs": 27,
  "toronto raptors": 28,
  "utah jazz": 29,
  "washington wizards": 30,
};

const TEAM_DIM: Array<{ id: number; name: string; code: string; city: string }> = [
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

const TEAM_CANONICAL: Record<string, string> = {
  atlantahawks: "hawks",
  hawks: "hawks",
  bostonceltics: "celtics",
  celtics: "celtics",
  brooklynnets: "nets",
  nets: "nets",
  charlottehornets: "hornets",
  hornets: "hornets",
  chicagobulls: "bulls",
  bulls: "bulls",
  clevelandcavaliers: "cavaliers",
  cavaliers: "cavaliers",
  dallasmavericks: "mavericks",
  mavericks: "mavericks",
  denvernuggets: "nuggets",
  nuggets: "nuggets",
  detroitpistons: "pistons",
  pistons: "pistons",
  goldenstatewarriors: "warriors",
  warriors: "warriors",
  houstonrockets: "rockets",
  rockets: "rockets",
  indianapacers: "pacers",
  pacers: "pacers",
  losangelesclippers: "clippers",
  laclippers: "clippers",
  clippers: "clippers",
  losangeleslakers: "lakers",
  lakers: "lakers",
  memphisgrizzlies: "grizzlies",
  grizzlies: "grizzlies",
  miamiheat: "heat",
  heat: "heat",
  milwaukeebucks: "bucks",
  bucks: "bucks",
  minnesotatimberwolves: "timberwolves",
  wolves: "timberwolves",
  neworleanspelicans: "pelicans",
  pelicans: "pelicans",
  newyorkknicks: "knicks",
  knicks: "knicks",
  oklahomacitythunder: "thunder",
  thunder: "thunder",
  orlandomagic: "magic",
  magic: "magic",
  philadelphia76ers: "76ers",
  sixers: "76ers",
  phoenixsuns: "suns",
  suns: "suns",
  portlandtrailblazers: "blazers",
  trailblazers: "blazers",
  blazers: "blazers",
  sacramentokings: "kings",
  kings: "kings",
  sanantoniospurs: "spurs",
  spurs: "spurs",
  torontoraptors: "raptors",
  raptors: "raptors",
  utahjazz: "jazz",
  jazz: "jazz",
  washingtonwizards: "wizards",
  wizards: "wizards",
};

function normalizeName(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim().replace(/\s+/g, " ");
}

function canonicalTeamName(name: string): string {
  const key = name.toLowerCase().replace(/[^a-z]/g, "");
  if (TEAM_CANONICAL[key]) return TEAM_CANONICAL[key];
  return key;
}

function gameMatchKey(date: string, home: string, away: string): string {
  return `${date}|${canonicalTeamName(home)}|${canonicalTeamName(away)}`;
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

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function parseMinutesToInt(raw?: string): { minutes: number; minutesRaw: string | null } {
  if (!raw) return { minutes: 0, minutesRaw: null };
  const s = String(raw).trim();
  if (!s) return { minutes: 0, minutesRaw: null };
  const m = s.match(/^(\d+):(\d{1,2})$/);
  if (m) return { minutes: Number(m[1]), minutesRaw: s };
  const n = Number(s);
  if (Number.isFinite(n) && n >= 0) return { minutes: Math.floor(n), minutesRaw: s };
  return { minutes: 0, minutesRaw: s };
}

function hashString(s: string): number {
  let hash = 0;
  for (let i = 0; i < s.length; i++) {
    hash = (hash << 5) - hash + s.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

async function ensureIngestDayTable(): Promise<void> {
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "IngestDay" (
      "id" TEXT PRIMARY KEY,
      "date" DATE NOT NULL UNIQUE,
      "status" TEXT NOT NULL,
      "checksum" TEXT,
      "sourceFile" TEXT,
      "startedAt" TIMESTAMP(3),
      "finishedAt" TIMESTAMP(3),
      "durationMs" INTEGER,
      "error" TEXT,
      "metrics" JSONB,
      "createdAt" TIMESTAMP(3) NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "IngestDay_status_idx" ON "IngestDay" ("status")`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "IngestDay_date_idx" ON "IngestDay" ("date")`);
}

async function ensureTeams(): Promise<void> {
  for (const team of TEAM_DIM) {
    await prisma.team.upsert({
      where: { id: team.id },
      update: { name: team.name, code: team.code, city: team.city },
      create: team,
    });
  }
}

function cuidLike(): string {
  return `ing_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

async function setIngestDayRunning(date: string, checksum: string, sourceFile: string): Promise<void> {
  await prisma.$executeRawUnsafe(
    `
    INSERT INTO "IngestDay" ("id","date","status","checksum","sourceFile","startedAt","updatedAt")
    VALUES ('${cuidLike()}', '${date}', 'running', '${checksum}', '${sourceFile.replace(/'/g, "''")}', NOW(), NOW())
    ON CONFLICT ("date") DO UPDATE SET
      "status" = 'running',
      "checksum" = EXCLUDED."checksum",
      "sourceFile" = EXCLUDED."sourceFile",
      "startedAt" = NOW(),
      "finishedAt" = NULL,
      "durationMs" = NULL,
      "error" = NULL,
      "metrics" = NULL,
      "updatedAt" = NOW();
  `,
  );
}

async function setIngestDayDone(date: string, durationMs: number, metrics: JsonObject): Promise<void> {
  const metricsStr = JSON.stringify(metrics).replace(/'/g, "''");
  await prisma.$executeRawUnsafe(
    `
    UPDATE "IngestDay"
    SET "status"='done',
        "finishedAt"=NOW(),
        "durationMs"=${durationMs},
        "metrics"='${metricsStr}'::jsonb,
        "updatedAt"=NOW()
    WHERE "date"='${date}';
  `,
  );
}

async function setIngestDayFailed(date: string, durationMs: number, err: string): Promise<void> {
  const safeErr = err.replace(/'/g, "''");
  await prisma.$executeRawUnsafe(
    `
    UPDATE "IngestDay"
    SET "status"='failed',
        "finishedAt"=NOW(),
        "durationMs"=${durationMs},
        "error"='${safeErr}',
        "updatedAt"=NOW()
    WHERE "date"='${date}';
  `,
  );
}

async function getExistingIngestDay(date: string): Promise<{ status: string; checksum: string | null } | null> {
  const rows = await prisma.$queryRawUnsafe<Array<{ status: string; checksum: string | null }>>(
    `SELECT "status","checksum" FROM "IngestDay" WHERE "date"='${date}' LIMIT 1`,
  );
  return rows[0] ?? null;
}

async function upsertGameFromBundle(game: DayBundleGame): Promise<string | null> {
  const g = game.data;
  if (!g.gameId || !g.date || g.homeTeamId == null || g.awayTeamId == null) return null;

  const homeTeamId = NBA_TO_INTERNAL_TEAM[g.homeTeamId];
  const awayTeamId = NBA_TO_INTERNAL_TEAM[g.awayTeamId];
  if (!homeTeamId || !awayTeamId) return null;

  const seasonYear = Number(String(game.season).slice(0, 4));
  const gameDate = dateStringToUtcMidday(g.date);
  const sourceGameId = Number.parseInt(g.gameId, 10) || hashString(g.gameId);

  const gameRow = await prisma.game.upsert({
    where: { nbaGameId: g.gameId },
    create: {
      sourceGameId,
      nbaGameId: g.gameId,
      date: gameDate,
      season: Number.isFinite(seasonYear) ? seasonYear : Number(new Date(g.date).getUTCFullYear()),
      stage: g.gameId.startsWith("004") ? 4 : g.gameId.startsWith("001") ? 1 : 2,
      league: "NBA",
      homeTeamId,
      awayTeamId,
      homeScore: g.homeScore ?? 0,
      awayScore: g.awayScore ?? 0,
      status: g.status ?? null,
      periods: g.period ?? null,
      gameCode: g.gameCode ?? null,
      tipoffTimeUtc: g.gameTimeUTC ? new Date(g.gameTimeUTC) : null,
      isNeutralSite: g.isNeutral ?? false,
      seriesText: g.seriesText ?? null,
      poRoundDesc: g.poRoundDesc ?? null,
      homeWinsPreGame: g.homeWins ?? null,
      homeLossesPreGame: g.homeLosses ?? null,
      awayWinsPreGame: g.awayWins ?? null,
      awayLossesPreGame: g.awayLosses ?? null,
      broadcasts: (g.broadcasts as object) ?? undefined,
    },
    update: {
      date: gameDate,
      season: Number.isFinite(seasonYear) ? seasonYear : Number(new Date(g.date).getUTCFullYear()),
      homeTeamId,
      awayTeamId,
      homeScore: g.homeScore ?? 0,
      awayScore: g.awayScore ?? 0,
      status: g.status ?? null,
      periods: g.period ?? null,
      gameCode: g.gameCode ?? null,
      tipoffTimeUtc: g.gameTimeUTC ? new Date(g.gameTimeUTC) : null,
      isNeutralSite: g.isNeutral ?? false,
      seriesText: g.seriesText ?? null,
      poRoundDesc: g.poRoundDesc ?? null,
      homeWinsPreGame: g.homeWins ?? null,
      homeLossesPreGame: g.homeLosses ?? null,
      awayWinsPreGame: g.awayWins ?? null,
      awayLossesPreGame: g.awayLosses ?? null,
      broadcasts: (g.broadcasts as object) ?? undefined,
    },
    select: { id: true },
  });

  return gameRow.id;
}

function outcomeKey(event: OddsEvent): string {
  return `${event.id ?? "unknown"}|${event.home_team ?? ""}|${event.away_team ?? ""}|${event.commence_time ?? ""}`;
}

async function ingestDayBundle(filePath: string, force = false): Promise<void> {
  const startedAt = Date.now();
  const raw = await fs.readFile(filePath, "utf-8");
  const checksum = createHash("sha256").update(raw).digest("hex");
  const bundle = JSON.parse(raw) as DayBundle;
  const date = bundle.date;

  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error(`Invalid day bundle date in ${filePath}`);
  }

  const existing = await getExistingIngestDay(date);
  if (!force && existing?.status === "done" && existing?.checksum === checksum) {
    console.log(`Skipping ${date} (already ingested, checksum match).`);
    return;
  }

  await setIngestDayRunning(date, checksum, path.basename(filePath));

  try {
    const gameIdByMatch = new Map<string, string>();
    const playersById = new Map<number, { firstname: string | null; lastname: string | null; slug: string | null; jerseyNum: string | null }>();
    const statsByGame = new Map<string, Array<Record<string, unknown>>>();

    const games = bundle.games ?? [];
    for (const game of games) {
      const dbGameId = await upsertGameFromBundle(game);
      if (!dbGameId) continue;

      const g = game.data;
      const rows = game.data.playerStats ?? [];
      for (const row of rows) {
        if (!row.personId || !row.teamId) continue;
        const playerId = row.personId;
        playersById.set(playerId, {
          firstname: row.firstName ?? null,
          lastname: row.familyName ?? null,
          slug: row.playerSlug ?? null,
          jerseyNum: row.jerseyNum ?? null,
        });

        const teamId = NBA_TO_INTERNAL_TEAM[row.teamId];
        if (!teamId) continue;
        const { minutes, minutesRaw } = parseMinutesToInt(row.minutes);
        if (!statsByGame.has(dbGameId)) statsByGame.set(dbGameId, []);
        statsByGame.get(dbGameId)!.push({
          gameId: dbGameId,
          playerId,
          teamId,
          minutes,
          minutesRaw,
          points: row.points ?? 0,
          assists: row.assists ?? 0,
          rebounds: row.reboundsTotal ?? 0,
          steals: row.steals ?? 0,
          blocks: row.blocks ?? 0,
          turnovers: row.turnovers ?? 0,
          fgm: row.fieldGoalsMade ?? null,
          fga: row.fieldGoalsAttempted ?? null,
          fgPct: row.fieldGoalsPercentage ?? null,
          fg3m: row.threePointersMade ?? null,
          fg3a: row.threePointersAttempted ?? null,
          fg3Pct: row.threePointersPercentage ?? null,
          ftm: row.freeThrowsMade ?? null,
          fta: row.freeThrowsAttempted ?? null,
          ftPct: row.freeThrowsPercentage ?? null,
          oreb: row.reboundsOffensive ?? null,
          dreb: row.reboundsDefensive ?? null,
          pf: row.foulsPersonal ?? null,
          plusMinus: row.plusMinusPoints ?? null,
          position: row.position ?? null,
          starter: row.starter ?? null,
          comment: row.comment ?? null,
        });
      }
    }

    if (playersById.size > 0) {
      await prisma.player.createMany({
        data: Array.from(playersById.entries()).map(([id, p]) => ({
          id,
          firstname: p.firstname,
          lastname: p.lastname,
          slug: p.slug,
          jerseyNum: p.jerseyNum,
        })),
        skipDuplicates: true,
      });
    }

    for (const [gameId, stats] of statsByGame.entries()) {
      await prisma.playerGameStat.deleteMany({ where: { gameId } });
      if (stats.length > 0) {
        await prisma.playerGameStat.createMany({
          data: stats as never,
          skipDuplicates: true,
        });
      }
    }

    const dbGames = await prisma.game.findMany({
      where: { date: dateStringToUtcMidday(date) },
      select: {
        id: true,
        homeTeam: { select: { name: true, code: true } },
        awayTeam: { select: { name: true, code: true } },
      },
    });
    for (const g of dbGames) {
      const home = g.homeTeam?.name ?? g.homeTeam?.code ?? "";
      const away = g.awayTeam?.name ?? g.awayTeam?.code ?? "";
      gameIdByMatch.set(gameMatchKey(date, home, away), g.id);
    }

    const oddsEvents = [...(bundle.odds?.historicalSameDay ?? []), ...(bundle.odds?.live ?? [])];
    const dedupOdds = new Map<string, OddsEvent>();
    for (const e of oddsEvents) dedupOdds.set(outcomeKey(e), e);

    const oddsRowsByGame = new Map<string, Array<Record<string, unknown>>>();
    for (const event of dedupOdds.values()) {
      const home = event.home_team ?? "";
      const away = event.away_team ?? "";
      const key = gameMatchKey(date, home, away);
      const gameId = gameIdByMatch.get(key);
      if (!gameId) continue;

      for (const book of event.bookmakers ?? []) {
        if (!book.key) continue;
        let spreadHome: number | null = null;
        let spreadAway: number | null = null;
        let totalOver: number | null = null;
        let totalUnder: number | null = null;
        let mlHome: number | null = null;
        let mlAway: number | null = null;

        for (const market of book.markets ?? []) {
          if (market.key === "spreads") {
            for (const o of market.outcomes ?? []) {
              if (!o.name) continue;
              const oc = canonicalTeamName(o.name);
              const hc = canonicalTeamName(home);
              const ac = canonicalTeamName(away);
              if (oc === hc) spreadHome = o.point ?? null;
              if (oc === ac) spreadAway = o.point ?? null;
            }
          } else if (market.key === "totals") {
            for (const o of market.outcomes ?? []) {
              if (o.name === "Over") totalOver = o.point ?? null;
              if (o.name === "Under") totalUnder = o.point ?? null;
            }
          } else if (market.key === "h2h") {
            for (const o of market.outcomes ?? []) {
              if (!o.name) continue;
              const oc = canonicalTeamName(o.name);
              const hc = canonicalTeamName(home);
              const ac = canonicalTeamName(away);
              if (oc === hc) mlHome = o.price ?? null;
              if (oc === ac) mlAway = o.price ?? null;
            }
          }
        }

        if (!oddsRowsByGame.has(gameId)) oddsRowsByGame.set(gameId, []);
        oddsRowsByGame.get(gameId)!.push({
          gameId,
          source: book.key,
          spreadHome,
          spreadAway,
          totalOver,
          totalUnder,
          mlHome,
          mlAway,
          fetchedAt: new Date(),
        });
      }
    }

    for (const [gameId, rows] of oddsRowsByGame.entries()) {
      await prisma.gameOdds.deleteMany({ where: { gameId } });
      if (rows.length > 0) {
        await prisma.gameOdds.createMany({ data: rows as never });
      }
    }

    const allPlayers = await prisma.player.findMany({
      select: { id: true, firstname: true, lastname: true },
    });
    const playerNameToId = new Map<string, number>();
    for (const p of allPlayers) {
      const full = normalizeName(`${p.firstname ?? ""} ${p.lastname ?? ""}`);
      if (full) playerNameToId.set(full, p.id);
      const trimmed = full.replace(/\s(jr|sr|ii|iii|iv)$/i, "");
      if (trimmed && !playerNameToId.has(trimmed)) playerNameToId.set(trimmed, p.id);
    }

    const propRowsByGame = new Map<string, Array<Record<string, unknown>>>();
    for (const event of bundle.playerProps?.events ?? []) {
      const home = event.home_team ?? "";
      const away = event.away_team ?? "";
      const gameId = gameIdByMatch.get(gameMatchKey(date, home, away));
      if (!gameId) continue;

      for (const book of event.bookmakers ?? []) {
        if (!book.key) continue;
        for (const market of book.markets ?? []) {
          if (!market.key) continue;
          const byPlayer = new Map<string, { over?: { price?: number; point?: number }; under?: { price?: number; point?: number } }>();
          for (const o of market.outcomes ?? []) {
            if (!o.description) continue;
            const key = normalizeName(o.description);
            const cur = byPlayer.get(key) ?? {};
            if (o.name === "Over" || o.name === "Yes") cur.over = { price: o.price, point: o.point };
            if (o.name === "Under" || o.name === "No") cur.under = { price: o.price, point: o.point };
            byPlayer.set(key, cur);
          }

          for (const [playerNameKey, v] of byPlayer.entries()) {
            const playerId = playerNameToId.get(playerNameKey);
            if (!playerId) continue;
            const line = v.over?.point ?? v.under?.point ?? null;
            if (line == null || !Number.isFinite(line)) continue;
            if (!propRowsByGame.has(gameId)) propRowsByGame.set(gameId, []);
            propRowsByGame.get(gameId)!.push({
              gameId,
              playerId,
              source: book.key,
              market: market.key,
              line,
              overPrice: v.over?.price ?? null,
              underPrice: v.under?.price ?? null,
              fetchedAt: new Date(),
            });
          }
        }
      }
    }

    for (const [gameId, rows] of propRowsByGame.entries()) {
      await prisma.playerPropOdds.deleteMany({ where: { gameId } });
      if (rows.length > 0) {
        await prisma.playerPropOdds.createMany({ data: rows as never, skipDuplicates: true });
      }
    }

    const durationMs = Date.now() - startedAt;
    await setIngestDayDone(date, durationMs, {
      gamesInBundle: games.length,
      gamesUpserted: gameIdByMatch.size,
      playersSeen: playersById.size,
      statsRows: Array.from(statsByGame.values()).reduce((acc, rows) => acc + rows.length, 0),
      gameOddsRows: Array.from(oddsRowsByGame.values()).reduce((acc, rows) => acc + rows.length, 0),
      propOddsRows: Array.from(propRowsByGame.values()).reduce((acc, rows) => acc + rows.length, 0),
      sourceFile: path.basename(filePath),
    });
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    await setIngestDayFailed(date, durationMs, (err as Error).message);
    throw err;
  }
}

async function runWithConcurrency<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
  const queue = [...items];
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (queue.length > 0) {
      const next = queue.shift();
      if (next == null) return;
      await fn(next);
    }
  });
  await Promise.all(workers);
}

export async function ingestDayBundles(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const force = (flags.force ?? "false") === "true";
  const from = flags.from;
  const to = flags.to;
  const date = flags.date;
  const concurrency = Math.max(1, Number(flags.concurrency ?? "2"));

  const dayDir = path.join(getDataDir(), "raw", "day");
  if (!fssync.existsSync(dayDir)) {
    throw new Error(`Day bundle directory not found: ${dayDir}`);
  }

  await ensureIngestDayTable();
  await ensureTeams();

  const allDates = (await fs.readdir(dayDir))
    .filter((f) => /^\d{4}-\d{2}-\d{2}\.json$/.test(f))
    .map((f) => f.replace(".json", ""))
    .sort();

  let targets = allDates;
  if (date) targets = allDates.filter((d) => d === date);
  if (from) targets = targets.filter((d) => d >= from);
  if (to) targets = targets.filter((d) => d <= to);

  if (targets.length === 0) {
    console.log("No day bundle files matched requested range.");
    return;
  }

  console.log(`\n=== Ingest Day Bundles ===`);
  console.log(`Source: ${dayDir}`);
  console.log(`Dates: ${targets[0]} -> ${targets[targets.length - 1]} (${targets.length} files)`);
  console.log(`Concurrency: ${concurrency}`);
  console.log(`Force: ${force}`);

  let done = 0;
  await runWithConcurrency(targets, concurrency, async (d) => {
    const filePath = path.join(dayDir, `${d}.json`);
    await ingestDayBundle(filePath, force);
    done++;
    if (done % 25 === 0 || done === targets.length) {
      console.log(`Progress: ${done}/${targets.length} day bundles ingested`);
    }
  });

  console.log(`\nDone. Ingested ${targets.length} day bundle files.`);
}
