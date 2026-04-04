import * as fs from "fs";
import * as path from "path";
import { getDataDir } from "../config/paths";

type JsonValue = string | number | boolean | null | JsonValue[] | { [k: string]: JsonValue };

interface RawGameFile {
  gameId?: string;
  date?: string;
  [k: string]: JsonValue | undefined;
}

interface DayPropsBundle {
  date?: string;
  generatedAt?: string;
  source?: string;
  eventCount?: number;
  events?: JsonValue[];
}

interface DayDataBundle {
  date: string;
  generatedAt: string;
  source: "day-bundle-v1";
  coverage: {
    games: { count: number; seasons: string[] };
    oddsHistorical: { available: boolean; eventCount: number; sameDayEventCount: number };
    oddsLive: { available: boolean; eventCount: number };
    playerProps: { available: boolean; eventCount: number; source: string };
    injuries: {
      source: string;
      earlyAvailable: boolean;
      earlyRowCount: number;
      finalAvailable: boolean;
      finalRowCount: number;
    };
  };
  games: Array<{ season: string; file: string; data: RawGameFile }>;
  odds: {
    historical: JsonValue[];
    historicalSameDay: JsonValue[];
    live: JsonValue[];
  };
  playerProps: {
    source: string;
    eventCount: number;
    events: JsonValue[];
  };
  injuries: {
    source: string;
    early: JsonValue | null;
    final: JsonValue | null;
  };
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

function isDateString(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function listDateFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .map((f) => f.replace(/\.json$/i, ""))
    .filter(isDateString)
    .sort();
}

function listDateDirs(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && isDateString(d.name))
    .map((d) => d.name)
    .sort();
}

function dateInRange(date: string, from?: string, to?: string): boolean {
  if (from && date < from) return false;
  if (to && date > to) return false;
  return true;
}

function extractDateFromCommence(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const d = new Date(value);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function readJsonFile<T>(filePath: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf-8")) as T;
  } catch {
    return null;
  }
}

function loadPlayerPropsEvents(dataDir: string, date: string): { source: string; events: JsonValue[] } {
  const dayPath = path.join(dataDir, "raw", "odds", "player-props-day", `${date}.json`);
  const dayBundle = readJsonFile<DayPropsBundle>(dayPath);
  if (dayBundle?.events && Array.isArray(dayBundle.events)) {
    return { source: dayBundle.source || "player-props-day-bundle-v1", events: dayBundle.events };
  }

  const rawDir = path.join(dataDir, "raw", "odds", "player-props", date);
  if (!fs.existsSync(rawDir)) return { source: "none", events: [] };

  const files = fs.readdirSync(rawDir).filter((f) => f.endsWith(".json")).sort();
  const events: JsonValue[] = [];
  for (const file of files) {
    const parsed = readJsonFile<JsonValue>(path.join(rawDir, file));
    if (parsed) events.push(parsed);
  }
  return { source: "raw-player-props-events", events };
}

function loadOddsFileEvents(filePath: string): JsonValue[] {
  const parsed = readJsonFile<JsonValue>(filePath);
  if (!parsed) return [];
  return Array.isArray(parsed) ? parsed : [];
}

function listInjurySnapshotDates(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const dates = new Set<string>();
  for (const f of fs.readdirSync(dir)) {
    const m = f.match(/^(\d{4}-\d{2}-\d{2})\.(?:early|final|custom|t\d{4})\.json$/);
    if (m) dates.add(m[1]);
  }
  return Array.from(dates).sort();
}

function countRowsInInjuryPayload(payload: JsonValue | null): number {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return 0;
  const rows = (payload as { rows?: unknown }).rows;
  return Array.isArray(rows) ? rows.length : 0;
}

function loadInjurySnapshots(dataDir: string, date: string): {
  source: string;
  early: JsonValue | null;
  final: JsonValue | null;
} {
  const injuriesDir = path.join(dataDir, "raw", "injuries");
  const earlyPath = path.join(injuriesDir, `${date}.early.json`);
  const finalPath = path.join(injuriesDir, `${date}.final.json`);
  const early = fs.existsSync(earlyPath) ? readJsonFile<JsonValue>(earlyPath) : null;
  const final = fs.existsSync(finalPath) ? readJsonFile<JsonValue>(finalPath) : null;
  return {
    source: "raw-injuries-snapshots-v1",
    early,
    final,
  };
}

export async function buildDailyDataBundles(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const dataDir = getDataDir();

  const seasonsRoot = path.join(dataDir, "raw", "seasons");
  const historicalOddsDir = path.join(dataDir, "raw", "odds", "historical");
  const liveOddsDir = path.join(dataDir, "raw", "odds", "live");
  const propsDayDir = path.join(dataDir, "raw", "odds", "player-props-day");
  const propsRawDir = path.join(dataDir, "raw", "odds", "player-props");
  const injuriesDir = path.join(dataDir, "raw", "injuries");
  const outDir = path.join(dataDir, "raw", "day");
  fs.mkdirSync(outDir, { recursive: true });

  const from = flags.from;
  const to = flags.to;

  const gamesByDate = new Map<string, Array<{ season: string; file: string; data: RawGameFile }>>();
  const allDates = new Set<string>();

  if (fs.existsSync(seasonsRoot)) {
    const seasons = fs
      .readdirSync(seasonsRoot, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name)
      .sort();

    for (const season of seasons) {
      const seasonDir = path.join(seasonsRoot, season);
      const files = fs
        .readdirSync(seasonDir)
        .filter((f) => f.endsWith(".json") && !f.startsWith("_"))
        .sort();

      for (const file of files) {
        const parsed = readJsonFile<RawGameFile>(path.join(seasonDir, file));
        const gameDate = parsed?.date;
        if (!gameDate || !isDateString(gameDate)) continue;
        if (!dateInRange(gameDate, from, to)) continue;

        if (!gamesByDate.has(gameDate)) gamesByDate.set(gameDate, []);
        gamesByDate.get(gameDate)!.push({ season, file, data: parsed });
        allDates.add(gameDate);
      }
    }
  }

  for (const d of listDateFiles(historicalOddsDir)) {
    if (dateInRange(d, from, to)) allDates.add(d);
  }
  for (const d of listDateFiles(liveOddsDir)) {
    if (dateInRange(d, from, to)) allDates.add(d);
  }
  for (const d of listDateFiles(propsDayDir)) {
    if (dateInRange(d, from, to)) allDates.add(d);
  }
  for (const d of listDateDirs(propsRawDir)) {
    if (dateInRange(d, from, to)) allDates.add(d);
  }
  for (const d of listInjurySnapshotDates(injuriesDir)) {
    if (dateInRange(d, from, to)) allDates.add(d);
  }

  const dates = Array.from(allDates).sort();
  console.log(`Building day bundles in ${outDir}`);
  console.log(`Dates to build: ${dates.length}`);

  for (const date of dates) {
    const games = gamesByDate.get(date) ?? [];

    const historicalPath = path.join(historicalOddsDir, `${date}.json`);
    const historical = fs.existsSync(historicalPath) ? loadOddsFileEvents(historicalPath) : [];
    const historicalSameDay = historical.filter((e) => {
      if (!e || typeof e !== "object") return false;
      const obj = e as Record<string, unknown>;
      const eventDate = extractDateFromCommence(obj.commence_time);
      return eventDate === date;
    });

    const livePath = path.join(liveOddsDir, `${date}.json`);
    const live = fs.existsSync(livePath) ? loadOddsFileEvents(livePath) : [];

    const props = loadPlayerPropsEvents(dataDir, date);
    const injuries = loadInjurySnapshots(dataDir, date);
    const injuryEarlyRows = countRowsInInjuryPayload(injuries.early);
    const injuryFinalRows = countRowsInInjuryPayload(injuries.final);

    const seasons = Array.from(new Set(games.map((g) => g.season))).sort();

    const bundle: DayDataBundle = {
      date,
      generatedAt: new Date().toISOString(),
      source: "day-bundle-v1",
      coverage: {
        games: { count: games.length, seasons },
        oddsHistorical: {
          available: historical.length > 0,
          eventCount: historical.length,
          sameDayEventCount: historicalSameDay.length,
        },
        oddsLive: {
          available: live.length > 0,
          eventCount: live.length,
        },
        playerProps: {
          available: props.events.length > 0,
          eventCount: props.events.length,
          source: props.source,
        },
        injuries: {
          source: injuries.source,
          earlyAvailable: injuries.early != null,
          earlyRowCount: injuryEarlyRows,
          finalAvailable: injuries.final != null,
          finalRowCount: injuryFinalRows,
        },
      },
      games,
      odds: {
        historical,
        historicalSameDay,
        live,
      },
      playerProps: {
        source: props.source,
        eventCount: props.events.length,
        events: props.events,
      },
      injuries,
    };

    const outPath = path.join(outDir, `${date}.json`);
    fs.writeFileSync(outPath, JSON.stringify(bundle, null, 2));
  }

  console.log(`Done. Wrote ${dates.length} day files.`);
}
