import * as fs from "fs/promises";
import * as path from "path";
import { fetchInjuryReportForDate } from "../api/nbaInjuries";
import { getDataDir } from "../config/paths";

type SnapshotSpec = { label: string; timeEt: string };

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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Returns the set of dates (YYYY-MM-DD) that have at least one game in raw seasons data.
 * Used to avoid fetching injury reports on days with no games (which often return 403).
 * If seasons dir is missing or empty, returns empty set (caller may then fetch all dates with a warning).
 */
async function getDatesWithGames(
  dataDir: string,
  from?: string,
  to?: string,
): Promise<Set<string>> {
  const seasonsRoot = path.join(dataDir, "raw", "seasons");
  try {
    await fs.access(seasonsRoot);
  } catch {
    return new Set();
  }

  const datesWithGames = new Set<string>();
  const seasonDirs = await fs.readdir(seasonsRoot, { withFileTypes: true });
  const seasons = seasonDirs.filter((d) => d.isDirectory()).map((d) => d.name);

  for (const season of seasons) {
    const seasonPath = path.join(seasonsRoot, season);
    const files = await fs.readdir(seasonPath);
    const jsonFiles = files.filter((f) => f.endsWith(".json") && !f.startsWith("_"));

    for (const file of jsonFiles) {
      const filePath = path.join(seasonPath, file);
      try {
        const content = await fs.readFile(filePath, "utf8");
        const parsed = JSON.parse(content) as { date?: string };
        const gameDate = parsed?.date;
        if (typeof gameDate !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(gameDate)) continue;
        if (from != null && gameDate < from) continue;
        if (to != null && gameDate > to) continue;
        datesWithGames.add(gameDate);
      } catch {
        // ignore unreadable or invalid game files
      }
    }
  }

  return datesWithGames;
}

/** NBA season start year: Oct–Dec = same year, Jan–Sep = previous calendar year. */
function getSeasonStartYear(dateStr: string): number {
  const [y, m] = dateStr.split("-").map(Number);
  return m >= 10 ? y : y - 1;
}

type InjuryTask = { date: string; snapshot: SnapshotSpec };

/**
 * Partition task indices into 3 runners by season (2 seasons each):
 * Runner 0: 2021, 2022  |  Runner 1: 2023, 2024  |  Runner 2: 2025, 2026
 * Older seasons go to runner 0; future seasons go to runner 2.
 */
function partitionTasksBySeason(
  tasks: InjuryTask[],
): [InjuryTask[], InjuryTask[], InjuryTask[]] {
  const buckets: [InjuryTask[], InjuryTask[], InjuryTask[]] = [[], [], []];
  for (const task of tasks) {
    const year = getSeasonStartYear(task.date);
    if (year <= 2022) buckets[0].push(task);
    else if (year <= 2024) buckets[1].push(task);
    else buckets[2].push(task);
  }
  return buckets;
}

function parseSnapshotSpecs(flags: Record<string, string>): SnapshotSpec[] {
  const snapshotsFlag = flags.snapshots;
  const customTime = flags.time;
  const out: SnapshotSpec[] = [];

  if (customTime) {
    out.push({ label: "custom", timeEt: customTime });
    return out;
  }

  const requested = (snapshotsFlag ?? "early,final")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  for (const name of requested) {
    if (name === "early") out.push({ label: "early", timeEt: "07:00" });
    else if (name === "final") out.push({ label: "final", timeEt: "17:30" });
    else if (/^\d{1,2}:\d{2}$/.test(name)) {
      const hhmm = name.padStart(5, "0");
      out.push({ label: `t${hhmm.replace(":", "")}`, timeEt: hhmm });
    }
  }

  if (out.length === 0) {
    throw new Error("Invalid snapshot selection. Use --snapshots early,final or --time HH:MM");
  }

  const uniq = new Map<string, SnapshotSpec>();
  for (const s of out) uniq.set(s.label, s);
  return [...uniq.values()];
}

export async function syncInjuries(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const date = flags.date;
  const from = flags.from;
  const to = flags.to;
  const snapshots = parseSnapshotSpecs(flags);
  const skipExisting = (flags["skip-existing"] ?? "false") === "true";
  const delayMs = Math.max(0, Number(flags["delay-ms"] ?? "0"));
  const concurrency = Math.max(1, Number(flags.concurrency ?? "3"));

  let dates: string[] = [];
  if (date) {
    if (!isDateString(date)) {
      throw new Error(`Invalid --date value '${date}'. Expected YYYY-MM-DD.`);
    }
    dates = [date];
  } else if (from && to) {
    if (!isDateString(from) || !isDateString(to) || from > to) {
      throw new Error(`Invalid --from/--to range: ${from} -> ${to}`);
    }
    dates = dateRange(from, to);
  } else {
    console.warn(
      "Tip: If you passed --from/--to, run with -- so flags reach the script:\n  bun run sync:injuries -- --from 2021-10-01 --to 2026-06-30",
    );
    throw new Error(
      "Usage: sync:injuries --date YYYY-MM-DD OR --from YYYY-MM-DD --to YYYY-MM-DD [--snapshots early,final | --time HH:MM]",
    );
  }

  const dataDir = getDataDir();
  const fromFilter = date ? date : from ?? undefined;
  const toFilter = date ? date : to ?? undefined;
  const datesWithGames = await getDatesWithGames(dataDir, fromFilter, toFilter);

  if (datesWithGames.size > 0) {
    const before = dates.length;
    dates = dates.filter((d) => datesWithGames.has(d));
    const dropped = before - dates.length;
    if (dropped > 0) {
      console.log(`Filtered to days with games: ${before} -> ${dates.length} (skipped ${dropped} days with no games).`);
    }
  } else {
    console.warn(
      "No game dates found in data/raw/seasons; fetching injuries for all requested dates (may get 403 on non-game days).",
    );
  }

  if (dates.length === 0) {
    console.log("\nNo dates to fetch (no days with games in range). Done.");
    return;
  }

  const outDir = path.join(dataDir, "raw", "injuries");
  await fs.mkdir(outDir, { recursive: true });

  console.log(`\n=== Sync Injuries ===`);
  console.log(`Dates: ${dates[0]} -> ${dates[dates.length - 1]} (${dates.length} game days)`);
  console.log(
    `Snapshots: ${snapshots.map((s) => `${s.label}@${s.timeEt}`).join(", ")} (${snapshots.length} per day)`,
  );
  console.log(`Skip existing: ${skipExisting}`);
  console.log(`Delay between requests: ${delayMs}ms`);
  console.log(`Concurrency: ${concurrency}`);

  let skipped = 0;
  const tasksToFetch: Array<{ date: string; snapshot: SnapshotSpec }> = [];
  for (const d of dates) {
    for (const snapshot of snapshots) {
      if (!skipExisting) {
        tasksToFetch.push({ date: d, snapshot });
        continue;
      }
      const outPath = path.join(outDir, `${d}.${snapshot.label}.json`);
      try {
        await fs.access(outPath);
        skipped++;
      } catch {
        tasksToFetch.push({ date: d, snapshot });
      }
    }
  }

  const bySnapshot = new Map<string, number>();
  for (const t of tasksToFetch) {
    const k = t.snapshot.label;
    bySnapshot.set(k, (bySnapshot.get(k) ?? 0) + 1);
  }
  console.log(
    `Will fetch: ${tasksToFetch.length} (${[...bySnapshot.entries()].map(([l, n]) => `${l}=${n}`).join(", ")})`,
  );
  if (skipExisting) {
    console.log(`Skipped existing: ${skipped}`);
  }
  if (tasksToFetch.length === 0) {
    console.log("\nDone. Nothing to fetch.");
    return;
  }

  let success = 0;
  let failed = 0;
  let completed = 0;
  const totalTasks = tasksToFetch.length;

  const runWorker = async (myTasks: Array<{ date: string; snapshot: SnapshotSpec }>): Promise<void> => {
    for (const task of myTasks) {
      const d = task.date;
      const snapshot = task.snapshot;
      const outPath = path.join(outDir, `${d}.${snapshot.label}.json`);
      try {
        const rows = await fetchInjuryReportForDate(d, snapshot.timeEt);
        const payload = {
          date: d,
          snapshotLabel: snapshot.label,
          snapshotTimeEt: snapshot.timeEt,
          fetchedAt: new Date().toISOString(),
          source: "nbainjuries",
          rowCount: Array.isArray(rows) ? rows.length : 0,
          rows: Array.isArray(rows) ? rows : [],
        };
        await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
        success++;
      } catch (err) {
        failed++;
        const firstLine = ((err as Error).message ?? "").split("\n")[0];
        console.warn(`  ${d} (${snapshot.label}@${snapshot.timeEt}): failed: ${firstLine}`);
      } finally {
        completed++;
        if (completed % 50 === 0 || completed === totalTasks) {
          console.log(`Progress: ${completed}/${totalTasks} | ok=${success} failed=${failed}`);
        }
        if (delayMs > 0) {
          await sleep(delayMs);
        }
      }
    }
  };

  const useSeasonPartition = !date && from != null && to != null && tasksToFetch.length > 10;
  if (useSeasonPartition) {
    const [bucket0, bucket1, bucket2] = partitionTasksBySeason(tasksToFetch);
    console.log(
      `Runners: 2021–2022=${bucket0.length} | 2023–2024=${bucket1.length} | 2025–2026=${bucket2.length}`,
    );
    await Promise.all([
      runWorker(bucket0),
      runWorker(bucket1),
      runWorker(bucket2),
    ]);
  } else {
    let nextIndex = 0;
    const worker = async (): Promise<void> => {
      while (true) {
        const idx = nextIndex++;
        if (idx >= tasksToFetch.length) return;
        const task = tasksToFetch[idx];
        const d = task.date;
        const snapshot = task.snapshot;
        const outPath = path.join(outDir, `${d}.${snapshot.label}.json`);
        try {
          const rows = await fetchInjuryReportForDate(d, snapshot.timeEt);
          const payload = {
            date: d,
            snapshotLabel: snapshot.label,
            snapshotTimeEt: snapshot.timeEt,
            fetchedAt: new Date().toISOString(),
            source: "nbainjuries",
            rowCount: Array.isArray(rows) ? rows.length : 0,
            rows: Array.isArray(rows) ? rows : [],
          };
          await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
          success++;
        } catch (err) {
          failed++;
          const firstLine = ((err as Error).message ?? "").split("\n")[0];
          console.warn(`  ${d} (${snapshot.label}@${snapshot.timeEt}): failed: ${firstLine}`);
        } finally {
          completed++;
          if (completed % 50 === 0 || completed === totalTasks) {
            console.log(`Progress: ${completed}/${totalTasks} | ok=${success} failed=${failed}`);
          }
          if (delayMs > 0) {
            await sleep(delayMs);
          }
        }
      }
    };
    const workers = Array.from(
      { length: Math.min(concurrency, tasksToFetch.length) },
      () => worker(),
    );
    await Promise.all(workers);
  }

  console.log(`\nDone. Fetched ${success}/${totalTasks} snapshot files; failed=${failed}.`);
}
