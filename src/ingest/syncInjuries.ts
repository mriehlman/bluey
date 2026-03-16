import * as fs from "fs/promises";
import * as path from "path";
import { fetchInjuryReportForDate } from "../api/nbaInjuries.js";

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

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    else if (name === "final") out.push({ label: "final", timeEt: "17:00" });
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
  const concurrency = Math.max(1, Number(flags.concurrency ?? "1"));

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
    throw new Error("Usage: sync:injuries --date YYYY-MM-DD OR --from YYYY-MM-DD --to YYYY-MM-DD [--time HH:MM]");
  }

  const outDir = path.join(getDataDir(), "raw", "injuries");
  await fs.mkdir(outDir, { recursive: true });

  console.log(`\n=== Sync Injuries ===`);
  console.log(`Dates: ${dates[0]} -> ${dates[dates.length - 1]} (${dates.length})`);
  console.log(`Snapshots: ${snapshots.map((s) => `${s.label}@${s.timeEt}`).join(", ")}`);
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

  console.log(`Will fetch: ${tasksToFetch.length}`);
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
        if (completed % 25 === 0 || completed === tasksToFetch.length) {
          console.log(`Progress: ${completed}/${tasksToFetch.length} | ok=${success} failed=${failed}`);
        }
        if (delayMs > 0) {
          await sleep(delayMs);
        }
      }
    }
  };

  const workers = Array.from({ length: Math.min(concurrency, tasksToFetch.length) }, () => worker());
  await Promise.all(workers);

  console.log(`\nDone. Fetched ${success}/${tasksToFetch.length} snapshot files; failed=${failed}.`);
}
