import * as fs from "fs";
import * as path from "path";
import { buildPlayerPropsDayFiles, ingestPlayerPropsRaw } from "./syncPlayerProps.js";
import { buildFeatureBins, buildQuantizedGameFeatures } from "../patterns/discoveryV2.js";
import { reportPlayerPropCoverage } from "../reports/playerPropCoverage.js";

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else if (args[i].startsWith("--")) {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

function scanRawPropDates(rawRoot: string): string[] {
  if (!fs.existsSync(rawRoot)) return [];
  return fs
    .readdirSync(rawRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
    .sort();
}

function findMissingDatesInRange(dates: string[]): string[] {
  if (dates.length === 0) return [];
  const present = new Set(dates);
  const missing: string[] = [];
  let cur = new Date(`${dates[0]}T00:00:00Z`);
  const end = new Date(`${dates[dates.length - 1]}T00:00:00Z`);
  while (cur <= end) {
    const d = cur.toISOString().slice(0, 10);
    if (!present.has(d)) missing.push(d);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return missing;
}

export async function backfillPropCompleteness(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const minCoveragePct = Number(flags["min-coverage-pct"] ?? "20");
  const skipRebuild = (flags["skip-rebuild"] ?? "false") === "true";
  const skipDayFiles = (flags["skip-day-files"] ?? "false") === "true";
  const rawRoot = path.join(getDataDir(), "raw", "odds", "player-props");

  const rawDates = scanRawPropDates(rawRoot);
  if (rawDates.length === 0) {
    throw new Error(`No raw player prop date folders found at ${rawRoot}`);
  }

  const from = flags.from ?? rawDates[0];
  const to = flags.to ?? rawDates[rawDates.length - 1];
  const targetDates = rawDates.filter((d) => d >= from && d <= to);
  if (targetDates.length === 0) {
    throw new Error(`No raw player prop folders in requested range ${from} -> ${to}`);
  }

  const rangeMissingRawDates = findMissingDatesInRange(targetDates);

  console.log("\n=== Prop Completeness Backfill ===\n");
  console.log(`Raw root: ${rawRoot}`);
  console.log(`Raw folders discovered: ${rawDates.length}`);
  console.log(`Backfill range: ${from} -> ${to} (${targetDates.length} raw dates)`);
  console.log(`Missing raw dates inside range: ${rangeMissingRawDates.length}`);
  if (rangeMissingRawDates.length > 0) {
    console.log(`  First missing dates: ${rangeMissingRawDates.slice(0, 10).join(", ")}`);
  }

  if (!skipDayFiles) {
    console.log("\nStep 1/5: Building day bundle files...");
    await buildPlayerPropsDayFiles(["--from", from, "--to", to]);
  } else {
    console.log("\nStep 1/5: Skipping day bundle file build (--skip-day-files true)");
  }

  console.log("\nStep 2/5: Ingesting raw player props...");
  await ingestPlayerPropsRaw(["--from", from, "--to", to, "--prefer-day-files", "true"]);

  if (!skipRebuild) {
    console.log("\nStep 3/5: Rebuilding feature bins...");
    await buildFeatureBins([]);
    console.log("\nStep 4/5: Rebuilding quantized game features...");
    await buildQuantizedGameFeatures([]);
  } else {
    console.log("\nStep 3/5: Skipping feature rebuild (--skip-rebuild true)");
    console.log("Step 4/5: Skipping quantized rebuild (--skip-rebuild true)");
  }

  console.log("\nStep 5/5: Running coverage check...");
  const coverage = await reportPlayerPropCoverage(["--from", from, "--to", to]);

  if (coverage.contextCoveragePct < minCoveragePct) {
    throw new Error(
      `Coverage gate failed: ${coverage.contextCoveragePct.toFixed(1)}% < ${minCoveragePct.toFixed(1)}% (context games with props)`,
    );
  }

  console.log(
    `Coverage gate passed: ${coverage.contextCoveragePct.toFixed(1)}% >= ${minCoveragePct.toFixed(1)}%`,
  );
}
