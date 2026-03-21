import { prisma } from "@bluey/db";

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

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function backfillSuggestedLedger(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const from = flags.from ?? "2023-10-01";
  const to = flags.to ?? new Date().toISOString().slice(0, 10);
  const baseUrl = flags["base-url"] ?? "http://localhost:3000";
  const delayMs = Math.max(0, Number(flags["delay-ms"] ?? 0));
  const stopOnError = (flags["stop-on-error"] ?? "false") === "true";
  const dryRun = (flags["dry-run"] ?? "false") === "true";

  const dateRows = await prisma.$queryRawUnsafe<Array<{ date: string }>>(
    `SELECT DISTINCT TO_CHAR("date", 'YYYY-MM-DD') as "date"
     FROM "Game"
     WHERE "date" >= '${sqlEsc(from)}' AND "date" <= '${sqlEsc(to)}'
     ORDER BY "date" ASC`,
  );
  const dates = dateRows.map((r) => r.date).filter(Boolean);

  console.log("\n=== Backfill SuggestedPlayLedger ===\n");
  console.log(`Range: ${from} -> ${to}`);
  console.log(`Dates found in Game table: ${dates.length}`);
  console.log(`Base URL: ${baseUrl}`);
  console.log(`Delay: ${delayMs}ms`);
  console.log(`Dry run: ${dryRun}\n`);
  if (dates.length === 0) return;

  let ok = 0;
  let failed = 0;
  const failures: string[] = [];

  for (let i = 0; i < dates.length; i++) {
    const d = dates[i];
    const url = `${baseUrl}/api/predictions?date=${encodeURIComponent(d)}&refreshLedger=1`;

    if (dryRun) {
      console.log(`[${i + 1}/${dates.length}] DRY ${d}`);
      continue;
    }

    try {
      const res = await fetch(url, { method: "GET" });
      if (!res.ok) {
        failed++;
        const msg = `[${i + 1}/${dates.length}] FAIL ${d} -> HTTP ${res.status}`;
        failures.push(msg);
        console.log(msg);
        if (stopOnError) break;
      } else {
        ok++;
        console.log(`[${i + 1}/${dates.length}] OK   ${d}`);
      }
    } catch (err) {
      failed++;
      const msg = `[${i + 1}/${dates.length}] ERR  ${d} -> ${(err as Error).message}`;
      failures.push(msg);
      console.log(msg);
      if (stopOnError) break;
    }

    if (delayMs > 0 && i < dates.length - 1) {
      await sleep(delayMs);
    }
  }

  console.log("\n=== Backfill complete ===");
  console.log(`Success: ${ok}`);
  console.log(`Failed:  ${failed}`);
  if (failures.length > 0) {
    console.log("\nSample failures:");
    for (const f of failures.slice(0, 10)) console.log(`  ${f}`);
    if (failures.length > 10) {
      console.log(`  ... and ${failures.length - 10} more`);
    }
  }
}
