import { prisma } from "../db/prisma.js";
import { getEasternDateFromUtc } from "./utils.js";
import { syncNbaStatsForDate } from "./syncNbaStats.js";
import { syncOddsForDate, syncPlayerPropsForDate } from "./syncOdds.js";
import { buildPlayerPropsDayFiles } from "./syncPlayerProps.js";
import { buildDailyDataBundles } from "./buildDailyDataBundles.js";
import { ingestDayBundles } from "./ingestDayBundles.js";

type CoverageRow = {
  date: string;
  games: number;
  statsGames: number;
  oddsGames: number;
  propsGames: number;
};

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

function isDateString(value: string | undefined | null): value is string {
  return !!value && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function dateDaysAgo(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function looksLikeRateLimitError(err: unknown): boolean {
  const msg = (err as Error)?.message?.toLowerCase?.() ?? "";
  return (
    msg.includes("429") ||
    msg.includes("rate") ||
    msg.includes("too many requests") ||
    msg.includes("timed out") ||
    msg.includes("connection")
  );
}

async function runStatsWithRetry(date: string, retries: number, baseDelayMs: number): Promise<void> {
  let attempt = 0;
  while (true) {
    try {
      await syncNbaStatsForDate(date);
      return;
    } catch (err) {
      const canRetry = attempt < retries && looksLikeRateLimitError(err);
      if (!canRetry) throw err;
      const delay = baseDelayMs * Math.pow(2, attempt);
      console.warn(`  nba_api throttled for ${date}; retrying in ${Math.round(delay / 1000)}s...`);
      await sleep(delay);
      attempt++;
    }
  }
}

async function getCoverageRows(toDate: string): Promise<CoverageRow[]> {
  const [gamesRows, statsRows, oddsRows, propsRows] = await Promise.all([
    prisma.$queryRawUnsafe<Array<{ date: string; games: number }>>(
      `SELECT to_char("date", 'YYYY-MM-DD') as "date", COUNT(*)::int as "games"
       FROM "Game"
       WHERE "date" <= '${toDate}'
       GROUP BY "date"`,
    ),
    prisma.$queryRawUnsafe<Array<{ date: string; statsGames: number }>>(
      `SELECT to_char(g."date", 'YYYY-MM-DD') as "date", COUNT(DISTINCT pgs."gameId")::int as "statsGames"
       FROM "PlayerGameStat" pgs
       JOIN "Game" g ON g."id" = pgs."gameId"
       WHERE g."date" <= '${toDate}'
       GROUP BY g."date"`,
    ),
    prisma.$queryRawUnsafe<Array<{ date: string; oddsGames: number }>>(
      `SELECT to_char(g."date", 'YYYY-MM-DD') as "date", COUNT(DISTINCT go."gameId")::int as "oddsGames"
       FROM "GameOdds" go
       JOIN "Game" g ON g."id" = go."gameId"
       WHERE g."date" <= '${toDate}'
       GROUP BY g."date"`,
    ),
    prisma.$queryRawUnsafe<Array<{ date: string; propsGames: number }>>(
      `SELECT to_char(g."date", 'YYYY-MM-DD') as "date", COUNT(DISTINCT ppo."gameId")::int as "propsGames"
       FROM "PlayerPropOdds" ppo
       JOIN "Game" g ON g."id" = ppo."gameId"
       WHERE g."date" <= '${toDate}'
       GROUP BY g."date"`,
    ),
  ]);

  const byDate = new Map<string, CoverageRow>();
  for (const row of gamesRows) {
    if (!isDateString(row.date)) continue;
    byDate.set(row.date, {
      date: row.date,
      games: row.games ?? 0,
      statsGames: 0,
      oddsGames: 0,
      propsGames: 0,
    });
  }
  for (const row of statsRows) {
    if (!isDateString(row.date) || !byDate.has(row.date)) continue;
    byDate.get(row.date)!.statsGames = row.statsGames ?? 0;
  }
  for (const row of oddsRows) {
    if (!isDateString(row.date) || !byDate.has(row.date)) continue;
    byDate.get(row.date)!.oddsGames = row.oddsGames ?? 0;
  }
  for (const row of propsRows) {
    if (!isDateString(row.date) || !byDate.has(row.date)) continue;
    byDate.get(row.date)!.propsGames = row.propsGames ?? 0;
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function isIncompleteDay(
  row: CoverageRow,
  todayEastern: string,
  propsStartDate: string,
): boolean {
  const needsStats = row.date < todayEastern;
  const needsOdds = true;
  const needsProps = row.date >= propsStartDate;

  if (needsStats && row.statsGames < row.games) return true;
  if (needsOdds && row.oddsGames < row.games) return true;
  if (needsProps && row.propsGames < row.games) return true;
  return false;
}

async function detectAutoRange(
  todayEastern: string,
  propsStartDate: string,
  lookbackDays: number,
): Promise<{ from: string; to: string }> {
  const windowStart = dateDaysAgo(todayEastern, Math.max(1, lookbackDays));
  const rows = await getCoverageRows(todayEastern);
  const rowsByDate = new Map(rows.map((r) => [r.date, r]));
  const datesToCheck = dateRange(windowStart, todayEastern);

  let earliestMissing: string | null = null;
  for (const date of datesToCheck) {
    const row = rowsByDate.get(date);
    if (!row) {
      earliestMissing = earliestMissing ?? date;
      continue;
    }
    if (row.games <= 0) {
      earliestMissing = earliestMissing ?? date;
      continue;
    }
    if (isIncompleteDay(row, todayEastern, propsStartDate)) {
      earliestMissing = earliestMissing ?? date;
    }
  }
  if (earliestMissing) {
    return { from: earliestMissing, to: todayEastern };
  }

  if (rows.length === 0) {
    return { from: todayEastern, to: todayEastern };
  }

  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i];
    if (row.games <= 0) continue;
    if (isIncompleteDay(row, todayEastern, propsStartDate)) {
      return { from: row.date, to: todayEastern };
    }
  }

  // Nothing missing: keep current day synced.
  return { from: todayEastern, to: todayEastern };
}

export async function syncMissingDays(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const todayEastern = getEasternDateFromUtc(new Date());
  const propsStartDate = flags["props-start"] ?? "2023-05-03";
  const dryRun = (flags["dry-run"] ?? "false") === "true";
  const forceStats = (flags["force-stats"] ?? "false") === "true";
  const includeTodayStats = (flags["include-today-stats"] ?? "false") === "true";
  const statsRetries = Math.max(0, Number(flags["stats-retries"] ?? "3"));
  const statsRetryDelayMs = Math.max(1000, Number(flags["stats-retry-delay-ms"] ?? "15000"));
  const statsDelayMs = Math.max(0, Number(flags["stats-delay-ms"] ?? "2000"));
  const lookbackDays = Math.max(1, Number(flags["lookback-days"] ?? "30"));

  let from = flags.from;
  let to = flags.to ?? todayEastern;

  if (!from) {
    const auto = await detectAutoRange(to, propsStartDate, lookbackDays);
    from = auto.from;
    to = auto.to;
  }

  if (!isDateString(from) || !isDateString(to) || from > to) {
    throw new Error(`Invalid range: from=${from ?? "undefined"} to=${to ?? "undefined"}`);
  }

  const dates = dateRange(from, to);
  const coverageRows = await getCoverageRows(to);
  const coverageByDate = new Map(coverageRows.map((r) => [r.date, r]));
  console.log(`\n=== Fill Missing Day Data ===`);
  console.log(`Range: ${from} -> ${to} (${dates.length} days)`);
  console.log(`Props required from: ${propsStartDate}`);
  console.log(`Dry run: ${dryRun}`);
  console.log(`Force stats: ${forceStats}`);
  console.log(`Include today stats: ${includeTodayStats}`);
  console.log(`Stats retries: ${statsRetries} (base delay ${statsRetryDelayMs}ms)`);
  console.log(`Stats pacing delay: ${statsDelayMs}ms`);
  console.log(`Auto-detect lookback days: ${lookbackDays}`);

  if (dryRun) {
    for (const date of dates) {
      const row = coverageByDate.get(date);
      const statsMissing = row ? row.statsGames < row.games : true;
      const oddsMissing = row ? row.oddsGames < row.games : true;
      const propsMissing = date >= propsStartDate ? (row ? row.propsGames < row.games : true) : false;
      const shouldStats =
        forceStats ||
        (date < todayEastern || includeTodayStats) &&
          statsMissing &&
          (row ? row.games > 0 : true);
      console.log(
        `  ${date}: stats=${shouldStats ? "run" : "skip"} odds=${oddsMissing ? "run" : "skip"} props=${propsMissing ? "run" : "skip"}`,
      );
    }
    return;
  }

  let statsOk = 0;
  let oddsOk = 0;
  let propsOk = 0;
  for (const date of dates) {
    const row = coverageByDate.get(date);
    const statsMissing = row ? row.statsGames < row.games : true;
    const oddsMissing = row ? row.oddsGames < row.games : true;
    const propsMissing = date >= propsStartDate ? (row ? row.propsGames < row.games : true) : false;
    const shouldStats =
      forceStats ||
      (date < todayEastern || includeTodayStats) &&
        statsMissing &&
        (row ? row.games > 0 : true);

    console.log(`\n[${date}] syncing stats/odds/props...`);

    if (shouldStats) {
      try {
        await runStatsWithRetry(date, statsRetries, statsRetryDelayMs);
        statsOk++;
        if (statsDelayMs > 0) {
          await sleep(statsDelayMs);
        }
      } catch (err) {
        console.warn(`  Stats sync failed for ${date}: ${(err as Error).message}`);
      }
    } else {
      const reason =
        !forceStats && date >= todayEastern && !includeTodayStats
          ? "today/future stats disabled by default"
          : !statsMissing
            ? "stats already complete"
            : "no known games for date";
      console.log(`  Stats skipped (${reason})`);
    }

    if (oddsMissing || row == null) {
      try {
        await syncOddsForDate(date);
        oddsOk++;
      } catch (err) {
        console.warn(`  Odds sync failed for ${date}: ${(err as Error).message}`);
      }
    } else {
      console.log("  Odds skipped (already complete)");
    }

    if (date >= propsStartDate) {
      if (propsMissing || row == null) {
        try {
          await syncPlayerPropsForDate(date);
          propsOk++;
        } catch (err) {
          console.warn(`  Props sync failed for ${date}: ${(err as Error).message}`);
        }
      } else {
        console.log("  Props skipped (already complete)");
      }
    } else {
      console.log(`  Props skipped (before ${propsStartDate})`);
    }
  }

  console.log(`\nBuilding player-props day bundles (${from} -> ${to})...`);
  await buildPlayerPropsDayFiles(["--from", from, "--to", to]);

  console.log(`Building day bundles (${from} -> ${to})...`);
  await buildDailyDataBundles(["--from", from, "--to", to]);

  console.log(`Ingesting day bundles (${from} -> ${to})...`);
  await ingestDayBundles(["--from", from, "--to", to, "--force", "true"]);

  console.log(`\nDone.`);
  console.log(`  Stats synced days: ${statsOk}/${dates.length}`);
  console.log(`  Odds synced days:  ${oddsOk}/${dates.length}`);
  const propsExpected = dates.filter((d) => d >= propsStartDate).length;
  console.log(`  Props synced days: ${propsOk}/${propsExpected}`);
}
