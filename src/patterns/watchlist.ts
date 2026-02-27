import { prisma } from "../db/prisma.js";
import { CATALOG } from "../features/eventCatalog.js";
import type { NightContext, TeamAgg } from "../features/eventCatalog.js";

const EVENT_LOGIC_VERSION = process.env.EVENT_LOGIC_VERSION ?? "unknown";

export async function watchlistAdd(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const patternKey = flags.patternKey;
  const patternId = flags.patternId;
  const notes = flags.notes ?? null;

  if (!patternKey && !patternId) {
    console.error("Usage: watchlist:add --patternKey <KEY1+KEY2> [--notes \"...\"]");
    console.error("   or: watchlist:add --patternId <id> [--notes \"...\"]");
    process.exit(1);
  }

  const pattern = await prisma.pattern.findUnique({
    where: patternId ? { id: patternId } : { patternKey: patternKey! },
  });

  if (!pattern) {
    console.error(`Pattern not found: ${patternId ?? patternKey}`);
    process.exit(1);
  }

  await prisma.patternWatchlist.upsert({
    where: { patternId: pattern.id },
    update: { notes, enabled: true },
    create: { patternId: pattern.id, notes },
  });

  console.log(`Added to watchlist: ${pattern.patternKey}`);
  if (notes) console.log(`  Notes: ${notes}`);
}

export async function watchlistList(): Promise<void> {
  const items = await prisma.patternWatchlist.findMany({
    include: { pattern: true },
    orderBy: { createdAt: "asc" },
  });

  if (items.length === 0) {
    console.log("Watchlist is empty. Add patterns with: bun watchlist:add --patternKey <KEY>");
    return;
  }

  console.log(`\n=== Pattern Watchlist (${items.length} items) ===\n`);
  console.log(
    "  " + "Status".padEnd(10) + "Pattern".padEnd(50) + "Score".padStart(7) + "  Occ".padStart(5) + "  Notes"
  );
  console.log("  " + "-".repeat(100));

  for (const item of items) {
    const status = item.enabled ? "ACTIVE" : "PAUSED";
    console.log(
      "  " +
      status.padEnd(10) +
      item.pattern.patternKey.padEnd(50) +
      (item.pattern.overallScore?.toFixed(3) ?? "-").padStart(7) +
      String(item.pattern.occurrences).padStart(5) +
      `  ${item.notes ?? ""}`
    );
  }

  console.log();
}

export async function watchlistRemove(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const patternKey = flags.patternKey;
  if (!patternKey) {
    console.error("Usage: watchlist:remove --patternKey <KEY1+KEY2>");
    process.exit(1);
  }

  const pattern = await prisma.pattern.findUnique({ where: { patternKey } });
  if (!pattern) {
    console.error(`Pattern not found: ${patternKey}`);
    process.exit(1);
  }

  await prisma.patternWatchlist.deleteMany({ where: { patternId: pattern.id } });
  console.log(`Removed from watchlist: ${patternKey}`);
}

export async function checkLatest(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const daysBack = flags.days ? Number(flags.days) : 3;

  const watchedItems = await prisma.patternWatchlist.findMany({
    where: { enabled: true },
    include: { pattern: { include: { hits: { orderBy: { date: "desc" }, take: 1 } } } },
  });

  if (watchedItems.length === 0) {
    console.log("No active watchlist items. Add patterns with: bun watchlist:add --patternKey <KEY>");
    return;
  }

  const recentDates = await prisma.game.findMany({
    select: { date: true },
    distinct: ["date"],
    orderBy: { date: "desc" },
    take: daysBack,
  });

  if (recentDates.length === 0) {
    console.log("No recent game dates found.");
    return;
  }

  console.log(`\n=== Watchlist Check — Last ${recentDates.length} Game Dates ===`);
  console.log(`  EVENT_LOGIC_VERSION: ${EVENT_LOGIC_VERSION}\n`);

  const missingStatsDates: string[] = [];

  for (const { date } of recentDates) {
    const dateStr = date.toISOString().slice(0, 10);

    const games = await prisma.game.findMany({
      where: { date },
      include: { homeTeam: true, awayTeam: true },
    });

    const gameIds = games.map((g) => g.id);

    const stats = await prisma.playerGameStat.findMany({
      where: { gameId: { in: gameIds } },
      include: { player: true },
    });

    const statsPresent = stats.length > 0;
    if (!statsPresent && games.length > 0) {
      missingStatsDates.push(dateStr);
    }

    const aggRows = await prisma.nightTeamAggregate.findMany({ where: { date } });
    const teamAggregates: TeamAgg[] | undefined =
      aggRows.length > 0
        ? aggRows.map((r) => ({
            teamId: r.teamId,
            points: r.points,
            rebounds: r.rebounds,
            assists: r.assists,
            steals: r.steals,
            blocks: r.blocks,
            turnovers: r.turnovers,
            minutes: r.minutes,
          }))
        : undefined;

    const season = games[0]?.season ?? 0;
    const ctx: NightContext = { date: dateStr, season, games, stats, teamAggregates };

    const eventResults = new Map<string, boolean>();
    for (const def of CATALOG) {
      const result = def.compute(ctx);
      eventResults.set(def.key, result.hit);
    }

    const statsTag = statsPresent ? "" : "  [NO STATS]";
    console.log(`  ${dateStr} (${games.length} games):${statsTag}`);

    let anyHit = false;
    for (const item of watchedItems) {
      const pattern = item.pattern;
      const allHit = pattern.eventKeys.every((k) => eventResults.get(k) === true);

      if (allHit) {
        anyHit = true;
        console.log(`    HIT  ${pattern.patternKey}${item.notes ? ` — ${item.notes}` : ""}`);
      }
    }

    if (!anyHit) {
      console.log(`    (no watchlist patterns hit)`);
    }

    console.log();
  }

  if (missingStatsDates.length > 0) {
    console.log(`  WARNING: ${missingStatsDates.length} date(s) have games but NO player stats.`);
    console.log(`  Pattern hits may be undercounted for: ${missingStatsDates.join(", ")}`);
    console.log(`  Run 'bun ingest:playerstats' to backfill, then re-check.\n`);
  }

  console.log(`=== End Watchlist Check (eventLogicVersion: ${EVENT_LOGIC_VERSION}) ===`);
}
