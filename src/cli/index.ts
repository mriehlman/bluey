import { ingestGames } from "../ingest/games.js";
import { ingestPlayerStats } from "../ingest/playerstats.js";
import { syncGamesForDate, syncGamesForRange, syncGamesForSeason } from "../ingest/syncGames.js";
import { syncPlayerStatsForDate } from "../ingest/syncPlayerStats.js";
import { syncOddsLive, syncOddsForDate } from "../ingest/syncOdds.js";
import { getPlayerTotals } from "../stats/playerRollup.js";
import { getTeamTotals } from "../stats/teamRollup.js";
import { buildNightEvents } from "../features/buildNightEvents.js";
import { buildNightAggregates } from "../features/buildNightAggregates.js";
import { explainNight } from "../features/explainNight.js";
import { buildNights } from "../features/buildNights.js";
import { searchPatterns } from "../patterns/searchPatterns.js";
import { searchGamePatterns } from "../patterns/searchGamePatterns.js";
import { explainPattern } from "../patterns/explain.js";
import { rankPatterns } from "../patterns/rank.js";
import { dedupePatterns } from "../patterns/dedupe.js";
import { watchlistAdd, watchlistList, watchlistRemove, checkLatest } from "../patterns/watchlist.js";
import { coverageReport } from "../reports/coverage.js";
import { eventCoverageReport } from "../reports/eventCoverage.js";
import { aggregateCoverageReport } from "../reports/aggregateCoverage.js";
import { playerProfile } from "../profiles/playerProfile.js";
import { teamProfile } from "../profiles/teamProfile.js";
import { buildGameContext } from "../features/buildGameContext.js";
import { buildGameEvents } from "../features/buildGameEvents.js";
import { predictGames, predictPlayers } from "../features/predictGames.js";
import type { RollupFilters } from "../stats/filters.js";
import type { PatternFilterConfig } from "../patterns/config.js";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }
  return flags;
}

function filtersFromFlags(flags: Record<string, string>): RollupFilters {
  const f: RollupFilters = {};
  if (flags.season) f.season = Number(flags.season);
  if (flags.dateFrom) f.dateFrom = flags.dateFrom;
  if (flags.dateTo) f.dateTo = flags.dateTo;
  if (flags.opponentTeamId) f.opponentTeamId = Number(flags.opponentTeamId);
  if (flags.homeAway && ["home", "away", "either"].includes(flags.homeAway)) {
    f.homeAway = flags.homeAway as "home" | "away" | "either";
  }
  if (flags.minMinutes) f.minMinutes = Number(flags.minMinutes);
  if (flags.stage) f.stage = Number(flags.stage);
  return f;
}

const COMMANDS: Record<string, (args: string[]) => Promise<void>> = {
  "ingest:games": async () => {
    await ingestGames();
  },

  "ingest:playerstats": async () => {
    await ingestPlayerStats();
  },

  "sync:games": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncGamesForDate(flags.date);
    } else if (flags.from && flags.to) {
      await syncGamesForRange(flags.from, flags.to);
    } else {
      console.error("Usage: sync:games --date YYYY-MM-DD  or  sync:games --from YYYY-MM-DD --to YYYY-MM-DD");
      process.exit(1);
    }
  },

  "sync:stats": async (args) => {
    const flags = parseFlags(args);
    if (!flags.date) {
      console.error("Usage: sync:stats --date YYYY-MM-DD");
      process.exit(1);
    }
    await syncPlayerStatsForDate(flags.date);
  },

  "sync:odds": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncOddsForDate(flags.date);
    } else {
      await syncOddsLive();
    }
  },

  "sync:daily": async () => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().slice(0, 10);

    console.log(`\n=== Daily Sync for ${dateStr} ===\n`);

    console.log("Step 1/3: Syncing games...");
    await syncGamesForDate(dateStr);

    console.log("\nStep 2/3: Syncing player stats...");
    await syncPlayerStatsForDate(dateStr);

    console.log("\nStep 3/3: Syncing odds...");
    try {
      await syncOddsForDate(dateStr);
    } catch (err) {
      console.warn("  Odds sync failed (non-fatal):", (err as Error).message);
    }

    console.log("\n=== Daily sync complete ===");
  },

  "sync:backfill": async (args) => {
    const flags = parseFlags(args);
    const fromSeason = Number(flags["from-season"] || flags.season);
    const toSeason = Number(flags["to-season"] || flags.season || fromSeason);

    if (!fromSeason) {
      console.error(
        "Usage: sync:backfill --season YYYY  or  sync:backfill --from-season YYYY --to-season YYYY",
      );
      process.exit(1);
    }

    for (let season = fromSeason; season <= toSeason; season++) {
      console.log(`\n=== Backfilling season ${season} ===\n`);

      console.log("Step 1/2: Syncing games...");
      const games = await syncGamesForSeason(season);

      console.log("\nStep 2/2: Syncing player stats...");
      const uniqueDates = [...new Set(games.filter((g) => g.status === "Final").map((g) => g.date.slice(0, 10)))].sort();
      console.log(`  Processing ${uniqueDates.length} game dates...`);

      let totalStats = 0;
      for (let i = 0; i < uniqueDates.length; i++) {
        const count = await syncPlayerStatsForDate(uniqueDates[i]);
        totalStats += count;
        if ((i + 1) % 10 === 0) {
          console.log(`  Progress: ${i + 1}/${uniqueDates.length} dates (${totalStats} stats)`);
        }
      }

      console.log(`\n=== Season ${season} backfill complete: ${totalStats} total stats ===`);
    }
  },

  "stats:player": async (args) => {
    const flags = parseFlags(args);
    const playerId = Number(flags.playerId);
    if (!playerId) {
      console.error("Usage: stats:player --playerId <id> [--season N] [--dateFrom YYYY-MM-DD] ...");
      process.exit(1);
    }
    const filters = filtersFromFlags(flags);
    const result = await getPlayerTotals(playerId, filters);
    console.log(`\nPlayer ${playerId} Totals:`);
    console.log(`  Games Played: ${result.gamesPlayed}`);
    console.log(`  Minutes:      ${Math.floor(result.minutes / 60)}:${String(result.minutes % 60).padStart(2, "0")} (${result.minutes}s)`);
    console.log(`  Points:       ${result.points}`);
    console.log(`  Assists:      ${result.assists}`);
    console.log(`  Rebounds:     ${result.rebounds}`);
    console.log(`  Steals:       ${result.steals}`);
    console.log(`  Blocks:       ${result.blocks}`);
    console.log(`  Turnovers:    ${result.turnovers}`);
    if (result.gamesPlayed > 0) {
      console.log(`\n  Per Game Averages:`);
      const gp = result.gamesPlayed;
      console.log(`  PPG: ${(result.points / gp).toFixed(1)}  APG: ${(result.assists / gp).toFixed(1)}  RPG: ${(result.rebounds / gp).toFixed(1)}`);
    }
  },

  "stats:team": async (args) => {
    const flags = parseFlags(args);
    const teamId = Number(flags.teamId);
    if (!teamId) {
      console.error("Usage: stats:team --teamId <id> [--season N] [--dateFrom YYYY-MM-DD] ...");
      process.exit(1);
    }
    const filters = filtersFromFlags(flags);
    const result = await getTeamTotals(teamId, filters);
    console.log(`\nTeam ${teamId} Totals:`);
    console.log(`  Games Played: ${result.gamesPlayed}`);
    console.log(`  Points:       ${result.points}`);
    console.log(`  Assists:      ${result.assists}`);
    console.log(`  Rebounds:     ${result.rebounds}`);
    console.log(`  Steals:       ${result.steals}`);
    console.log(`  Blocks:       ${result.blocks}`);
    console.log(`  Turnovers:    ${result.turnovers}`);
    if (result.gamesPlayed > 0) {
      const gp = result.gamesPlayed;
      console.log(`\n  Per Game Averages:`);
      console.log(`  PPG: ${(result.points / gp).toFixed(1)}  APG: ${(result.assists / gp).toFixed(1)}  RPG: ${(result.rebounds / gp).toFixed(1)}`);
    }
  },

  "build:nightly-events": async (args) => {
    await buildNightEvents(args);
  },

  "build:night-aggregates": async (args) => {
    await buildNightAggregates(args);
  },

  "events:explain": async (args) => {
    await explainNight(args);
  },

  "build:nights": async (args) => {
    await buildNights(args);
  },

  "search:patterns": async (args) => {
    const flags = parseFlags(args);
    const overrides: Partial<PatternFilterConfig> = {};
    if (flags.minOcc) overrides.minOccurrences = Number(flags.minOcc);
    if (flags.minSeasons) overrides.minSeasonsWithHits = Number(flags.minSeasons);
    if (flags.maxCluster) overrides.maxClusterShare = Number(flags.maxCluster);
    if (flags.minAvg) overrides.minAvgHitsPerSeason = Number(flags.minAvg);
    if (flags.maxAvg) overrides.maxAvgHitsPerSeason = Number(flags.maxAvg);
    if (flags.maxResults) overrides.maxResults = Number(flags.maxResults);
    await searchPatterns(Object.keys(overrides).length > 0 ? overrides : undefined);
  },

  "patterns:explain": async (args) => {
    await explainPattern(args);
  },

  "patterns:rank": async (args) => {
    await rankPatterns(args);
  },

  "patterns:dedupe": async (args) => {
    await dedupePatterns(args);
  },

  "patterns:check-latest": async (args) => {
    await checkLatest(args);
  },

  "watchlist:add": async (args) => {
    await watchlistAdd(args);
  },

  "watchlist:list": async () => {
    await watchlistList();
  },

  "watchlist:remove": async (args) => {
    await watchlistRemove(args);
  },

  "report:coverage": async () => {
    await coverageReport();
  },

  "report:events": async () => {
    await eventCoverageReport();
  },

  "report:aggregates": async () => {
    await aggregateCoverageReport();
  },

  "profile:player": async (args) => {
    await playerProfile(args);
  },

  "profile:team": async (args) => {
    await teamProfile(args);
  },

  "build:game-context": async (args) => {
    await buildGameContext(args);
  },

  "build:game-events": async (args) => {
    await buildGameEvents(args);
  },

  "search:game-patterns": async (args) => {
    const flags = parseFlags(args);
    const overrides: Record<string, number> = {};
    if (flags.minSample) overrides.minSample = Number(flags.minSample);
    if (flags.minHitRate) overrides.minHitRate = Number(flags.minHitRate);
    if (flags.maxLegs) overrides.maxLegs = Number(flags.maxLegs);
    if (flags.maxResults) overrides.maxResults = Number(flags.maxResults);
    if (flags.minSeasons) overrides.minSeasons = Number(flags.minSeasons);
    await searchGamePatterns(Object.keys(overrides).length > 0 ? overrides : undefined);
  },

  "predict:games": async (args) => {
    await predictGames(args);
  },

  "predict:players": async (args) => {
    await predictPlayers(args);
  },
};

async function main() {
  const command = process.argv[2];

  if (!command || !COMMANDS[command]) {
    console.log("Bluey CLI\n");
    console.log("Available commands:");
    for (const cmd of Object.keys(COMMANDS)) {
      console.log(`  ${cmd}`);
    }
    console.log("\nUsage: bun run src/cli/index.ts <command> [options]");
    process.exit(command ? 1 : 0);
  }

  const args = process.argv.slice(3);

  try {
    await COMMANDS[command](args);
  } catch (err) {
    console.error(`Error running ${command}:`, err);
    process.exit(1);
  }
}

main();
