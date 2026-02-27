import { ingestGames } from "../ingest/games.js";
import { ingestPlayerStats } from "../ingest/playerstats.js";
import { getPlayerTotals } from "../stats/playerRollup.js";
import { getTeamTotals } from "../stats/teamRollup.js";
import { buildNightEvents } from "../features/buildNightEvents.js";
import { searchPatterns } from "../patterns/searchPatterns.js";
import { explainPattern } from "../patterns/explain.js";
import { rankPatterns } from "../patterns/rank.js";
import { coverageReport } from "../reports/coverage.js";
import { eventCoverageReport } from "../reports/eventCoverage.js";
import { playerProfile } from "../profiles/playerProfile.js";
import { teamProfile } from "../profiles/teamProfile.js";
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

  "build:nightly-events": async () => {
    await buildNightEvents();
  },

  "search:patterns": async (args) => {
    const flags = parseFlags(args);
    const overrides: Partial<PatternFilterConfig> = {};
    if (flags.minOcc) overrides.minOccurrences = Number(flags.minOcc);
    if (flags.minSeasons) overrides.minSeasonsWithHits = Number(flags.minSeasons);
    if (flags.maxCluster) overrides.maxClusterShare = Number(flags.maxCluster);
    if (flags.minAvg) overrides.minAvgHitsPerSeason = Number(flags.minAvg);
    if (flags.maxAvg) overrides.maxAvgHitsPerSeason = Number(flags.maxAvg);
    await searchPatterns(Object.keys(overrides).length > 0 ? overrides : undefined);
  },

  "patterns:explain": async (args) => {
    await explainPattern(args);
  },

  "patterns:rank": async (args) => {
    await rankPatterns(args);
  },

  "report:coverage": async () => {
    await coverageReport();
  },

  "report:events": async () => {
    await eventCoverageReport();
  },

  "profile:player": async (args) => {
    await playerProfile(args);
  },

  "profile:team": async (args) => {
    await teamProfile(args);
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
