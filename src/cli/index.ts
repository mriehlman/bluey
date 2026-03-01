import * as fs from "fs/promises";
import { ingestGames } from "../ingest/games.js";
import { ingestPlayerStats } from "../ingest/playerstats.js";
import { syncGamesForDate, syncGamesForRange, syncGamesForSeason, syncUpcomingGames } from "../ingest/syncGames.js";
import { syncPlayerStatsForDate } from "../ingest/syncPlayerStats.js";
import { syncNbaStatsForDate, syncNbaStatsForDateRange } from "../ingest/syncNbaStats.js";
import { syncOddsLive, syncOddsForDate } from "../ingest/syncOdds.js";
import { syncPlayerPropsLive } from "../ingest/syncPlayerProps.js";
import { fetchSeasonToJson, processSeasonFromJson } from "../ingest/rawDataPipeline.js";
import { ingestSeason, ingestAllSeasons, getIngestionStatus } from "../ingest/ingestRawNbaData.js";
import { enrichSeason, enrichAllSeasons, getEnrichmentStatus } from "../ingest/enrichFromRawNba.js";
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
import { runBacktest, backtestExisting, analyzePattern, quickValidate } from "../backtest/backtest.js";
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

  "sync:nba-stats": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncNbaStatsForDate(flags.date);
    } else if (flags.from && flags.to) {
      await syncNbaStatsForDateRange(flags.from, flags.to);
    } else {
      console.error("Usage: sync:nba-stats --date YYYY-MM-DD  or  --from YYYY-MM-DD --to YYYY-MM-DD");
      process.exit(1);
    }
  },

  "sync:odds": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncOddsForDate(flags.date);
    } else {
      await syncOddsLive();
    }
  },

  "sync:player-props": async () => {
    await syncPlayerPropsLive();
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

  "sync:upcoming": async (args) => {
    const flags = parseFlags(args);
    const dateStr = flags.date || new Date().toISOString().slice(0, 10);
    
    console.log(`\n=== Sync Upcoming Games for ${dateStr} ===\n`);
    
    console.log("Step 1/2: Syncing upcoming games...");
    await syncUpcomingGames(dateStr);
    
    console.log("\nStep 2/2: Syncing live odds...");
    try {
      await syncOddsLive();
    } catch (err) {
      console.warn("  Odds sync failed (non-fatal):", (err as Error).message);
    }
    
    console.log("\n=== Upcoming sync complete ===");
  },

  "predict:today": async (args) => {
    const flags = parseFlags(args);
    const dateStr = flags.date || new Date().toISOString().slice(0, 10);
    
    console.log(`\n=== Today's Predictions for ${dateStr} ===\n`);
    
    // Step 1: Sync upcoming games
    console.log("Step 1/3: Syncing upcoming games...");
    await syncUpcomingGames(dateStr);
    
    // Step 2: Sync live odds  
    console.log("\nStep 2/3: Syncing live odds...");
    try {
      await syncOddsLive();
    } catch (err) {
      console.warn("  Odds sync failed (non-fatal):", (err as Error).message);
    }
    
    // Step 3: Run predictions
    console.log("\nStep 3/3: Running predictions...\n");
    await predictGames(["--date", dateStr]);
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

  "fetch:season": async (args) => {
    const flags = parseFlags(args);
    const season = flags.season;
    
    if (!season) {
      console.error("Usage: fetch:season --season 2024-25 [--from YYYY-MM-DD] [--to YYYY-MM-DD]");
      process.exit(1);
    }
    
    await fetchSeasonToJson(season, flags.from, flags.to);
  },

  "process:season": async (args) => {
    const flags = parseFlags(args);
    const season = flags.season;
    
    if (!season) {
      console.error("Usage: process:season --season 2024-25");
      process.exit(1);
    }
    
    await processSeasonFromJson(season);
  },

  "backfill:odds": async (args) => {
    const { fetchHistoricalOdds, fetchHistoricalEventOdds } = await import("../api/oddsApi.js");
    
    const flags = parseFlags(args);
    const from = flags.from || "2020-07-30";
    const to = flags.to || new Date().toISOString().slice(0, 10);
    const concurrency = Number(flags.concurrency) || 10;
    const batchDelay = Number(flags.delay) || 0; // ms to wait between batches
    const skipProps = flags.skipProps === "true";
    
    const dataDir = process.env.DATA_DIR || "./data";
    const historicalDir = `${dataDir}/raw/odds/historical`;
    const propsDir = `${dataDir}/raw/odds/player-props`;
    const progressFile = `${dataDir}/raw/odds/_progress.json`;
    
    await fs.mkdir(historicalDir, { recursive: true });
    await fs.mkdir(propsDir, { recursive: true });
    
    const PLAYER_PROPS_START = "2023-05-03";
    const PLAYER_PROP_MARKETS = "player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists";
    
    const scanExistingFiles = async (dir: string): Promise<Set<string>> => {
      const dates = new Set<string>();
      try {
        const files = await fs.readdir(dir);
        for (const file of files) {
          const match = file.match(/^(\d{4}-\d{2}-\d{2})\.json$/);
          if (match) dates.add(match[1]);
        }
      } catch { /* dir doesn't exist yet */ }
      return dates;
    };
    
    const scanExistingPropDates = async (): Promise<Set<string>> => {
      const dates = new Set<string>();
      try {
        const subdirs = await fs.readdir(propsDir);
        for (const subdir of subdirs) {
          if (/^\d{4}-\d{2}-\d{2}$/.test(subdir)) {
            // Only count if directory has actual files (not empty)
            try {
              const files = await fs.readdir(`${propsDir}/${subdir}`);
              const jsonFiles = files.filter(f => f.endsWith('.json'));
              if (jsonFiles.length > 0) {
                dates.add(subdir);
              }
            } catch { /* empty or unreadable */ }
          }
        }
      } catch { /* dir doesn't exist yet */ }
      return dates;
    };

    console.log("Scanning existing files...");
    const existingOddsDates = await scanExistingFiles(historicalDir);
    const existingPropDates = await scanExistingPropDates();
    
    const allDates: string[] = [];
    const startDate = new Date(from);
    const endDate = new Date(to);
    let current = new Date(startDate);

    while (current <= endDate) {
      allDates.push(current.toISOString().slice(0, 10));
      current.setDate(current.getDate() + 1);
    }

    const pendingOddsDates = allDates.filter(d => !existingOddsDates.has(d));
    const playerPropDates = allDates.filter(d => d >= PLAYER_PROPS_START);
    const pendingPropDates = skipProps ? [] : playerPropDates.filter(d => !existingPropDates.has(d));

    console.log(`\n=== Backfilling Historical Odds (PARALLEL: ${concurrency} concurrent) ===`);
    console.log(`Range: ${from} to ${to}`);
    console.log(`Total dates: ${allDates.length}`);
    console.log(`\nGame odds:`);
    console.log(`  Already have: ${existingOddsDates.size} files`);
    console.log(`  Remaining: ${pendingOddsDates.length}`);
    if (!skipProps) {
      console.log(`\nPlayer props (available from ${PLAYER_PROPS_START}):`);
      console.log(`  Dates in range: ${playerPropDates.length}`);
      console.log(`  Already have: ${existingPropDates.size} dates`);
      console.log(`  Remaining: ${pendingPropDates.length}`);
    } else {
      console.log(`\nPlayer props: SKIPPED (--skipProps true)`);
    }
    console.log(`\nPress Ctrl+C to pause anytime.\n`);

    if (pendingOddsDates.length === 0 && pendingPropDates.length === 0) {
      console.log("All dates already downloaded!");
      return;
    }

    let sessionOddsCount = 0;
    let sessionPropsCount = 0;
    let interrupted = false;
    let rateLimited = false;
    const errors: string[] = [];
    
    const saveProgress = async () => {
      const updatedOddsDates = await scanExistingFiles(historicalDir);
      const updatedPropDates = await scanExistingPropDates();
      const progress = {
        startDate: from,
        endDate: to,
        completedDates: Array.from(updatedOddsDates).sort(),
        completedPlayerPropDates: Array.from(updatedPropDates).sort(),
        lastRunAt: new Date().toISOString(),
      };
      await fs.writeFile(progressFile, JSON.stringify(progress, null, 2));
    };
    
    process.on("SIGINT", async () => {
      console.log("\n\nInterrupted! Saving progress...");
      interrupted = true;
      await saveProgress();
      console.log(`Progress saved. Run again to continue.`);
      process.exit(0);
    });

    const fetchDateOdds = async (dateStr: string): Promise<{ date: string; success: boolean; events: number }> => {
      if (interrupted || rateLimited) return { date: dateStr, success: false, events: 0 };
      
      try {
        const events = await fetchHistoricalOdds(dateStr);
        const filepath = `${historicalDir}/${dateStr}.json`;
        await fs.writeFile(filepath, JSON.stringify(events, null, 2));
        return { date: dateStr, success: true, events: events.length };
      } catch (err) {
        const errorMsg = (err as Error).message;
        if (errorMsg.includes("429") || errorMsg.includes("rate")) {
          rateLimited = true;
        }
        errors.push(`${dateStr}: ${errorMsg}`);
        return { date: dateStr, success: false, events: 0 };
      }
    };

    const processBatch = async <T, R>(items: T[], fn: (item: T) => Promise<R>, batchSize: number): Promise<R[]> => {
      const results: R[] = [];
      for (let i = 0; i < items.length; i += batchSize) {
        if (interrupted || rateLimited) break;
        const batch = items.slice(i, i + batchSize);
        const batchResults = await Promise.all(batch.map(fn));
        results.push(...batchResults);

        const completed = Math.min(i + batchSize, items.length);
        const successful = results.filter(r => (r as { success?: boolean }).success).length;
        console.log(`  Progress: ${completed}/${items.length} (${successful} successful)`);

        if (completed % 50 === 0) {
          await saveProgress();
        }
        
        if (batchDelay > 0 && i + batchSize < items.length) {
          await new Promise(r => setTimeout(r, batchDelay));
        }
      }
      return results;
    };

    console.log(`\n--- Fetching Game Odds (${pendingOddsDates.length} dates) ---\n`);
    const oddsResults = await processBatch(pendingOddsDates, fetchDateOdds, concurrency);
    sessionOddsCount = oddsResults.filter(r => r.success).length;

    if (rateLimited) {
      console.log("\nRate limited! Saving progress and stopping...");
      await saveProgress();
      console.log(`Saved ${sessionOddsCount} game odds files before rate limit.`);
      process.exit(1);
    }

    if (!skipProps && pendingPropDates.length > 0 && !interrupted) {
      console.log(`\n--- Fetching Player Props (${pendingPropDates.length} dates) ---`);
      console.log(`  Reading event IDs from local game odds files (no extra API calls)\n`);

      for (const dateStr of pendingPropDates) {
        if (interrupted || rateLimited) break;

        try {
          const oddsFilePath = `${historicalDir}/${dateStr}.json`;
          let eventIds: string[] = [];

          try {
            const oddsData = JSON.parse(await fs.readFile(oddsFilePath, "utf-8")) as Array<{ id: string; commence_time: string }>;
            // Only get events that actually occur on this date (filter out futures)
            const sameDay = oddsData.filter(e => e.commence_time?.startsWith(dateStr));
            eventIds = sameDay.map(e => e.id);
            
            if (sameDay.length < oddsData.length) {
              console.log(`  ${dateStr}: ${sameDay.length} same-day events (${oddsData.length - sameDay.length} futures skipped)`);
            }
          } catch {
            console.warn(`  ${dateStr}: No local odds file, skipping`);
            continue;
          }

          if (eventIds.length === 0) {
            continue; // No same-day events, skip
          }

          if (eventIds.length > 0) {
            const datePropsDir = `${propsDir}/${dateStr}`;
            const propsToSave: Array<{ eventId: string; data: unknown }> = [];

            const fetchProp = async (eventId: string) => {
              try {
                const propsData = await fetchHistoricalEventOdds(eventId, dateStr, PLAYER_PROP_MARKETS);
                if (propsData && propsData.bookmakers?.length) {
                  propsToSave.push({ eventId, data: propsData });
                  return true;
                }
              } catch (err) {
                const msg = (err as Error).message;
                if (msg.includes("429") || msg.includes("rate")) {
                  rateLimited = true;
                }
              }
              return false;
            };

            const propResults = await Promise.all(eventIds.map(fetchProp));
            const propsCount = propResults.filter(Boolean).length;
            
            // Only create directory and save if we got actual data
            if (propsToSave.length > 0) {
              await fs.mkdir(datePropsDir, { recursive: true });
              for (const { eventId, data } of propsToSave) {
                await fs.writeFile(
                  `${datePropsDir}/${eventId}.json`,
                  JSON.stringify(data, null, 2)
                );
              }
            }
            
            console.log(`  ${dateStr}: ${propsCount}/${eventIds.length} events`);
            sessionPropsCount += propsCount;
          }
        } catch (propErr) {
          const errorMsg = (propErr as Error).message;
          console.warn(`  ${dateStr} error: ${errorMsg}`);
          if (errorMsg.includes("429") || errorMsg.includes("rate")) {
            rateLimited = true;
            break;
          }
        }
      }
    }
    
    await saveProgress();
    
    const finalOddsDates = await scanExistingFiles(historicalDir);
    const finalPropDates = await scanExistingPropDates();
    
    console.log(`\n=== Backfill Complete ===`);
    console.log(`This session: ${sessionOddsCount} game odds dates, ${sessionPropsCount} player prop events`);
    console.log(`Total game odds: ${finalOddsDates.size}/${allDates.length} dates`);
    console.log(`Total player props: ${finalPropDates.size}/${playerPropDates.length} dates`);
    if (errors.length > 0) {
      console.log(`\nErrors (${errors.length}):`);
      errors.slice(0, 10).forEach(e => console.log(`  ${e}`));
      if (errors.length > 10) console.log(`  ... and ${errors.length - 10} more`);
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

  "backtest:run": async (args) => {
    await runBacktest(args);
  },

  "backtest:existing": async (args) => {
    await backtestExisting(args);
  },

  "backtest:analyze": async (args) => {
    await analyzePattern(args);
  },

  "backtest:quick": async (args) => {
    await quickValidate(args);
  },

  "ingest:raw": async (args) => {
    const flags = parseFlags(args);
    const force = args.includes("--force");
    
    if (flags.season) {
      await ingestSeason(flags.season, { force });
    } else if (args.includes("--all")) {
      await ingestAllSeasons({ force });
    } else {
      console.error("Usage: ingest:raw --season 2024-25  or  ingest:raw --all");
      console.error("  --force    Re-ingest all games (ignore progress)");
      process.exit(1);
    }
  },

  "ingest:status": async () => {
    const status = await getIngestionStatus();
    console.log("\n=== Raw Data Ingestion Status ===\n");
    console.log("Season     | Raw Games | Ingested | In DB | Complete");
    console.log("-----------|-----------|----------|-------|----------");
    for (const [season, data] of Object.entries(status)) {
      console.log(
        `${season.padEnd(10)} | ${String(data.rawGames).padStart(9)} | ${String(data.ingestedGames).padStart(8)} | ${String(data.dbGames).padStart(5)} | ${data.percentComplete}%`
      );
    }
    console.log("");
  },

  "enrich:raw": async (args) => {
    const flags = parseFlags(args);
    
    if (flags.season) {
      await enrichSeason(flags.season);
    } else if (args.includes("--all")) {
      await enrichAllSeasons();
    } else {
      console.error("Usage: enrich:raw --season 2024-25  or  enrich:raw --all");
      console.error("  Enriches existing games with detailed stats from raw NBA JSON files");
      process.exit(1);
    }
  },

  "enrich:status": async () => {
    await getEnrichmentStatus();
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
