import * as fs from "fs/promises";
import { prisma } from "@bluey/db";
import { syncGamesForDate, syncGamesForRange, backfillGameExternalIds } from "@bluey/core/ingest/syncGames";
import { getEasternDateFromUtc, dateStringToUtcMidday } from "@bluey/core/ingest/utils";
import { syncNbaStatsForDate, syncNbaStatsForDateRange, syncUpcomingFromNba } from "@bluey/core/ingest/syncNbaStats";
import { syncOddsLive, syncOddsForDate } from "@bluey/core/ingest/syncOdds";
import { syncPlayerPropsLive } from "@bluey/core/ingest/syncPlayerProps";
import { syncInjuries } from "@bluey/core/ingest/syncInjuries";
import { syncLineups } from "@bluey/core/ingest/syncLineups";
import { buildDailyDataBundles } from "@bluey/core/ingest/buildDailyDataBundles";
import { ingestDayBundles } from "@bluey/core/ingest/ingestDayBundles";
import { normalizePlayerMinutes } from "@bluey/core/ingest/normalizePlayerMinutes";
import { syncMissingDays } from "@bluey/core/ingest/syncMissingDays";
import { getPlayerTotals } from "@bluey/core/stats/playerRollup";
import { getTeamTotals } from "@bluey/core/stats/teamRollup";
import {
  buildFeatureBins,
  buildQuantizedGameFeatures,
  discoverPatternsV2,
  validatePatternsV2,
  monitorPatternDecay,
  analyzePatternsV2,
  analyzeV2Bankroll,
  evaluatePatternsV2Purged,
} from "@bluey/core/patterns/discoveryV2";
import { evaluatePatternIdeas } from "@bluey/core/patterns/patternIdeas";
import { trainMetaModel, predictMetaScore, evaluateMetaModelMonthly, evaluateMetaModelPurged } from "@bluey/core/patterns/metaModel";
import { coverageReport } from "@bluey/core/reports/coverage";
import { reportProbabilityQuality } from "@bluey/core/reports/probabilityQuality";
import { playerProfile } from "@bluey/core/profiles/playerProfile";
import { teamProfile } from "@bluey/core/profiles/teamProfile";
import { buildGameContext } from "@bluey/core/features/buildGameContext";
import { buildGameEvents } from "@bluey/core/features/buildGameEvents";
import { predictGames, predictPlayers } from "@bluey/core/features/predictGames";
import { resolvePredictionLogs, reportPredictionAccuracy } from "@bluey/core/features/predictionAccuracy";
import { dailyPicks } from "@bluey/core/features/dailyPicks";
import { runBacktest, backtestExisting, analyzePattern, quickValidate } from "@bluey/core/backtest/backtest";
import { analyzeSuggestedPlayLedger } from "@bluey/core/backtest/suggestedPlayLedger";
import { updateSuggestedPlayClv } from "@bluey/core/backtest/updateClvSnapshots";
import { backfillSuggestedLedger } from "@bluey/core/backtest/backfillSuggestedLedger";
import { runPlayerPointsModel } from "@bluey/core/ml/playerPointsModel";
import {
  snapshotModelVersion,
  listModelVersions,
  activateModelVersion,
  deactivateModelVersion,
  deleteModelVersion,
} from "@bluey/core/patterns/modelVersion";
import type { RollupFilters } from "@bluey/core/stats/filters";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--") continue;
    if (arg.startsWith("--") && i + 1 < args.length) {
      const value = args[i + 1];
      if (!value.startsWith("--")) {
        flags[arg.slice(2)] = value;
        i++;
      }
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
    if (flags.date) {
      await syncNbaStatsForDate(flags.date);
      return;
    }
    if (flags.from && flags.to) {
      await syncNbaStatsForDateRange(flags.from, flags.to);
      return;
    }
    // No args: auto-detect gap and backfill using nba_api (no BallDontLie)
    const lastGameWithStats = await prisma.game.findFirst({
      where: { playerStats: { some: {} } },
      orderBy: { date: "desc" },
      select: { date: true },
    });
    const fromDate = lastGameWithStats?.date
      ? new Date(lastGameWithStats.date.getTime() + 86400000)
      : new Date(Date.now() - 7 * 86400000); // default: 7 days ago if empty
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const fromStr = fromDate.toISOString().slice(0, 10);
    const toStr = yesterday.toISOString().slice(0, 10);

    if (fromDate > yesterday) {
      console.log("Stats already up to date (no gap to backfill).");
      return;
    }

    console.log(`\n=== Syncing stats from ${fromStr} to ${toStr} (nba_api) ===\n`);
    await syncNbaStatsForDateRange(fromStr, toStr);
    console.log("\n=== Stats sync complete ===");
  },

  "sync:nba-stats": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncNbaStatsForDate(flags.date);
    } else if (flags.from && flags.to) {
      await syncNbaStatsForDateRange(flags.from, flags.to);
    } else {
      // No args: same as sync:stats (auto backfill)
      await (COMMANDS["sync:stats"] as (args: string[]) => Promise<void>)([]);
    }
  },

  "sync:odds": async (args) => {
    const flags = parseFlags(args);
    if (flags.date) {
      await syncOddsForDate(flags.date);
      return;
    }
    // No date: fetch live odds for today, then backfill historical from last game-with-odds date through yesterday
    console.log("Fetching live odds...");
    await syncOddsLive();

    const lastGameWithOdds = await prisma.game.findFirst({
      where: { odds: { some: {} } },
      orderBy: { date: "desc" },
      select: { date: true },
    });
    if (!lastGameWithOdds?.date) return;

    const lastDate = new Date(lastGameWithOdds.date);
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    const fromDate = new Date(lastDate.getTime() + 86400000);
    if (fromDate > yesterday) return;

    const fromStr = fromDate.toISOString().slice(0, 10);
    const toStr = yesterday.toISOString().slice(0, 10);
    console.log(`\nBackfilling odds from ${fromStr} to ${toStr}...`);

    let current = new Date(fromStr + "T00:00:00Z");
    const toDate = new Date(toStr + "T00:00:00Z");

    while (current <= toDate) {
      const d = current.toISOString().slice(0, 10);
      try {
        await syncOddsForDate(d);
      } catch (err) {
        console.warn(`  Odds for ${d} failed:`, (err as Error).message);
      }
      current.setDate(current.getDate() + 1);
    }
    console.log("Odds sync complete.");
  },

  "sync:player-props": async () => {
    await syncPlayerPropsLive();
  },

  "sync:injuries": async (args) => {
    await syncInjuries(args);
  },

  "sync:lineups": async (args) => {
    await syncLineups(args);
  },

  "build:day-bundles": async (args) => {
    await buildDailyDataBundles(args);
  },

  "ingest:day-bundles": async (args) => {
    await ingestDayBundles(args);
  },

  "repair:minutes": async (args) => {
    await normalizePlayerMinutes(args);
  },

  "sync:fill-missing-days": async (args) => {
    await syncMissingDays(args);
  },

  "sync:catchup": async () => {
    console.log("\n=== Sync Catchup: backfill stats and odds to current ===\n");
    console.log("Step 1/2: Syncing stats...");
    const statsArgs = [] as string[];
    await (COMMANDS["sync:stats"] as (args: string[]) => Promise<void>)(statsArgs);
    console.log("\nStep 2/2: Syncing odds...");
    try {
      await (COMMANDS["sync:odds"] as (args: string[]) => Promise<void>)([]);
    } catch (err) {
      console.warn("Odds sync failed (non-fatal):", (err as Error).message);
    }
    console.log("\n=== Catchup complete ===");
  },

  "sync:daily": async () => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().slice(0, 10);

    console.log(`\n=== Daily Sync for ${dateStr} (nba_api + Odds API) ===\n`);

    console.log("Step 1/2: Syncing player stats (nba_api)...");
    await syncNbaStatsForDate(dateStr);

    console.log("\nStep 2/2: Syncing odds (Odds API)...");
    try {
      await syncOddsForDate(dateStr);
    } catch (err) {
      console.warn("  Odds sync failed (non-fatal):", (err as Error).message);
    }

    console.log("\n=== Daily sync complete ===");
  },

  "sync:backfill-external-ids": async () => {
    await backfillGameExternalIds();
  },

  "sync:upcoming": async (args) => {
    const flags = parseFlags(args);
    const dateStr = flags.date || new Date().toISOString().slice(0, 10);
    const skipExisting = (flags["skip-existing"] ?? "false") === "true";

    console.log(`\n=== Sync Upcoming Games for ${dateStr} (nba_api) ===\n`);

    if (skipExisting) {
      const gameDate = dateStringToUtcMidday(dateStr);
      const gamesForDate = await prisma.game.findMany({
        where: { date: gameDate },
        select: { homeScore: true, awayScore: true, status: true },
      });
      const allFinal =
        gamesForDate.length > 0 &&
        gamesForDate.every(
          (g) =>
            (g.homeScore ?? 0) > 0 &&
            (g.awayScore ?? 0) > 0 &&
            g.status?.includes("Final"),
        );
      if (allFinal) {
        console.log(`Skip: ${gamesForDate.length} games already have final scores for ${dateStr}.`);
        return;
      }
    }

    console.log("Step 1/2: Syncing upcoming games...");
    await syncUpcomingFromNba(dateStr);

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
    
    // Step 1: Sync upcoming games (nba_api)
    console.log("Step 1/3: Syncing upcoming games...");
    await syncUpcomingFromNba(dateStr);
    
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
      console.log(`\n=== Backfilling season ${season} (nba_api) ===\n`);
      const fromStr = `${season}-10-01`;
      const toStr = `${season + 1}-06-30`;
      const totalStats = await syncNbaStatsForDateRange(fromStr, toStr);
      console.log(`\n=== Season ${season} backfill complete: ${totalStats} total stats ===`);
    }
  },

  "backfill:odds": async (args) => {
    const { fetchHistoricalOdds, fetchHistoricalEventOdds } = await import("@bluey/core/api/oddsApi");
    
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
            // Only get events that tip on this date (Eastern - NBA game day)
            const sameDay = oddsData.filter(e => e.commence_time && getEasternDateFromUtc(new Date(e.commence_time)) === dateStr);
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

  "report:coverage": async () => {
    await coverageReport();
  },

  "report:probability-quality": async (args) => {
    await reportProbabilityQuality(args);
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

  "build:feature-bins": async (args) => {
    await buildFeatureBins(args);
  },

  "build:quantized-game-features": async (args) => {
    await buildQuantizedGameFeatures(args);
  },

  "search:discover-patterns-v2": async (args) => {
    await discoverPatternsV2(args);
  },

  "validate:patterns-v2": async (args) => {
    await validatePatternsV2(args);
  },

  "monitor:pattern-decay": async (args) => {
    await monitorPatternDecay(args);
  },

  "analyze:patterns-v2": async (args) => {
    await analyzePatternsV2(args);
  },
  "analyze:v2-bankroll": async (args) => {
    await analyzeV2Bankroll(args);
  },

  "evaluate:patterns-v2-purged": async (args) => {
    await evaluatePatternsV2Purged(args);
  },
  "evaluate:pattern-ideas": async (args) => {
    await evaluatePatternIdeas(args);
  },

  "train:meta-model": async (args) => {
    await trainMetaModel(args);
  },

  "predict:meta-score": async (args) => {
    await predictMetaScore(args);
  },

  "evaluate:meta-monthly": async (args) => {
    await evaluateMetaModelMonthly(args);
  },

  "evaluate:meta-purged": async (args) => {
    await evaluateMetaModelPurged(args);
  },

  "predict:players": async (args) => {
    await predictPlayers(args);
  },

  "picks:daily": async (args) => {
    await dailyPicks(args);
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

  "analyze:suggested-ledger": async (args) => {
    await analyzeSuggestedPlayLedger(args);
  },
  "backfill:suggested-ledger": async (args) => {
    await backfillSuggestedLedger(args);
  },

  "update:clv-snapshots": async (args) => {
    await updateSuggestedPlayClv(args);
  },

  "snapshot:model-version": async (args) => {
    await snapshotModelVersion(args);
  },

  "list:model-versions": async (_args) => {
    await listModelVersions();
  },

  "activate:model-version": async (args) => {
    await activateModelVersion(args);
  },

  "deactivate:model-version": async (_args) => {
    await deactivateModelVersion();
  },

  "delete:model-version": async (args) => {
    await deleteModelVersion(args);
  },

  "resolve:predictions": async (args) => {
    await resolvePredictionLogs(args);
  },

  "report:accuracy": async (args) => {
    await reportPredictionAccuracy(args);
  },

  "ml:player-points": async (args) => {
    await runPlayerPointsModel(args);
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
    console.log("\nUsage: bun run cli <command> [options]");
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
