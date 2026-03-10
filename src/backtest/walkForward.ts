/**
 * Walk-Forward Validation Engine
 * 
 * Implements rolling window validation where patterns are discovered
 * on training seasons and validated on subsequent test season.
 * 
 * Example: Train on 2020-2022, Test on 2023
 *          Train on 2021-2023, Test on 2024
 */

import { prisma } from "../db/prisma.js";
import { scorePattern } from "../patterns/gamePatternScoring.js";
import { calculateSignificance } from "./significance.js";
import { simulatePnL, aggregatePnL } from "./plSimulation.js";
import { gradePattern } from "./grading.js";
import type {
  BacktestConfig,
  PatternCandidate,
  PatternTestResult,
  WalkForwardFold,
  WalkForwardResult,
  GameResult,
} from "./backtestTypes.js";

interface EventRow {
  gameId: string;
  season: number;
  eventKey: string;
  type: string;
  side: string;
}

interface GameInfo {
  id: string;
  date: Date;
  season: number;
}

/**
 * Run walk-forward validation across multiple folds
 */
export async function walkForwardValidation(
  config: BacktestConfig
): Promise<WalkForwardResult> {
  const startTime = Date.now();
  
  console.log("\n=== Walk-Forward Validation ===\n");
  console.log(`Config: minSample=${config.minSample}, minHitRate=${config.minHitRate}, maxLegs=${config.maxLegs}`);
  console.log(`Bet sizing: ${config.betSizing}, fraction: ${config.betFraction}`);
  
  const allSeasons = [...new Set([...config.trainSeasons, ...config.testSeasons])].sort();
  console.log(`Seasons in scope: ${allSeasons.join(", ")}`);

  console.log("\nLoading game events...");
  const allEvents: EventRow[] = await prisma.gameEvent.findMany({
    where: { season: { in: allSeasons } },
    select: { gameId: true, season: true, eventKey: true, type: true, side: true },
  });
  console.log(`  Loaded ${allEvents.length} events`);

  const games: GameInfo[] = await prisma.game.findMany({
    where: { season: { in: allSeasons } },
    select: { id: true, date: true, season: true },
  });
  const gameMap = new Map(games.map(g => [g.id, g]));
  console.log(`  Loaded ${games.length} games`);

  const folds: WalkForwardFold[] = [];
  const allRobustPatterns: Map<string, PatternTestResult[]> = new Map();

  for (let i = 0; i < config.testSeasons.length; i++) {
    const testSeason = config.testSeasons[i];
    const trainSeasons = config.trainSeasons.filter(s => s < testSeason);
    
    if (trainSeasons.length === 0) {
      console.log(`\nSkipping test season ${testSeason}: no training seasons available`);
      continue;
    }

    console.log(`\n--- Fold ${i + 1}: Train on ${trainSeasons.join(",")} → Test on ${testSeason} ---`);
    
    const fold = await runFold(
      allEvents,
      gameMap,
      trainSeasons,
      testSeason,
      config,
      i + 1
    );
    
    folds.push(fold);

    for (const result of fold.results) {
      if (!allRobustPatterns.has(result.patternKey)) {
        allRobustPatterns.set(result.patternKey, []);
      }
      allRobustPatterns.get(result.patternKey)!.push(result);
    }
  }

  const robustPatterns: PatternTestResult[] = [];
  for (const [patternKey, results] of allRobustPatterns) {
    if (results.length === folds.length && results.every(r => r.pnl.roi > 0)) {
      const bestResult = results.reduce((best, r) => 
        r.stats.zScoreVsChance > best.stats.zScoreVsChance ? r : best
      );
      robustPatterns.push(bestResult);
    }
  }
  robustPatterns.sort((a, b) => b.stats.zScoreVsChance - a.stats.zScoreVsChance);

  const allPnLs = folds.flatMap(f => f.results.map(r => r.pnl));
  const aggregatedPnL = aggregatePnL(allPnLs, config.startingBankroll);
  
  const totalTestHits = folds.reduce((sum, f) => 
    sum + f.results.reduce((s, r) => s + r.testHitCount, 0), 0);
  const totalTestSamples = folds.reduce((sum, f) => 
    sum + f.results.reduce((s, r) => s + r.testSampleSize, 0), 0);

  const result: WalkForwardResult = {
    config,
    folds,
    robustPatterns,
    aggregate: {
      totalBets: aggregatedPnL.totalBets,
      totalWins: aggregatedPnL.wins,
      totalLosses: aggregatedPnL.losses,
      netPnL: aggregatedPnL.netPnL,
      overallROI: aggregatedPnL.roi,
      avgHitRate: totalTestSamples > 0 ? totalTestHits / totalTestSamples : 0,
      avgEdge: totalTestSamples > 0 ? (totalTestHits / totalTestSamples) - 0.524 : 0,
      consistentlyProfitable: robustPatterns.length,
      degradedPatterns: folds.reduce((sum, f) => 
        sum + f.results.filter(r => r.testHitRate < r.trainHitRate * 0.85).length, 0),
    },
    runtimeMs: Date.now() - startTime,
    completedAt: new Date(),
  };

  printWalkForwardSummary(result);

  return result;
}

/**
 * Run a single fold of walk-forward validation
 */
async function runFold(
  allEvents: EventRow[],
  gameMap: Map<string, GameInfo>,
  trainSeasons: number[],
  testSeason: number,
  config: BacktestConfig,
  foldNumber: number
): Promise<WalkForwardFold> {
  const embargoDays = Math.max(0, Number(config.embargoDays ?? 0));
  const testSeasonEvents = allEvents.filter(e => e.season === testSeason);
  const testStartTs =
    testSeasonEvents
      .map((e) => gameMap.get(e.gameId)?.date?.getTime() ?? Number.POSITIVE_INFINITY)
      .reduce((mn, ts) => Math.min(mn, ts), Number.POSITIVE_INFINITY);
  const embargoCutoffTs = Number.isFinite(testStartTs)
    ? testStartTs - embargoDays * 86_400_000
    : Number.POSITIVE_INFINITY;
  const trainEvents = allEvents.filter((e) => {
    if (!trainSeasons.includes(e.season)) return false;
    if (embargoDays <= 0) return true;
    const gameTs = gameMap.get(e.gameId)?.date?.getTime();
    if (gameTs == null) return false;
    return gameTs < embargoCutoffTs;
  });
  const testEvents = testSeasonEvents;

  console.log(
    `  Train events: ${trainEvents.length}, Test events: ${testEvents.length}, embargoDays=${embargoDays}`,
  );

  const { conditionIndex: trainConditions, outcomeIndex: trainOutcomes, gameSeason: trainGameSeason } = 
    buildIndexes(trainEvents);
  const { conditionIndex: testConditions, outcomeIndex: testOutcomes, gameSeason: testGameSeason } = 
    buildIndexes(testEvents);

  const viableConditions: string[] = [];
  for (const [key, games] of trainConditions) {
    if (games.size >= config.minSample) {
      viableConditions.push(key);
    }
  }
  viableConditions.sort();
  console.log(`  ${viableConditions.length} viable conditions`);

  const combos = generateCombos(viableConditions, config.maxLegs);
  console.log(`  ${combos.length} condition combinations`);

  const totalTrainSeasons = trainSeasons.length;
  let maxSample = 0;
  for (const games of trainConditions.values()) {
    if (games.size > maxSample) maxSample = games.size;
  }

  const candidates: PatternCandidate[] = [];

  for (const combo of combos) {
    const sets = combo.map(key => trainConditions.get(key)!);
    sets.sort((a, b) => a.size - b.size);

    let intersection = new Set(sets[0]);
    for (let s = 1; s < sets.length; s++) {
      const next = new Set<string>();
      for (const id of intersection) {
        if (sets[s].has(id)) next.add(id);
      }
      intersection = next;
      if (intersection.size < config.minSample) break;
    }

    if (intersection.size < config.minSample) continue;

    const trainSampleSize = intersection.size;
    if (trainSampleSize > maxSample) maxSample = trainSampleSize;

    for (const [outcomeKey, outcomeGames] of trainOutcomes) {
      let hitCount = 0;
      const perSeason: Record<string, number> = {};

      for (const gameId of intersection) {
        if (outcomeGames.has(gameId)) {
          hitCount++;
          const season = trainGameSeason.get(gameId) ?? 0;
          const sKey = String(season);
          perSeason[sKey] = (perSeason[sKey] ?? 0) + 1;
        }
      }

      const hitRate = hitCount / trainSampleSize;
      if (hitRate < config.minHitRate) continue;

      const seasons = Object.keys(perSeason).length;
      if (seasons < config.minTrainSeasons) continue;

      const patternKey = [...combo, "->", outcomeKey].join("|");
      const scores = scorePattern({
        hitRate,
        sampleSize: trainSampleSize,
        seasons,
        totalSeasons: totalTrainSeasons,
        maxSample,
        perSeason,
      });

      candidates.push({
        patternKey,
        conditions: combo,
        outcome: outcomeKey,
        trainSampleSize,
        trainHitCount: hitCount,
        trainHitRate: hitRate,
        trainSeasons: seasons,
        trainPerSeason: perSeason,
        confidenceScore: scores.confidenceScore,
        valueScore: scores.valueScore,
        trainGameIds: Array.from(intersection),
      });
    }
  }

  console.log(`  ${candidates.length} candidate patterns from training`);

  candidates.sort((a, b) => b.confidenceScore - a.confidenceScore || b.valueScore - a.valueScore);
  const topCandidates = candidates.slice(0, 500);

  const results: PatternTestResult[] = [];
  let passed = 0;

  for (const candidate of topCandidates) {
    const testSetSets = candidate.conditions.map(key => testConditions.get(key) ?? new Set<string>());
    
    if (testSetSets.some(s => s.size === 0)) continue;

    testSetSets.sort((a, b) => a.size - b.size);
    let testIntersection = new Set(testSetSets[0]);
    for (let s = 1; s < testSetSets.length; s++) {
      const next = new Set<string>();
      for (const id of testIntersection) {
        if (testSetSets[s].has(id)) next.add(id);
      }
      testIntersection = next;
    }

    if (testIntersection.size < 5) continue;

    const testOutcomeGames = testOutcomes.get(candidate.outcome) ?? new Set<string>();
    let testHitCount = 0;
    const testPerSeason: Record<string, number> = {};
    const testGameResults: GameResult[] = [];

    for (const gameId of testIntersection) {
      const hit = testOutcomeGames.has(gameId);
      const gameInfo = gameMap.get(gameId);
      
      if (hit) {
        testHitCount++;
        const sKey = String(testSeason);
        testPerSeason[sKey] = (testPerSeason[sKey] ?? 0) + 1;
      }

      testGameResults.push({
        gameId,
        season: testSeason,
        date: gameInfo?.date ?? new Date(),
        hit,
      });
    }

    const testHitRate = testHitCount / testIntersection.size;
    const stats = calculateSignificance(
      candidate.trainHitCount,
      candidate.trainSampleSize,
      testHitCount,
      testIntersection.size
    );

    const pnl = simulatePnL(testGameResults, config, candidate.trainHitRate);
    const grade = gradePattern(candidate.trainHitRate, testHitRate, stats, pnl);

    if (stats.isSignificant || pnl.roi > 0) {
      passed++;
    }

    results.push({
      patternKey: candidate.patternKey,
      conditions: candidate.conditions,
      outcome: candidate.outcome,
      trainHitRate: candidate.trainHitRate,
      trainSampleSize: candidate.trainSampleSize,
      testSampleSize: testIntersection.size,
      testHitCount,
      testHitRate,
      testSeasons: Object.keys(testPerSeason).length,
      testPerSeason,
      testGameResults,
      stats,
      pnl,
      grade: grade.grade,
    });
  }

  results.sort((a, b) => {
    const gradeOrder = { A: 0, B: 1, C: 2, D: 3, F: 4 };
    return gradeOrder[a.grade] - gradeOrder[b.grade] || b.pnl.roi - a.pnl.roi;
  });

  const foldPnL = results.reduce((sum, r) => sum + r.pnl.netPnL, 0);
  const foldTotalBets = results.reduce((sum, r) => sum + r.pnl.totalBets, 0);
  const foldROI = foldTotalBets > 0 
    ? foldPnL / (foldTotalBets * config.startingBankroll * config.betFraction)
    : 0;

  console.log(`  Tested ${topCandidates.length} patterns, ${passed} passed validation`);
  console.log(`  Fold P&L: $${foldPnL.toFixed(2)}, ROI: ${(foldROI * 100).toFixed(2)}%`);

  return {
    foldNumber,
    trainSeasons,
    testSeason,
    patternsDiscovered: candidates.length,
    patternsPassed: passed,
    results,
    foldPnL,
    foldROI,
  };
}

function buildIndexes(events: EventRow[]): {
  conditionIndex: Map<string, Set<string>>;
  outcomeIndex: Map<string, Set<string>>;
  gameSeason: Map<string, number>;
} {
  const conditionIndex = new Map<string, Set<string>>();
  const outcomeIndex = new Map<string, Set<string>>();
  const gameSeason = new Map<string, number>();

  for (const evt of events) {
    gameSeason.set(evt.gameId, evt.season);
    const compositeKey = `${evt.eventKey}:${evt.side}`;

    if (evt.type === "condition") {
      const set = conditionIndex.get(compositeKey) ?? new Set();
      set.add(evt.gameId);
      conditionIndex.set(compositeKey, set);
    } else {
      const set = outcomeIndex.get(compositeKey) ?? new Set();
      set.add(evt.gameId);
      outcomeIndex.set(compositeKey, set);
    }
  }

  return { conditionIndex, outcomeIndex, gameSeason };
}

function generateCombos(conditions: string[], maxLegs: number): string[][] {
  const combos: string[][] = [];
  
  for (let i = 0; i < conditions.length; i++) {
    combos.push([conditions[i]]);
    
    if (maxLegs >= 2) {
      for (let j = i + 1; j < conditions.length; j++) {
        combos.push([conditions[i], conditions[j]]);
        
        if (maxLegs >= 3) {
          for (let k = j + 1; k < conditions.length; k++) {
            combos.push([conditions[i], conditions[j], conditions[k]]);
          }
        }
      }
    }
  }
  
  return combos;
}

function printWalkForwardSummary(result: WalkForwardResult): void {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("                 WALK-FORWARD VALIDATION SUMMARY            ");
  console.log("═══════════════════════════════════════════════════════════\n");
  
  console.log(`Folds completed: ${result.folds.length}`);
  console.log(`Runtime: ${(result.runtimeMs / 1000).toFixed(1)}s\n`);
  
  console.log("Fold-by-Fold Results:");
  console.log("─────────────────────────────────────────────────────────────");
  console.log(" Fold | Train Seasons | Test | Patterns | Passed | ROI");
  console.log("─────────────────────────────────────────────────────────────");
  
  for (const fold of result.folds) {
    console.log(
      ` ${String(fold.foldNumber).padStart(4)} | ` +
      `${fold.trainSeasons.join(",").padEnd(13)} | ` +
      `${String(fold.testSeason).padEnd(4)} | ` +
      `${String(fold.patternsDiscovered).padStart(8)} | ` +
      `${String(fold.patternsPassed).padStart(6)} | ` +
      `${(fold.foldROI * 100).toFixed(1)}%`
    );
  }
  console.log("─────────────────────────────────────────────────────────────\n");

  console.log("Aggregate Performance:");
  console.log(`  Total Bets:     ${result.aggregate.totalBets}`);
  console.log(`  Win/Loss:       ${result.aggregate.totalWins}/${result.aggregate.totalLosses}`);
  console.log(`  Avg Hit Rate:   ${(result.aggregate.avgHitRate * 100).toFixed(1)}%`);
  console.log(`  Avg Edge:       ${(result.aggregate.avgEdge * 100).toFixed(2)}%`);
  console.log(`  Net P&L:        $${result.aggregate.netPnL.toFixed(2)}`);
  console.log(`  Overall ROI:    ${(result.aggregate.overallROI * 100).toFixed(2)}%\n`);

  console.log("Pattern Quality:");
  console.log(`  Consistently Profitable: ${result.aggregate.consistentlyProfitable}`);
  console.log(`  Degraded in Testing:     ${result.aggregate.degradedPatterns}\n`);

  if (result.robustPatterns.length > 0) {
    console.log("Top 5 Robust Patterns (profitable in all folds):");
    console.log("─────────────────────────────────────────────────────────────");
    
    for (const pattern of result.robustPatterns.slice(0, 5)) {
      console.log(`\n  ${pattern.conditions.join(" + ")}  →  ${pattern.outcome}`);
      console.log(`    Train: ${(pattern.trainHitRate * 100).toFixed(1)}% (n=${pattern.trainSampleSize})`);
      console.log(`    Test:  ${(pattern.testHitRate * 100).toFixed(1)}% (n=${pattern.testSampleSize})`);
      console.log(`    Z-score: ${pattern.stats.zScoreVsChance.toFixed(2)}, ROI: ${(pattern.pnl.roi * 100).toFixed(1)}%`);
      console.log(`    Grade: ${pattern.grade}`);
    }
  }

  console.log("\n═══════════════════════════════════════════════════════════\n");
}
