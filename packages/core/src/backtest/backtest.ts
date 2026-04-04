/**
 * Main Backtest Orchestrator
 * 
 * Entry point for running backtests on game patterns.
 * Provides CLI interface and coordinates all backtest modules.
 */

import { prisma } from "@bluey/db";
import { walkForwardValidation } from "./walkForward";
import { calculateSignificance } from "./significance";
import { simulatePnL, aggregatePnL, americanToDecimal, monteCarloRuin } from "./plSimulation";
import { gradePattern, formatGrade } from "./grading";
import type {
  BacktestConfig,
  WalkForwardResult,
  PatternTestResult,
} from "./backtestTypes";

const DEFAULT_CONFIG: BacktestConfig = {
  trainSeasons: [],
  testSeasons: [],
  minSample: 15,
  minHitRate: 0.58,
  maxLegs: 3,
  minTrainSeasons: 1,
  startingBankroll: 10000,
  betSizing: "flat",
  betFraction: 0.02,
  standardOdds: -110,
  embargoDays: 0,
};

/**
 * Main backtest entry point - runs walk-forward validation
 */
export async function runBacktest(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  
  const availableSeasons = await getAvailableSeasons();
  console.log(`\nAvailable seasons with data: ${availableSeasons.join(", ")}`);
  
  let trainSeasons: number[];
  let testSeasons: number[];
  
  if (flags.trainSeasons) {
    trainSeasons = flags.trainSeasons.split(",").map(Number);
  } else if (availableSeasons.length >= 2) {
    trainSeasons = availableSeasons.slice(0, -1);
  } else {
    console.error("Not enough seasons for walk-forward validation. Need at least 2 seasons.");
    process.exit(1);
  }
  
  if (flags.testSeasons) {
    testSeasons = flags.testSeasons.split(",").map(Number);
  } else if (availableSeasons.length >= 2) {
    testSeasons = availableSeasons.slice(1);
  } else {
    console.error("Not enough seasons for walk-forward validation. Need at least 2 seasons.");
    process.exit(1);
  }

  const config: BacktestConfig = {
    ...DEFAULT_CONFIG,
    trainSeasons,
    testSeasons,
    minSample: flags.minSample ? Number(flags.minSample) : DEFAULT_CONFIG.minSample,
    minHitRate: flags.minHitRate ? Number(flags.minHitRate) : DEFAULT_CONFIG.minHitRate,
    maxLegs: flags.maxLegs ? Number(flags.maxLegs) : DEFAULT_CONFIG.maxLegs,
    minTrainSeasons: flags.minTrainSeasons ? Number(flags.minTrainSeasons) : DEFAULT_CONFIG.minTrainSeasons,
    startingBankroll: flags.bankroll ? Number(flags.bankroll) : DEFAULT_CONFIG.startingBankroll,
    betSizing: (flags.betSizing as "flat" | "kelly" | "halfKelly") ?? DEFAULT_CONFIG.betSizing,
    betFraction: flags.betFraction ? Number(flags.betFraction) : DEFAULT_CONFIG.betFraction,
    standardOdds: flags.odds ? Number(flags.odds) : DEFAULT_CONFIG.standardOdds,
    embargoDays: flags["embargo-days"] ? Number(flags["embargo-days"]) : DEFAULT_CONFIG.embargoDays,
  };

  const result = await walkForwardValidation(config);
  
  if (flags.save) {
    await saveBacktestResults(result);
    console.log("Results saved to database.");
  }
}

/**
 * Backtest existing patterns from the database
 */
export async function backtestExisting(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  
  console.log("\n=== Backtesting Existing Patterns ===\n");
  
  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
    take: flags.limit ? Number(flags.limit) : 100,
    include: {
      hits: {
        include: {
          game: {
            select: { id: true, date: true, season: true },
          },
        },
      },
    },
  });
  
  console.log(`Loaded ${patterns.length} patterns to analyze\n`);
  
  const availableSeasons = await getAvailableSeasons();
  if (availableSeasons.length < 2) {
    console.error("Need at least 2 seasons for backtesting");
    return;
  }
  
  const trainSeasons = flags.trainSeasons 
    ? flags.trainSeasons.split(",").map(Number)
    : availableSeasons.slice(0, -1);
  const testSeason = flags.testSeason 
    ? Number(flags.testSeason)
    : availableSeasons[availableSeasons.length - 1];
  
  console.log(`Training seasons: ${trainSeasons.join(", ")}`);
  console.log(`Test season: ${testSeason}\n`);
  
  const results: PatternTestResult[] = [];
  
  for (const pattern of patterns) {
    const trainHits = pattern.hits.filter(h => trainSeasons.includes(h.season));
    const testHits = pattern.hits.filter(h => h.season === testSeason);
    
    if (testHits.length < 5) continue;
    
    const trainHitCount = trainHits.filter(h => h.hit).length;
    const testHitCount = testHits.filter(h => h.hit).length;
    const testHitRate = testHitCount / testHits.length;
    
    const stats = calculateSignificance(
      trainHitCount,
      trainHits.length,
      testHitCount,
      testHits.length
    );
    
    const testGameResults = testHits.map(h => ({
      gameId: h.gameId,
      season: h.season,
      date: h.game.date,
      hit: h.hit,
    }));
    
    const config: BacktestConfig = {
      ...DEFAULT_CONFIG,
      trainSeasons,
      testSeasons: [testSeason],
    };
    
    const pnl = simulatePnL(testGameResults, config, pattern.hitRate);
    const grade = gradePattern(pattern.hitRate, testHitRate, stats, pnl);
    
    const testPerSeason: Record<string, number> = {};
    for (const h of testHits.filter(h => h.hit)) {
      const sKey = String(h.season);
      testPerSeason[sKey] = (testPerSeason[sKey] ?? 0) + 1;
    }
    
    results.push({
      patternKey: pattern.patternKey,
      conditions: pattern.conditions,
      outcome: pattern.outcome,
      trainHitRate: pattern.hitRate,
      trainSampleSize: trainHits.length,
      testSampleSize: testHits.length,
      testHitCount,
      testHitRate,
      testSeasons: 1,
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
  
  printBacktestResults(results, testSeason);
}

/**
 * Analyze a single pattern in detail
 */
export async function analyzePattern(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const patternKey = flags.pattern || args[0];
  
  if (!patternKey) {
    console.error("Usage: backtest:analyze --pattern <patternKey>");
    return;
  }
  
  console.log(`\n=== Pattern Analysis: ${patternKey} ===\n`);
  
  const pattern = await prisma.gamePattern.findUnique({
    where: { patternKey },
    include: {
      hits: {
        include: {
          game: {
            select: { id: true, date: true, season: true },
          },
        },
        orderBy: { game: { date: "asc" } },
      },
    },
  });
  
  if (!pattern) {
    console.error("Pattern not found");
    return;
  }
  
  console.log("Pattern Details:");
  console.log(`  Conditions: ${pattern.conditions.join(" + ")}`);
  console.log(`  Outcome: ${pattern.outcome}`);
  console.log(`  Overall: ${(pattern.hitRate * 100).toFixed(1)}% (${pattern.hitCount}/${pattern.sampleSize})`);
  console.log(`  Seasons: ${pattern.seasons}`);
  console.log(`  Confidence Score: ${pattern.confidenceScore?.toFixed(3) ?? "N/A"}`);
  console.log(`  Value Score: ${pattern.valueScore?.toFixed(3) ?? "N/A"}\n`);
  
  console.log("Per-Season Breakdown:");
  const perSeason = pattern.perSeason as Record<string, number>;
  const seasons = Object.keys(perSeason).sort();
  
  for (const season of seasons) {
    const seasonHits = pattern.hits.filter(h => String(h.season) === season);
    const hitCount = seasonHits.filter(h => h.hit).length;
    const total = seasonHits.length;
    const rate = total > 0 ? (hitCount / total * 100).toFixed(1) : "N/A";
    console.log(`  ${season}: ${rate}% (${hitCount}/${total})`);
  }
  
  const availableSeasons = [...new Set(pattern.hits.map(h => h.season))].sort();
  
  if (availableSeasons.length >= 2) {
    console.log("\n\nWalk-Forward Analysis (leave-one-out):");
    console.log("─".repeat(60));
    
    for (const testSeason of availableSeasons) {
      const trainSeasons = availableSeasons.filter(s => s !== testSeason);
      const trainHits = pattern.hits.filter(h => trainSeasons.includes(h.season));
      const testHits = pattern.hits.filter(h => h.season === testSeason);
      
      if (trainHits.length < 10 || testHits.length < 5) continue;
      
      const trainHitCount = trainHits.filter(h => h.hit).length;
      const trainHitRate = trainHitCount / trainHits.length;
      const testHitCount = testHits.filter(h => h.hit).length;
      const testHitRate = testHitCount / testHits.length;
      
      const stats = calculateSignificance(trainHitCount, trainHits.length, testHitCount, testHits.length);
      
      const testGameResults = testHits.map(h => ({
        gameId: h.gameId,
        season: h.season,
        date: h.game.date,
        hit: h.hit,
      }));
      
      const config: BacktestConfig = { ...DEFAULT_CONFIG, trainSeasons, testSeasons: [testSeason] };
      const pnl = simulatePnL(testGameResults, config, trainHitRate);
      const grade = gradePattern(trainHitRate, testHitRate, stats, pnl);
      
      console.log(`\n  Test Season ${testSeason} (train on ${trainSeasons.join(",")}):`);
      console.log(`    Train: ${(trainHitRate * 100).toFixed(1)}% (n=${trainHits.length})`);
      console.log(`    Test:  ${(testHitRate * 100).toFixed(1)}% (n=${testHits.length})`);
      console.log(`    Z-score vs chance: ${stats.zScoreVsChance.toFixed(2)} (p=${stats.pValueVsChance.toFixed(3)})`);
      console.log(`    Z-score vs train:  ${stats.zScoreVsTrain.toFixed(2)} (p=${stats.pValueVsTrain.toFixed(3)})`);
      console.log(`    ROI: ${(pnl.roi * 100).toFixed(1)}%`);
      console.log(`    Grade: ${formatGrade(grade.grade)}`);
    }
  }
  
  const winProb = pattern.hitRate;
  const decimalOdds = americanToDecimal(-110);
  const mc = monteCarloRuin(winProb, decimalOdds, 0.02, 100);
  
  console.log("\n\nRisk Analysis (100 bet simulation):");
  console.log(`  Ruin probability: ${(mc.ruinProb * 100).toFixed(1)}%`);
  console.log(`  Median final bankroll: ${(mc.medianFinal * 100).toFixed(0)}% of start`);
  console.log(`  5th percentile: ${(mc.p5Final * 100).toFixed(0)}%`);
  console.log(`  95th percentile: ${(mc.p95Final * 100).toFixed(0)}%`);
  
  console.log("\n");
}

/**
 * Quick validation report for patterns
 */
export async function quickValidate(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const limit = flags.limit ? Number(flags.limit) : 20;
  
  console.log("\n=== Quick Pattern Validation ===\n");
  
  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
    take: limit,
    include: {
      hits: {
        include: {
          game: { select: { date: true, season: true } },
        },
      },
    },
  });
  
  const availableSeasons = await getAvailableSeasons();
  const latestSeason = availableSeasons[availableSeasons.length - 1];
  
  console.log(`Validating top ${patterns.length} patterns against latest season (${latestSeason}):\n`);
  console.log("Pattern                                           | Train  | Test   | Z-score | Grade");
  console.log("─".repeat(85));
  
  for (const pattern of patterns) {
    const trainHits = pattern.hits.filter(h => h.season !== latestSeason);
    const testHits = pattern.hits.filter(h => h.season === latestSeason);
    
    if (testHits.length < 3) {
      const shortKey = pattern.patternKey.slice(0, 48).padEnd(48);
      console.log(`${shortKey} | ${(pattern.hitRate * 100).toFixed(0).padStart(4)}%  | N/A    | N/A     | N/A`);
      continue;
    }
    
    const trainHitCount = trainHits.filter(h => h.hit).length;
    const testHitCount = testHits.filter(h => h.hit).length;
    const testHitRate = testHitCount / testHits.length;
    
    const stats = calculateSignificance(trainHitCount, trainHits.length, testHitCount, testHits.length);
    
    const testGameResults = testHits.map(h => ({
      gameId: h.gameId,
      season: h.season,
      date: h.game.date,
      hit: h.hit,
    }));
    
    const config: BacktestConfig = { ...DEFAULT_CONFIG, trainSeasons: availableSeasons.slice(0, -1), testSeasons: [latestSeason] };
    const pnl = simulatePnL(testGameResults, config, pattern.hitRate);
    const grade = gradePattern(pattern.hitRate, testHitRate, stats, pnl);
    
    const shortKey = pattern.patternKey.slice(0, 48).padEnd(48);
    const trainStr = `${(pattern.hitRate * 100).toFixed(0)}%`.padStart(4);
    const testStr = `${(testHitRate * 100).toFixed(0)}%`.padStart(4);
    const zStr = stats.zScoreVsChance.toFixed(1).padStart(6);
    
    console.log(`${shortKey} | ${trainStr}  | ${testStr}  | ${zStr}  | ${formatGrade(grade.grade)}`);
  }
  
  console.log("\n");
}

async function getAvailableSeasons(): Promise<number[]> {
  const seasons = await prisma.gameEvent.findMany({
    select: { season: true },
    distinct: ["season"],
    orderBy: { season: "asc" },
  });
  
  return seasons.map(s => s.season);
}

async function saveBacktestResults(result: WalkForwardResult): Promise<void> {
  console.log("\nSaving backtest results...");
  
  const allResults = result.folds.flatMap(f => f.results);
  const aggregatedPnL = result.folds.reduce((sum, f) => sum + f.foldPnL, 0);
  const totalBets = result.folds.reduce((sum, f) => 
    sum + f.results.reduce((s, r) => s + r.pnl.totalBets, 0), 0);
  const aggregatedROI = totalBets > 0 
    ? aggregatedPnL / (totalBets * result.config.startingBankroll * result.config.betFraction)
    : 0;
  
  const run = await prisma.backtestRun.create({
    data: {
      name: `Walk-Forward ${result.config.testSeasons.join(",")}`,
      trainSeasons: result.config.trainSeasons,
      testSeasons: result.config.testSeasons,
      config: result.config as object,
      totalPatterns: result.folds.reduce((sum, f) => sum + f.patternsDiscovered, 0),
      passingPatterns: result.folds.reduce((sum, f) => sum + f.patternsPassed, 0),
      aggregatePnL: aggregatedPnL,
      aggregateROI: aggregatedROI,
      avgHitRate: result.aggregate.avgHitRate,
      runtimeMs: result.runtimeMs,
    },
  });
  
  const uniquePatterns = new Map<string, PatternTestResult>();
  for (const r of allResults) {
    const existing = uniquePatterns.get(r.patternKey);
    if (!existing || r.stats.zScoreVsChance > existing.stats.zScoreVsChance) {
      uniquePatterns.set(r.patternKey, r);
    }
  }
  
  const resultsToSave = Array.from(uniquePatterns.values()).map(r => ({
    runId: run.id,
    patternKey: r.patternKey,
    conditions: r.conditions,
    outcome: r.outcome,
    trainHitRate: r.trainHitRate,
    trainSampleSize: r.trainSampleSize,
    testHitRate: r.testHitRate,
    testSampleSize: r.testSampleSize,
    zScoreVsChance: r.stats.zScoreVsChance,
    pValueVsChance: r.stats.pValueVsChance,
    zScoreVsTrain: r.stats.zScoreVsTrain,
    pValueVsTrain: r.stats.pValueVsTrain,
    isSignificant: r.stats.isSignificant,
    isConsistent: r.stats.isConsistentWithTrain,
    pnlROI: r.pnl.roi,
    pnlNetPnL: r.pnl.netPnL,
    maxDrawdown: r.pnl.maxDrawdown,
    sharpeRatio: r.pnl.sharpeRatio,
    grade: r.grade,
    gradeScore: { A: 90, B: 75, C: 60, D: 45, F: 30 }[r.grade] ?? 0,
  }));
  
  if (resultsToSave.length > 0) {
    await prisma.backtestResult.createMany({
      data: resultsToSave,
      skipDuplicates: true,
    });
  }
  
  console.log(`  Saved ${resultsToSave.length} results to run ${run.id}`);
}

function printBacktestResults(results: PatternTestResult[], testSeason: number): void {
  console.log(`\n=== Backtest Results for Season ${testSeason} ===\n`);
  console.log(`Total patterns analyzed: ${results.length}\n`);
  
  const gradeCount: Record<string, number> = { A: 0, B: 0, C: 0, D: 0, F: 0 };
  for (const r of results) {
    gradeCount[r.grade]++;
  }
  
  console.log("Grade Distribution:");
  console.log(`  A: ${gradeCount.A}  B: ${gradeCount.B}  C: ${gradeCount.C}  D: ${gradeCount.D}  F: ${gradeCount.F}\n`);
  
  const profitable = results.filter(r => r.pnl.roi > 0);
  const significant = results.filter(r => r.stats.isSignificant);
  
  console.log(`Profitable patterns: ${profitable.length}/${results.length} (${(profitable.length/results.length*100).toFixed(0)}%)`);
  console.log(`Statistically significant: ${significant.length}/${results.length} (${(significant.length/results.length*100).toFixed(0)}%)\n`);
  
  console.log("Top 10 Patterns:");
  console.log("─".repeat(80));
  
  for (const r of results.slice(0, 10)) {
    console.log(`\n${formatGrade(r.grade)} ${r.conditions.join(" + ")}  →  ${r.outcome}`);
    console.log(`  Train: ${(r.trainHitRate * 100).toFixed(1)}% (n=${r.trainSampleSize})`);
    console.log(`  Test:  ${(r.testHitRate * 100).toFixed(1)}% (n=${r.testSampleSize})`);
    console.log(`  Z-score: ${r.stats.zScoreVsChance.toFixed(2)}, ROI: ${(r.pnl.roi * 100).toFixed(1)}%`);
  }
  
  console.log("\n");
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
