/**
 * Backtest Types and Interfaces
 * 
 * Defines the core data structures for backtesting game patterns.
 */

export interface BacktestConfig {
  /** Seasons to use for training (pattern discovery) */
  trainSeasons: number[];
  /** Seasons to use for testing (out-of-sample validation) */
  testSeasons: number[];
  /** Minimum sample size required in training data */
  minSample: number;
  /** Minimum hit rate required to consider a pattern */
  minHitRate: number;
  /** Maximum number of condition legs */
  maxLegs: number;
  /** Minimum number of training seasons with hits */
  minTrainSeasons: number;
  /** Bankroll for P&L simulation */
  startingBankroll: number;
  /** Bet sizing strategy */
  betSizing: "flat" | "kelly" | "halfKelly";
  /** Flat bet size (as fraction of bankroll if flat, or max fraction for Kelly) */
  betFraction: number;
  /** Standard odds for calculating P&L (American format, e.g., -110) */
  standardOdds: number;
}

export const DEFAULT_BACKTEST_CONFIG: BacktestConfig = {
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
};

export interface PatternCandidate {
  patternKey: string;
  conditions: string[];
  outcome: string;
  trainSampleSize: number;
  trainHitCount: number;
  trainHitRate: number;
  trainSeasons: number;
  trainPerSeason: Record<string, number>;
  confidenceScore: number;
  valueScore: number;
  /** Game IDs used for training */
  trainGameIds: string[];
}

export interface PatternTestResult {
  patternKey: string;
  conditions: string[];
  outcome: string;
  /** Training stats for reference */
  trainHitRate: number;
  trainSampleSize: number;
  /** Out-of-sample test results */
  testSampleSize: number;
  testHitCount: number;
  testHitRate: number;
  testSeasons: number;
  testPerSeason: Record<string, number>;
  /** Individual game results in test period */
  testGameResults: GameResult[];
  /** Statistical metrics */
  stats: SignificanceStats;
  /** P&L simulation results */
  pnl: PnLResult;
  /** Overall backtest grade */
  grade: BacktestGrade;
}

export interface GameResult {
  gameId: string;
  season: number;
  date: Date;
  hit: boolean;
  /** Actual odds if available */
  odds?: number;
}

export interface SignificanceStats {
  /** Z-score testing if test hit rate significantly differs from chance (52.4%) */
  zScoreVsChance: number;
  /** P-value for z-score vs chance */
  pValueVsChance: number;
  /** Z-score testing if test hit rate matches train hit rate */
  zScoreVsTrain: number;
  /** P-value for consistency with training */
  pValueVsTrain: number;
  /** Chi-square statistic for overall pattern validity */
  chiSquare: number;
  /** P-value for chi-square test */
  chiSquarePValue: number;
  /** Is statistically significant at alpha=0.05? */
  isSignificant: boolean;
  /** Does test rate match training (not significantly worse)? */
  isConsistentWithTrain: boolean;
  /** Confidence interval for test hit rate (95%) */
  confidenceInterval: [number, number];
}

export interface PnLResult {
  /** Total number of bets placed */
  totalBets: number;
  /** Number of winning bets */
  wins: number;
  /** Number of losing bets */
  losses: number;
  /** Total profit/loss in units */
  netPnL: number;
  /** Return on investment (P&L / total risked) */
  roi: number;
  /** Maximum drawdown experienced */
  maxDrawdown: number;
  /** Peak bankroll achieved */
  peakBankroll: number;
  /** Final bankroll */
  finalBankroll: number;
  /** Sharpe ratio (if enough bets) */
  sharpeRatio: number | null;
  /** Bet-by-bet P&L history */
  betHistory: BetResult[];
  /** Expected value per bet (edge * avg payout) */
  expectedValue: number;
}

export interface BetResult {
  gameId: string;
  date: Date;
  wager: number;
  payout: number;
  profit: number;
  bankrollAfter: number;
  hit: boolean;
}

export type BacktestGrade = "A" | "B" | "C" | "D" | "F";

export interface BacktestGradeDetails {
  grade: BacktestGrade;
  /** Breakdown of scoring components */
  components: {
    /** Test hit rate vs implied prob edge (0-25 points) */
    edgeScore: number;
    /** Statistical significance (0-25 points) */
    significanceScore: number;
    /** Consistency train vs test (0-25 points) */
    consistencyScore: number;
    /** P&L performance (0-25 points) */
    pnlScore: number;
  };
  /** Total score out of 100 */
  totalScore: number;
  /** Human-readable summary */
  summary: string;
}

export interface WalkForwardFold {
  /** Fold number (1-indexed) */
  foldNumber: number;
  /** Training season(s) for this fold */
  trainSeasons: number[];
  /** Test season for this fold */
  testSeason: number;
  /** Patterns discovered in training */
  patternsDiscovered: number;
  /** Patterns that passed test validation */
  patternsPassed: number;
  /** Results for each pattern tested */
  results: PatternTestResult[];
  /** Aggregate P&L for this fold */
  foldPnL: number;
  /** Aggregate ROI for this fold */
  foldROI: number;
}

export interface WalkForwardResult {
  /** Configuration used */
  config: BacktestConfig;
  /** Individual fold results */
  folds: WalkForwardFold[];
  /** Patterns that passed all folds */
  robustPatterns: PatternTestResult[];
  /** Aggregate statistics across all folds */
  aggregate: {
    totalBets: number;
    totalWins: number;
    totalLosses: number;
    netPnL: number;
    overallROI: number;
    avgHitRate: number;
    avgEdge: number;
    /** Patterns that were profitable in all folds */
    consistentlyProfitable: number;
    /** Patterns that degraded significantly in test */
    degradedPatterns: number;
  };
  /** Runtime in milliseconds */
  runtimeMs: number;
  /** Timestamp */
  completedAt: Date;
}

export interface BacktestSummary {
  /** Total patterns evaluated */
  totalEvaluated: number;
  /** Patterns passing all criteria */
  totalPassing: number;
  /** Grade distribution */
  gradeDistribution: Record<BacktestGrade, number>;
  /** Top patterns by grade */
  topPatterns: PatternTestResult[];
  /** Overall P&L if betting all passing patterns */
  aggregatePnL: PnLResult;
  /** Timestamp */
  completedAt: Date;
}
