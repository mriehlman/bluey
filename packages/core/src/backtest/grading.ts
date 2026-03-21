/**
 * Pattern Grading Module
 * 
 * Assigns letter grades (A-F) to backtested patterns based on
 * multiple quality dimensions.
 */

import type {
  BacktestGrade,
  BacktestGradeDetails,
  SignificanceStats,
  PnLResult,
} from "./backtestTypes";

const IMPLIED_PROB = 0.524;

/**
 * Grade a pattern based on backtest results
 */
export function gradePattern(
  trainHitRate: number,
  testHitRate: number,
  stats: SignificanceStats,
  pnl: PnLResult
): BacktestGradeDetails {
  const edgeScore = calculateEdgeScore(testHitRate);
  const significanceScore = calculateSignificanceScore(stats);
  const consistencyScore = calculateConsistencyScore(trainHitRate, testHitRate, stats);
  const pnlScore = calculatePnLScore(pnl);

  const totalScore = edgeScore + significanceScore + consistencyScore + pnlScore;
  const grade = scoreToGrade(totalScore);

  const summary = generateSummary(grade, {
    edgeScore,
    significanceScore,
    consistencyScore,
    pnlScore,
  });

  return {
    grade,
    components: {
      edgeScore,
      significanceScore,
      consistencyScore,
      pnlScore,
    },
    totalScore,
    summary,
  };
}

/**
 * Score based on edge over implied probability (0-25 points)
 */
function calculateEdgeScore(testHitRate: number): number {
  const edge = testHitRate - IMPLIED_PROB;
  
  if (edge <= 0) return 0;
  if (edge >= 0.15) return 25;
  
  return Math.min(25, (edge / 0.15) * 25);
}

/**
 * Score based on statistical significance (0-25 points)
 */
function calculateSignificanceScore(stats: SignificanceStats): number {
  let score = 0;

  if (stats.zScoreVsChance >= 1.96) {
    score += 10;
  } else if (stats.zScoreVsChance >= 1.65) {
    score += 7;
  } else if (stats.zScoreVsChance >= 1.28) {
    score += 4;
  }

  if (stats.pValueVsChance < 0.01) {
    score += 8;
  } else if (stats.pValueVsChance < 0.05) {
    score += 5;
  } else if (stats.pValueVsChance < 0.10) {
    score += 2;
  }

  const ciWidth = stats.confidenceInterval[1] - stats.confidenceInterval[0];
  if (ciWidth < 0.10) {
    score += 7;
  } else if (ciWidth < 0.15) {
    score += 5;
  } else if (ciWidth < 0.20) {
    score += 3;
  }

  return Math.min(25, score);
}

/**
 * Score based on train/test consistency (0-25 points)
 */
function calculateConsistencyScore(
  trainHitRate: number,
  testHitRate: number,
  stats: SignificanceStats
): number {
  let score = 0;

  const degradation = (trainHitRate - testHitRate) / trainHitRate;
  
  if (testHitRate >= trainHitRate) {
    score += 15;
  } else if (degradation < 0.05) {
    score += 12;
  } else if (degradation < 0.10) {
    score += 9;
  } else if (degradation < 0.15) {
    score += 6;
  } else if (degradation < 0.25) {
    score += 3;
  }

  if (stats.isConsistentWithTrain) {
    score += 10;
  } else if (stats.pValueVsTrain > 0.10) {
    score += 5;
  }

  return Math.min(25, score);
}

/**
 * Score based on P&L performance (0-25 points)
 */
function calculatePnLScore(pnl: PnLResult): number {
  let score = 0;

  if (pnl.roi >= 0.10) {
    score += 10;
  } else if (pnl.roi >= 0.05) {
    score += 8;
  } else if (pnl.roi > 0) {
    score += 5;
  } else if (pnl.roi > -0.05) {
    score += 2;
  }

  if (pnl.maxDrawdown < 0.10) {
    score += 5;
  } else if (pnl.maxDrawdown < 0.20) {
    score += 3;
  } else if (pnl.maxDrawdown < 0.30) {
    score += 1;
  }

  if (pnl.sharpeRatio !== null) {
    if (pnl.sharpeRatio >= 1.5) {
      score += 5;
    } else if (pnl.sharpeRatio >= 1.0) {
      score += 4;
    } else if (pnl.sharpeRatio >= 0.5) {
      score += 2;
    }
  } else {
    if (pnl.totalBets >= 30 && pnl.roi > 0) {
      score += 2;
    }
  }

  if (pnl.expectedValue > 0.05) {
    score += 5;
  } else if (pnl.expectedValue > 0.02) {
    score += 3;
  } else if (pnl.expectedValue > 0) {
    score += 1;
  }

  return Math.min(25, score);
}

/**
 * Convert total score to letter grade
 */
function scoreToGrade(totalScore: number): BacktestGrade {
  if (totalScore >= 85) return "A";
  if (totalScore >= 70) return "B";
  if (totalScore >= 55) return "C";
  if (totalScore >= 40) return "D";
  return "F";
}

/**
 * Generate human-readable summary
 */
function generateSummary(
  grade: BacktestGrade,
  components: { edgeScore: number; significanceScore: number; consistencyScore: number; pnlScore: number }
): string {
  const strengths: string[] = [];
  const weaknesses: string[] = [];

  if (components.edgeScore >= 20) {
    strengths.push("strong edge over breakeven");
  } else if (components.edgeScore < 10) {
    weaknesses.push("limited edge");
  }

  if (components.significanceScore >= 20) {
    strengths.push("statistically significant");
  } else if (components.significanceScore < 10) {
    weaknesses.push("not statistically significant");
  }

  if (components.consistencyScore >= 20) {
    strengths.push("consistent train/test performance");
  } else if (components.consistencyScore < 10) {
    weaknesses.push("significant degradation in testing");
  }

  if (components.pnlScore >= 20) {
    strengths.push("excellent P&L metrics");
  } else if (components.pnlScore < 10) {
    weaknesses.push("poor P&L performance");
  }

  let summary = `Grade ${grade}: `;
  
  if (strengths.length > 0) {
    summary += `Strengths: ${strengths.join(", ")}. `;
  }
  
  if (weaknesses.length > 0) {
    summary += `Weaknesses: ${weaknesses.join(", ")}.`;
  }

  if (strengths.length === 0 && weaknesses.length === 0) {
    summary += "Average performance across all metrics.";
  }

  return summary;
}

/**
 * Get grade color for terminal output
 */
export function gradeColor(grade: BacktestGrade): string {
  switch (grade) {
    case "A": return "\x1b[32m";
    case "B": return "\x1b[36m";
    case "C": return "\x1b[33m";
    case "D": return "\x1b[33m";
    case "F": return "\x1b[31m";
    default: return "\x1b[0m";
  }
}

/**
 * Format grade with color for terminal
 */
export function formatGrade(grade: BacktestGrade): string {
  const color = gradeColor(grade);
  const reset = "\x1b[0m";
  return `${color}${grade}${reset}`;
}
