/**
 * P&L Simulation Module
 * 
 * Simulates betting performance for backtested patterns,
 * including flat betting, Kelly criterion, and half-Kelly strategies.
 */

import type { 
  BacktestConfig, 
  GameResult, 
  PnLResult, 
  BetResult 
} from "./backtestTypes";

const IMPLIED_PROB = 0.524;

/**
 * Convert American odds to decimal odds
 */
export function americanToDecimal(americanOdds: number): number {
  if (americanOdds > 0) {
    return 1 + americanOdds / 100;
  } else {
    return 1 + 100 / Math.abs(americanOdds);
  }
}

/**
 * Convert American odds to implied probability
 */
export function americanToImpliedProb(americanOdds: number): number {
  if (americanOdds > 0) {
    return 100 / (americanOdds + 100);
  } else {
    return Math.abs(americanOdds) / (Math.abs(americanOdds) + 100);
  }
}

/**
 * Calculate Kelly criterion bet fraction
 * f* = (bp - q) / b
 * where b = decimal odds - 1, p = win probability, q = 1 - p
 */
export function kellyFraction(
  winProbability: number,
  decimalOdds: number
): number {
  const b = decimalOdds - 1;
  const p = winProbability;
  const q = 1 - p;
  
  const kelly = (b * p - q) / b;
  
  return Math.max(0, kelly);
}

/**
 * Simulate P&L for a series of bets
 */
export function simulatePnL(
  gameResults: GameResult[],
  config: BacktestConfig,
  estimatedWinProb: number
): PnLResult {
  if (gameResults.length === 0) {
    return {
      totalBets: 0,
      wins: 0,
      losses: 0,
      netPnL: 0,
      roi: 0,
      maxDrawdown: 0,
      peakBankroll: config.startingBankroll,
      finalBankroll: config.startingBankroll,
      sharpeRatio: null,
      betHistory: [],
      expectedValue: 0,
    };
  }

  const sortedResults = [...gameResults].sort(
    (a, b) => a.date.getTime() - b.date.getTime()
  );

  let bankroll = config.startingBankroll;
  let peakBankroll = bankroll;
  let maxDrawdown = 0;
  let totalWagered = 0;
  let wins = 0;
  let losses = 0;
  
  const betHistory: BetResult[] = [];
  const returns: number[] = [];

  for (const result of sortedResults) {
    const odds = result.odds ?? config.standardOdds;
    const decimalOdds = americanToDecimal(odds);
    
    let wager: number;
    
    switch (config.betSizing) {
      case "kelly": {
        const kellyF = kellyFraction(estimatedWinProb, decimalOdds);
        wager = Math.min(
          bankroll * kellyF,
          bankroll * config.betFraction
        );
        break;
      }
      case "halfKelly": {
        const kellyF = kellyFraction(estimatedWinProb, decimalOdds);
        wager = Math.min(
          bankroll * kellyF * 0.5,
          bankroll * config.betFraction
        );
        break;
      }
      case "flat":
      default:
        wager = config.startingBankroll * config.betFraction;
        break;
    }

    wager = Math.min(wager, bankroll);
    if (wager <= 0) continue;

    totalWagered += wager;
    
    let payout: number;
    let profit: number;

    if (result.hit) {
      payout = wager * decimalOdds;
      profit = payout - wager;
      wins++;
    } else {
      payout = 0;
      profit = -wager;
      losses++;
    }

    bankroll += profit;
    
    if (bankroll > peakBankroll) {
      peakBankroll = bankroll;
    }
    
    const drawdown = (peakBankroll - bankroll) / peakBankroll;
    if (drawdown > maxDrawdown) {
      maxDrawdown = drawdown;
    }

    returns.push(profit / wager);

    betHistory.push({
      gameId: result.gameId,
      date: result.date,
      wager,
      payout,
      profit,
      bankrollAfter: bankroll,
      hit: result.hit,
    });
  }

  const netPnL = bankroll - config.startingBankroll;
  const roi = totalWagered > 0 ? netPnL / totalWagered : 0;

  let sharpeRatio: number | null = null;
  if (returns.length >= 30) {
    const avgReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / returns.length;
    const stdDev = Math.sqrt(variance);
    
    if (stdDev > 0) {
      sharpeRatio = avgReturn / stdDev;
    }
  }

  const decimalOdds = americanToDecimal(config.standardOdds);
  const winPayout = decimalOdds - 1;
  const expectedValue = estimatedWinProb * winPayout - (1 - estimatedWinProb);

  return {
    totalBets: betHistory.length,
    wins,
    losses,
    netPnL,
    roi,
    maxDrawdown,
    peakBankroll,
    finalBankroll: bankroll,
    sharpeRatio,
    betHistory,
    expectedValue,
  };
}

/**
 * Calculate aggregate P&L for multiple patterns
 */
export function aggregatePnL(
  patternPnLs: PnLResult[],
  startingBankroll: number
): PnLResult {
  if (patternPnLs.length === 0) {
    return {
      totalBets: 0,
      wins: 0,
      losses: 0,
      netPnL: 0,
      roi: 0,
      maxDrawdown: 0,
      peakBankroll: startingBankroll,
      finalBankroll: startingBankroll,
      sharpeRatio: null,
      betHistory: [],
      expectedValue: 0,
    };
  }

  const allBets: BetResult[] = patternPnLs
    .flatMap(p => p.betHistory)
    .sort((a, b) => a.date.getTime() - b.date.getTime());

  let bankroll = startingBankroll;
  let peakBankroll = bankroll;
  let maxDrawdown = 0;
  let totalWagered = 0;
  let wins = 0;
  let losses = 0;
  const returns: number[] = [];

  const replayedHistory: BetResult[] = [];

  for (const bet of allBets) {
    totalWagered += bet.wager;
    
    bankroll += bet.profit;
    
    if (bet.hit) {
      wins++;
    } else {
      losses++;
    }

    if (bankroll > peakBankroll) {
      peakBankroll = bankroll;
    }
    
    const drawdown = (peakBankroll - bankroll) / peakBankroll;
    if (drawdown > maxDrawdown) {
      maxDrawdown = drawdown;
    }

    returns.push(bet.profit / bet.wager);

    replayedHistory.push({
      ...bet,
      bankrollAfter: bankroll,
    });
  }

  const netPnL = bankroll - startingBankroll;
  const roi = totalWagered > 0 ? netPnL / totalWagered : 0;

  let sharpeRatio: number | null = null;
  if (returns.length >= 30) {
    const avgReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / returns.length;
    const stdDev = Math.sqrt(variance);
    
    if (stdDev > 0) {
      sharpeRatio = avgReturn / stdDev;
    }
  }

  const avgExpectedValue = patternPnLs.length > 0
    ? patternPnLs.reduce((sum, p) => sum + p.expectedValue, 0) / patternPnLs.length
    : 0;

  return {
    totalBets: allBets.length,
    wins,
    losses,
    netPnL,
    roi,
    maxDrawdown,
    peakBankroll,
    finalBankroll: bankroll,
    sharpeRatio,
    betHistory: replayedHistory,
    expectedValue: avgExpectedValue,
  };
}

/**
 * Calculate CLV (Closing Line Value) if we have historical odds
 * This measures if our patterns capture market-beating value
 */
export function calculateCLV(
  gameResults: GameResult[],
  defaultOdds: number
): { avgCLV: number; clvPositive: number; clvNegative: number } {
  let totalCLV = 0;
  let clvPositive = 0;
  let clvNegative = 0;

  for (const result of gameResults) {
    const impliedProb = americanToImpliedProb(result.odds ?? defaultOdds);
    const standardImplied = americanToImpliedProb(defaultOdds);
    
    const clv = standardImplied - impliedProb;
    totalCLV += clv;
    
    if (clv > 0) {
      clvPositive++;
    } else if (clv < 0) {
      clvNegative++;
    }
  }

  return {
    avgCLV: gameResults.length > 0 ? totalCLV / gameResults.length : 0,
    clvPositive,
    clvNegative,
  };
}

/**
 * Monte Carlo simulation for bankroll risk analysis
 * Simulates many possible outcomes to estimate ruin probability
 */
export function monteCarloRuin(
  winProbability: number,
  decimalOdds: number,
  betFraction: number,
  numBets: number,
  simulations: number = 10000
): { ruinProb: number; medianFinal: number; p5Final: number; p95Final: number } {
  const finals: number[] = [];
  let ruins = 0;

  for (let sim = 0; sim < simulations; sim++) {
    let bankroll = 1;
    
    for (let bet = 0; bet < numBets; bet++) {
      const wager = bankroll * betFraction;
      
      if (Math.random() < winProbability) {
        bankroll += wager * (decimalOdds - 1);
      } else {
        bankroll -= wager;
      }
      
      if (bankroll <= 0.01) {
        ruins++;
        break;
      }
    }
    
    finals.push(bankroll);
  }

  finals.sort((a, b) => a - b);
  
  return {
    ruinProb: ruins / simulations,
    medianFinal: finals[Math.floor(simulations / 2)],
    p5Final: finals[Math.floor(simulations * 0.05)],
    p95Final: finals[Math.floor(simulations * 0.95)],
  };
}
