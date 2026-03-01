/**
 * Statistical Significance Testing Module
 * 
 * Provides z-scores, chi-square tests, and confidence intervals
 * for validating pattern reliability.
 */

import type { SignificanceStats } from "./backtestTypes.js";

const IMPLIED_PROB = 0.524;

/**
 * Calculate z-score for a proportion test
 * H0: p = p0, H1: p != p0
 */
export function proportionZScore(
  observed: number,
  sampleSize: number,
  nullProp: number
): number {
  if (sampleSize === 0) return 0;
  const p = observed / sampleSize;
  const se = Math.sqrt((nullProp * (1 - nullProp)) / sampleSize);
  if (se === 0) return 0;
  return (p - nullProp) / se;
}

/**
 * Calculate p-value from z-score (two-tailed)
 */
export function zScoreToPValue(z: number): number {
  const absZ = Math.abs(z);
  const t = 1 / (1 + 0.2316419 * absZ);
  const d = 0.3989423 * Math.exp(-absZ * absZ / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return 2 * p;
}

/**
 * Calculate z-score comparing two proportions
 * Tests if p1 is significantly different from p2
 */
export function twoProportionZScore(
  hits1: number,
  n1: number,
  hits2: number,
  n2: number
): number {
  if (n1 === 0 || n2 === 0) return 0;
  
  const p1 = hits1 / n1;
  const p2 = hits2 / n2;
  
  const pooledP = (hits1 + hits2) / (n1 + n2);
  const se = Math.sqrt(pooledP * (1 - pooledP) * (1 / n1 + 1 / n2));
  
  if (se === 0) return 0;
  return (p1 - p2) / se;
}

/**
 * Chi-square test for pattern validity
 * Tests if observed outcomes significantly differ from expected under null hypothesis
 */
export function chiSquareTest(
  observed: { hits: number; misses: number },
  expected: { hits: number; misses: number }
): { chiSquare: number; pValue: number } {
  const n = observed.hits + observed.misses;
  if (n === 0) return { chiSquare: 0, pValue: 1 };
  
  const expectedHits = expected.hits;
  const expectedMisses = expected.misses;
  
  if (expectedHits === 0 || expectedMisses === 0) {
    return { chiSquare: 0, pValue: 1 };
  }
  
  const chiSquare = 
    Math.pow(observed.hits - expectedHits, 2) / expectedHits +
    Math.pow(observed.misses - expectedMisses, 2) / expectedMisses;
  
  const pValue = chiSquarePValue(chiSquare, 1);
  
  return { chiSquare, pValue };
}

/**
 * Chi-square cumulative distribution function (1 degree of freedom)
 * Returns p-value for given chi-square statistic
 */
function chiSquarePValue(x: number, df: number = 1): number {
  if (x <= 0) return 1;
  
  const k = df / 2;
  const xHalf = x / 2;
  
  let sum = Math.exp(-xHalf);
  let term = sum;
  
  for (let i = 1; i < 200; i++) {
    term *= xHalf / (k + i);
    sum += term;
    if (term < 1e-12) break;
  }
  
  const gamma = gammaLn(k);
  const regularizedGamma = sum * Math.exp(k * Math.log(xHalf) - xHalf - gamma);
  
  return 1 - Math.min(1, Math.max(0, regularizedGamma));
}

/**
 * Log-gamma function approximation (Stirling's formula with corrections)
 */
function gammaLn(x: number): number {
  const coef = [
    76.18009172947146,
    -86.50532032941677,
    24.01409824083091,
    -1.231739572450155,
    0.1208650973866179e-2,
    -0.5395239384953e-5
  ];
  
  let y = x;
  let tmp = x + 5.5;
  tmp -= (x + 0.5) * Math.log(tmp);
  let ser = 1.000000000190015;
  
  for (let j = 0; j < 6; j++) {
    ser += coef[j] / ++y;
  }
  
  return -tmp + Math.log(2.5066282746310005 * ser / x);
}

/**
 * Wilson score interval for binomial proportion (95% confidence)
 */
export function wilsonConfidenceInterval(
  hits: number,
  n: number,
  alpha: number = 0.05
): [number, number] {
  if (n === 0) return [0, 1];
  
  const z = 1.96;
  const p = hits / n;
  
  const denominator = 1 + z * z / n;
  const center = p + z * z / (2 * n);
  const margin = z * Math.sqrt((p * (1 - p) + z * z / (4 * n)) / n);
  
  const lower = Math.max(0, (center - margin) / denominator);
  const upper = Math.min(1, (center + margin) / denominator);
  
  return [lower, upper];
}

/**
 * Calculate comprehensive significance statistics for a pattern
 */
export function calculateSignificance(
  trainHits: number,
  trainN: number,
  testHits: number,
  testN: number
): SignificanceStats {
  const testHitRate = testN > 0 ? testHits / testN : 0;
  const trainHitRate = trainN > 0 ? trainHits / trainN : 0;
  
  const zScoreVsChance = proportionZScore(testHits, testN, IMPLIED_PROB);
  const pValueVsChance = zScoreToPValue(zScoreVsChance);
  
  const zScoreVsTrain = twoProportionZScore(testHits, testN, trainHits, trainN);
  const pValueVsTrain = zScoreToPValue(zScoreVsTrain);
  
  const expectedHits = testN * IMPLIED_PROB;
  const expectedMisses = testN * (1 - IMPLIED_PROB);
  const { chiSquare, pValue: chiSquarePValue } = chiSquareTest(
    { hits: testHits, misses: testN - testHits },
    { hits: expectedHits, misses: expectedMisses }
  );
  
  const confidenceInterval = wilsonConfidenceInterval(testHits, testN);
  
  const isSignificant = pValueVsChance < 0.05 && testHitRate > IMPLIED_PROB;
  const isConsistentWithTrain = pValueVsTrain > 0.05 || testHitRate >= trainHitRate;
  
  return {
    zScoreVsChance,
    pValueVsChance,
    zScoreVsTrain,
    pValueVsTrain,
    chiSquare,
    chiSquarePValue,
    isSignificant,
    isConsistentWithTrain,
    confidenceInterval,
  };
}

/**
 * Calculate required sample size for desired power
 * @param effectSize Expected edge over chance (e.g., 0.06 for 58% vs 52.4%)
 * @param power Statistical power desired (default 0.8)
 * @param alpha Significance level (default 0.05)
 */
export function requiredSampleSize(
  effectSize: number,
  power: number = 0.8,
  alpha: number = 0.05
): number {
  const zAlpha = 1.96;
  const zBeta = 0.84;
  
  const p = IMPLIED_PROB + effectSize;
  const q = 1 - p;
  
  const n = Math.ceil(
    Math.pow(zAlpha * Math.sqrt(IMPLIED_PROB * (1 - IMPLIED_PROB)) + zBeta * Math.sqrt(p * q), 2) /
    Math.pow(effectSize, 2)
  );
  
  return n;
}

/**
 * Assess pattern stability across seasons
 * Returns coefficient of variation for hit rates across seasons
 */
export function seasonalStability(perSeason: Record<string, number>, totalHits: number): number {
  const counts = Object.values(perSeason);
  if (counts.length < 2) return 0;
  
  const mean = totalHits / counts.length;
  if (mean === 0) return 0;
  
  const variance = counts.reduce((sum, c) => sum + Math.pow(c - mean, 2), 0) / counts.length;
  const stdDev = Math.sqrt(variance);
  
  return stdDev / mean;
}
