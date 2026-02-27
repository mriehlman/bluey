export interface PatternMetrics {
  occurrences: number;
  seasons: number;
  perSeason: Record<number, number>;
  longestGapDays: number | null;
  legs: number;
  lastHitDate?: Date | null;
}

export interface PatternScores {
  stabilityScore: number;
  rarityScore: number;
  balanceScore: number;
  recencyScore: number;
  overallScore: number;
}

export interface ScoringWeights {
  stability: number;
  balance: number;
  rarity: number;
  recency: number;
}

export const DEFAULT_WEIGHTS: ScoringWeights = {
  stability: 0.35,
  balance: 0.25,
  rarity: 0.25,
  recency: 0.15,
};

/**
 * Original stability score kept for backward compatibility with search:patterns.
 * New ranking uses computePatternScores() instead.
 */
export function stabilityScore(m: PatternMetrics): number {
  const seasonScore = m.seasons * 10;

  const counts = Object.values(m.perSeason);
  const mean = counts.reduce((a, b) => a + b, 0) / counts.length;
  const variance = counts.reduce((sum, c) => sum + (c - mean) ** 2, 0) / counts.length;
  const stddev = Math.sqrt(variance);
  const balanceScore = 10 / (1 + stddev);

  const maxSeasonHits = Math.max(...counts, 0);
  const clusterPenalty = m.occurrences > 0 ? -(maxSeasonHits / m.occurrences) * 15 : 0;

  const gapBonus = m.longestGapDays != null ? Math.min(m.longestGapDays / 30, 5) : 0;

  const legsPenalty = m.legs * -1;

  return seasonScore + balanceScore + clusterPenalty + gapBonus + legsPenalty;
}

/**
 * seasonsWithHits / totalSeasonsConsidered
 * Ranges 0..1 — higher means the pattern is more durable.
 */
function computeStability(m: PatternMetrics, totalSeasons: number): number {
  if (totalSeasons <= 0) return 0;
  return Math.min(m.seasons / totalSeasons, 1);
}

/**
 * 1 - clusterShare, where clusterShare = max(perSeason) / total.
 * Ranges 0..1 — higher means hits are spread evenly across seasons.
 */
function computeBalance(m: PatternMetrics): number {
  if (m.occurrences <= 0) return 0;
  const counts = Object.values(m.perSeason);
  const maxSeasonHits = Math.max(...counts, 0);
  const clusterShare = maxSeasonHits / m.occurrences;
  return 1 - clusterShare;
}

/**
 * Reward patterns whose avgHits/season is near a "sweet spot" target (2-3).
 * 1 - min(1, abs(avgHits - target) / targetRange)
 * Ranges 0..1 — higher means the hit rate is in the desirable range.
 */
function computeRarity(m: PatternMetrics, target = 2.5, targetRange = 3): number {
  if (m.seasons <= 0) return 0;
  const avgHits = m.occurrences / m.seasons;
  return 1 - Math.min(1, Math.abs(avgHits - target) / targetRange);
}

/**
 * Reward patterns that still hit recently.
 * Uses daysSinceLastHit relative to a reference date (usually "now").
 * Ranges 0..1 — decays linearly over ~2 years, floor at 0.
 */
function computeRecency(m: PatternMetrics, referenceDate: Date): number {
  if (!m.lastHitDate) return 0;
  const daysSince = Math.max(
    0,
    Math.round((referenceDate.getTime() - m.lastHitDate.getTime()) / (1000 * 60 * 60 * 24))
  );
  const decayWindow = 730; // ~2 years
  return Math.max(0, 1 - daysSince / decayWindow);
}

export function computePatternScores(
  m: PatternMetrics,
  totalSeasons: number,
  referenceDate: Date = new Date(),
  weights: ScoringWeights = DEFAULT_WEIGHTS,
): PatternScores {
  const stability = computeStability(m, totalSeasons);
  const balance = computeBalance(m);
  const rarity = computeRarity(m);
  const recency = computeRecency(m, referenceDate);

  const overall =
    weights.stability * stability +
    weights.balance * balance +
    weights.rarity * rarity +
    weights.recency * recency;

  return {
    stabilityScore: parseFloat(stability.toFixed(4)),
    rarityScore: parseFloat(rarity.toFixed(4)),
    balanceScore: parseFloat(balance.toFixed(4)),
    recencyScore: parseFloat(recency.toFixed(4)),
    overallScore: parseFloat(overall.toFixed(4)),
  };
}
