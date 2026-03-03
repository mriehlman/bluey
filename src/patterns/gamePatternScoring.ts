/** Standard -110 vig implied probability (100/210 ≈ 0.476, but 52.4% is common for pushes) */
const IMPLIED_PROB = 0.524;

export interface PatternScoreInput {
  hitRate: number;
  sampleSize: number;
  seasons: number;
  totalSeasons: number;
  maxSample: number;
  perSeason: Record<string, number>;
  /** Optional: last date this pattern hit (for recency bonus) */
  lastHitDate?: Date | null;
  /** Optional: per-outcome implied prob override (e.g. spreads/totals may differ) */
  impliedProb?: number;
}

export interface PatternScores {
  confidenceScore: number;
  valueScore: number;
}

export function scorePattern(input: PatternScoreInput): PatternScores {
  const {
    hitRate,
    sampleSize,
    seasons,
    totalSeasons,
    maxSample,
    perSeason,
    lastHitDate,
    impliedProb = IMPLIED_PROB,
  } = input;

  const seasonCounts = Object.values(perSeason);
  const maxSeasonShare = seasonCounts.length > 0
    ? Math.max(...seasonCounts) / sampleSize
    : 1;

  // Sample size (40%), season diversity (30%), even season distribution (30%)
  let confidenceScore =
    (sampleSize / maxSample) * 0.4 +
    (seasons / totalSeasons) * 0.3 +
    (1 - maxSeasonShare) * 0.3;

  // Recency bonus: patterns that hit recently get a small boost (up to +0.05)
  if (lastHitDate) {
    const daysSinceHit = (Date.now() - lastHitDate.getTime()) / 86_400_000;
    if (daysSinceHit < 90) {
      const recencyBonus = Math.max(0, 0.05 * (1 - daysSinceHit / 90));
      confidenceScore = Math.min(1, confidenceScore + recencyBonus);
    }
  }

  const edge = hitRate - impliedProb;
  const valueScore = edge * Math.sqrt(sampleSize);

  return { confidenceScore, valueScore };
}
