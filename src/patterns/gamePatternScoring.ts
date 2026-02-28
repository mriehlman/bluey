const IMPLIED_PROB = 0.524; // standard -110 vig

export interface PatternScoreInput {
  hitRate: number;
  sampleSize: number;
  seasons: number;
  totalSeasons: number;
  maxSample: number;
  perSeason: Record<string, number>;
}

export interface PatternScores {
  confidenceScore: number;
  valueScore: number;
}

export function scorePattern(input: PatternScoreInput): PatternScores {
  const { hitRate, sampleSize, seasons, totalSeasons, maxSample, perSeason } = input;

  const seasonCounts = Object.values(perSeason);
  const maxSeasonShare = seasonCounts.length > 0
    ? Math.max(...seasonCounts) / sampleSize
    : 1;

  const confidenceScore =
    (sampleSize / maxSample) * 0.4 +
    (seasons / totalSeasons) * 0.3 +
    (1 - maxSeasonShare) * 0.3;

  const edge = hitRate - IMPLIED_PROB;
  const valueScore = edge * Math.sqrt(sampleSize);

  return { confidenceScore, valueScore };
}
