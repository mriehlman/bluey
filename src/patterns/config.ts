export interface PatternFilterConfig {
  minOccurrences: number;
  minSeasonsWithHits: number;
  maxClusterShare: number;
  minAvgHitsPerSeason: number;
  maxAvgHitsPerSeason: number;
}

export const DEFAULT_FILTER_CONFIG: PatternFilterConfig = {
  minOccurrences: 3,
  minSeasonsWithHits: 2,
  maxClusterShare: 0.6,
  minAvgHitsPerSeason: 0.5,
  maxAvgHitsPerSeason: 6,
};
