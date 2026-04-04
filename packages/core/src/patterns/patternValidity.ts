export type PatternValidityDefaults = {
  minSampleSize: number;
  minWinRate: number;
  requireOutOfSampleEvidence: boolean;
  requireLeakageCheck: boolean;
};

export type FeatureLineageMetadata = {
  available_before_game: boolean;
  source_timestamp_required: boolean;
  derived_from_postgame_data: boolean;
};

export const PATTERN_VALIDITY_DEFAULTS: PatternValidityDefaults = {
  minSampleSize: 50,
  // Baseline 52.4% + governance buffer (0.5pp).
  minWinRate: 0.529,
  requireOutOfSampleEvidence: true,
  requireLeakageCheck: true,
};

const DEFAULT_FEATURE_METADATA: Record<string, FeatureLineageMetadata> = {
  season: {
    available_before_game: true,
    source_timestamp_required: false,
    derived_from_postgame_data: false,
  },
  spread_home: {
    available_before_game: true,
    source_timestamp_required: true,
    derived_from_postgame_data: false,
  },
  total_over: {
    available_before_game: true,
    source_timestamp_required: true,
    derived_from_postgame_data: false,
  },
};

export type PatternValidityInput = {
  sampleSize: number;
  posteriorHitRate: number;
  hasOutOfSampleEvidence?: boolean;
  hasLeakageRisk?: boolean;
  rules?: Partial<PatternValidityDefaults>;
};

export type PatternValidityResult = {
  pass: boolean;
  reasons: string[];
};

export function evaluatePatternValidity(input: PatternValidityInput): PatternValidityResult {
  const rules = { ...PATTERN_VALIDITY_DEFAULTS, ...(input.rules ?? {}) };
  const reasons: string[] = [];

  if (!Number.isFinite(input.sampleSize) || input.sampleSize < rules.minSampleSize) {
    reasons.push("insufficient_sample_size");
  }

  if (!Number.isFinite(input.posteriorHitRate) || input.posteriorHitRate < rules.minWinRate) {
    reasons.push("below_min_win_rate");
  }

  if (rules.requireOutOfSampleEvidence && input.hasOutOfSampleEvidence !== true) {
    reasons.push("missing_out_of_sample_evidence");
  }

  if (rules.requireLeakageCheck && input.hasLeakageRisk !== false) {
    reasons.push("leakage_check_failed");
  }

  return {
    pass: reasons.length === 0,
    reasons,
  };
}

export function inferLeakageRiskFromConditions(conditions: readonly string[]): boolean {
  const leakageMarkers = [
    "outcome",
    "result",
    "final_score",
    "postgame",
    "closing_line_value",
  ];
  return conditions.some((token) => {
    const normalized = token.toLowerCase();
    return leakageMarkers.some((marker) => normalized.includes(marker));
  });
}

export function extractFeatureKeysFromConditions(conditions: readonly string[]): string[] {
  return [...new Set(conditions
    .map((condition) => condition.replace(/^!/, ""))
    .map((condition) => {
      const idx = condition.indexOf(":");
      return idx >= 0 ? condition.slice(0, idx) : condition;
    })
    .filter(Boolean))];
}

export function inferLeakageRiskFromFeatureMetadata(
  featureKeys: readonly string[],
  metadata: Record<string, FeatureLineageMetadata> = DEFAULT_FEATURE_METADATA,
): boolean {
  return featureKeys.some((featureKey) => {
    const info = metadata[featureKey];
    if (!info) return false;
    if (!info.available_before_game) return true;
    if (info.derived_from_postgame_data) return true;
    return false;
  });
}
