type FamilyGate = {
  minPosterior: number;
  minMeta: number;
  minEv: number;
};

type StrictGateByFamilyConfig = {
  minCalibratedEdge: number;
  maxUncertaintyScore: number;
  /**
   * Strict mode only: block extreme moneyline longshots when implied win probability is too low.
   * Example: +750 ~= 0.118 implied probability, so min 0.15 would block it.
   */
  minMoneylineImpliedProbability?: number;
  /**
   * Optional strict-mode escape hatch for longshots:
   * if adjusted edge meets this threshold, allow despite low implied probability.
   */
  minMoneylineLongshotEdgeOverride?: number;
};

export const DISCOVERY_DEFAULTS = {
  objective: "hit_rate",
  trainSeason: 2023,
  valSeason: 2024,
  forwardFrom: 2025,
  minLosoFoldsPass: 2,
  maxDepth: 3,
  minLeaf: 50,
  minTokenSupport: 40,
  minOutcomeSamples: 60,
  maxItemset: 3,
  minSupport: 50,
  maxPatterns: 1000,
  minConditionCount: 2,
  minDistinctFeatureCount: 2,
  maxPerOutcome: 80,
  maxPerFamily: 180,
  maxPerFamilyByBucket: {
    PLAYER: 220,
    TOTAL: 170,
    SPREAD: 170,
    MONEYLINE: 170,
    OTHER: 120,
  },
  maxTrainCoverage: 0.35,
  minOutcomeTrainSeasons: 2,
  minOutcomeTrainCoverage: 0.005,
  maxOutcomeTrainCoverage: 0.8,
  minValSamples: 15,
  minForwardSamples: 20,
  minValPosteriorDiscovery: 0.52,
  minForwardPosteriorDiscovery: 0.52,
  minValEdge: 0,
  minForwardEdgeDiscovery: 0,
  maxConditionOverlap: 0.8,
  priorStrength: 25,
  recencyHalfLifeDays: 90,
  lowNPenaltyK: 120,
  forwardStabilityWeight: 12,
  seasonWeightEnabled: true,
  seasonWeightMin: 0.4,
  maxSingletons: 80,
} as const;

export const VALIDATION_DEFAULTS = {
  objective: "hit_rate",
  minTrainEdge: 0.015,
  minValEdge: 0.008,
  minForwardEdge: 0.005,
  minTrainPosterior: 0.52,
  minValPosterior: 0.52,
  minForwardPosterior: 0.515,
  minTrainSamples: 80,
  minValSamples: 25,
  minForwardSamples: 30,
  uncertaintyPenaltyK: 120,
  uncertaintyPenaltyScale: 0.35,
  fdrAlpha: 0.1,
  fdrMethod: "bh",
  fdrPermutations: 3000,
  fdrSeed: 1337,
  runDecay: true,
  decayMinWindowSamples: 15,
  decayCollapseEdge: -0.005,
  progressEvery: 50,
} as const;

const defaultFamilyGates: Record<"PLAYER" | "TOTAL" | "SPREAD" | "MONEYLINE" | "OTHER", FamilyGate> = {
  PLAYER: { minPosterior: 0.55, minMeta: 0.53, minEv: -0.01 },
  TOTAL: { minPosterior: 0.53, minMeta: 0.51, minEv: -0.01 },
  SPREAD: { minPosterior: 0.53, minMeta: 0.51, minEv: -0.01 },
  MONEYLINE: { minPosterior: 0.56, minMeta: 0.53, minEv: 0.00 },
  OTHER: { minPosterior: 0.54, minMeta: 0.52, minEv: -0.01 },
};

export const PREDICTION_TUNING = {
  maxNegativeAmericanOdds: -200,
  familyGates: defaultFamilyGates,
  /** Exclude these bet families from suggested picks. Empty = include all; filter in UI. */
  excludeFamilies: [] as const,
} as const;

export const PICK_QUALITY_TUNING = {
  // Keep legacy behavior unless explicitly enabled.
  enableStrictActionabilityGates: false,
  enableDynamicVoteWeighting: false,
  // Scales separation around neutral vote weights (1.0 = baseline behavior).
  voteWeightingStrength: 1.0,
  calibrationMinSample: 40,
  sourceReliabilityMinSample: 40,
  uncertaintyPenaltyScale: 0.18,
  strictGateByFamily: {
    PLAYER: { minCalibratedEdge: 0.015, maxUncertaintyScore: 0.72 },
    TOTAL: { minCalibratedEdge: 0.01, maxUncertaintyScore: 0.74 },
    SPREAD: { minCalibratedEdge: 0.01, maxUncertaintyScore: 0.74 },
    MONEYLINE: {
      minCalibratedEdge: 0.012,
      // Tighter uncertainty cap to avoid shaky coin-flip style ML picks.
      maxUncertaintyScore: 0.52,
      // Allow borderline plus-money dogs (e.g. +570 ~ 0.149) while still blocking extreme longshots.
      minMoneylineImpliedProbability: 0.145,
      // Leave unset to hard-block extreme longshots in strict mode.
      minMoneylineLongshotEdgeOverride: undefined,
    },
    OTHER: { minCalibratedEdge: 0.01, maxUncertaintyScore: 0.75 },
  } satisfies Record<"PLAYER" | "TOTAL" | "SPREAD" | "MONEYLINE" | "OTHER", StrictGateByFamilyConfig>,
} as const;

export const LEDGER_TUNING = {
  stake: 10,
  bankrollStart: 1000,
  maxBetPicksPerGame: 6,
  allowFallbackOddsForLedger: false,
  fallbackAmericanOdds: -110,
} as const;
