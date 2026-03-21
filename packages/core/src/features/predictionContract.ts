export type PredictionMarket =
  | "spread"
  | "total"
  | "moneyline"
  | "player_prop"
  | "other";

export const PREDICTION_CONTRACT_VERSION = "1.1.0";
export const FEATURE_SCHEMA_VERSION = "v1";
export const DEFAULT_MODEL_BUNDLE_VERSION = "unversioned";
export const RANKING_POLICY_VERSION = "rank_v1";
export const AGGREGATION_POLICY_VERSION = "agg_v1";
export const REQUIRED_MODEL_VOTE_IDS = [
  "pattern_match_model",
  "aggregation_model",
  "meta_model",
  "confidence_model",
] as const;

export type ModelVoteDecision = "yes" | "no" | "abstain";

export type ModelVote = {
  model_id: string;
  decision: ModelVoteDecision;
  // Normalized confidence in [0, 1]; null when abstaining.
  confidence: number | null;
};

export type FeatureSnapshotPayload = {
  game_id: string;
  season: number;
  feature_schema_version: string;
  tokens: string[];
  generated_from: "pregame_token_set";
  odds_timestamp_used: string | null;
  stats_snapshot_cutoff: string | null;
  injury_lineup_cutoff: string | null;
};

export type PredictionRecord = {
  prediction_id: string;
  game_id: string;
  market: PredictionMarket;
  selection: string;
  model_votes: ModelVote[];
  confidence_score: number;
  edge_estimate: number;
  supporting_patterns: string[];
  prediction_contract_version: string;
  ranking_policy_version: string;
  aggregation_policy_version: string;
  model_bundle_version: string;
  feature_schema_version: string;
  feature_snapshot_id: string;
  feature_snapshot_payload: FeatureSnapshotPayload;
  generated_at: string;
};

function hashString(input: string): string {
  // FNV-1a 32-bit for deterministic, lightweight IDs.
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export function createFeatureSnapshotId(input: {
  gameId: string;
  season: number;
  tokens: Iterable<string>;
}): string {
  const sorted = [...input.tokens].sort();
  const payload = `${input.gameId}|${input.season}|${sorted.join("|")}`;
  return `fs_${hashString(payload)}`;
}

export function createFeatureSnapshotPayload(input: {
  gameId: string;
  season: number;
  tokens: Iterable<string>;
  oddsTimestampUsed?: string | null;
  statsSnapshotCutoff?: string | null;
  injuryLineupCutoff?: string | null;
}): FeatureSnapshotPayload {
  return {
    game_id: input.gameId,
    season: input.season,
    feature_schema_version: FEATURE_SCHEMA_VERSION,
    tokens: [...input.tokens].sort(),
    generated_from: "pregame_token_set",
    odds_timestamp_used: input.oddsTimestampUsed ?? null,
    stats_snapshot_cutoff: input.statsSnapshotCutoff ?? null,
    injury_lineup_cutoff: input.injuryLineupCutoff ?? null,
  };
}

export function createPredictionId(input: {
  gameId: string;
  market: PredictionMarket;
  selection: string;
  predictionContractVersion: string;
  modelBundleVersion: string;
  rankingPolicyVersion: string;
  aggregationPolicyVersion: string;
  featureSnapshotId: string;
}): string {
  const payload = [
    input.gameId,
    input.market,
    input.selection,
    input.predictionContractVersion,
    input.modelBundleVersion,
    input.rankingPolicyVersion,
    input.aggregationPolicyVersion,
    input.featureSnapshotId,
  ].join("|");
  return `pred_${hashString(payload)}`;
}

export function validatePredictionRecord(record: PredictionRecord): string[] {
  const errors: string[] = [];

  if (!record.prediction_id) errors.push("prediction_id is required");
  if (!record.game_id) errors.push("game_id is required");
  if (!record.selection) errors.push("selection is required");
  if (!record.feature_snapshot_id) errors.push("feature_snapshot_id is required");
  if (!record.generated_at) errors.push("generated_at is required");
  if (!record.prediction_contract_version) errors.push("prediction_contract_version is required");
  if (!record.ranking_policy_version) errors.push("ranking_policy_version is required");
  if (!record.aggregation_policy_version) errors.push("aggregation_policy_version is required");
  if (!record.model_bundle_version) errors.push("model_bundle_version is required");
  if (!record.feature_schema_version) errors.push("feature_schema_version is required");
  if (!Array.isArray(record.model_votes) || record.model_votes.length === 0) {
    errors.push("model_votes must contain at least one vote");
  }
  if (!Array.isArray(record.supporting_patterns)) {
    errors.push("supporting_patterns must be an array");
  }
  if (!(record.confidence_score >= 0 && record.confidence_score <= 1)) {
    errors.push("confidence_score must be between 0 and 1");
  }
  if (
    !record.feature_snapshot_payload ||
    !Array.isArray(record.feature_snapshot_payload.tokens)
  ) {
    errors.push("feature_snapshot_payload.tokens must be an array");
  }
  if (record.feature_snapshot_payload?.game_id !== record.game_id) {
    errors.push("feature_snapshot_payload.game_id must match game_id");
  }
  for (const fieldName of [
    "odds_timestamp_used",
    "stats_snapshot_cutoff",
    "injury_lineup_cutoff",
  ] as const) {
    const value = record.feature_snapshot_payload?.[fieldName];
    if (value != null && Number.isNaN(Date.parse(value))) {
      errors.push(`feature_snapshot_payload.${fieldName} must be ISO timestamp or null`);
    }
  }
  if (
    record.feature_snapshot_payload?.feature_schema_version !==
    record.feature_schema_version
  ) {
    errors.push("feature_schema_version must match feature_snapshot_payload");
  }

  const seenModelIds = new Set<string>();
  for (const [idx, vote] of record.model_votes.entries()) {
    if (!vote.model_id) errors.push(`model_votes[${idx}].model_id is required`);
    seenModelIds.add(vote.model_id);
    if (!["yes", "no", "abstain"].includes(vote.decision)) {
      errors.push(`model_votes[${idx}].decision must be yes/no/abstain`);
    }
    if (vote.decision === "abstain" && vote.confidence != null) {
      errors.push(`model_votes[${idx}].confidence must be null for abstain`);
    }
    if (
      vote.decision !== "abstain" &&
      (vote.confidence == null || !(vote.confidence >= 0 && vote.confidence <= 1))
    ) {
      errors.push(`model_votes[${idx}].confidence must be between 0 and 1`);
    }
  }
  for (const requiredId of REQUIRED_MODEL_VOTE_IDS) {
    if (!seenModelIds.has(requiredId)) {
      errors.push(`model_votes must include ${requiredId}`);
    }
  }
  if (Number.isNaN(Date.parse(record.generated_at))) {
    errors.push("generated_at must be an ISO timestamp");
  }

  return errors;
}

export function assertPredictionRecord(record: PredictionRecord): void {
  const errors = validatePredictionRecord(record);
  if (errors.length > 0) {
    throw new Error(`Invalid PredictionRecord: ${errors.join("; ")}`);
  }
}
