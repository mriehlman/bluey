import { describe, expect, test } from "bun:test";
import {
  AGGREGATION_POLICY_VERSION,
  DEFAULT_MODEL_BUNDLE_VERSION,
  FEATURE_SCHEMA_VERSION,
  PREDICTION_CONTRACT_VERSION,
  RANKING_POLICY_VERSION,
  createFeatureSnapshotId,
  validatePredictionRecord,
  type PredictionRecord,
} from "./predictionContract";
import { evaluatePatternValidity } from "../patterns/patternValidity";
import { generateGamePredictions, type PredictionInput } from "./predictionEngine";

function buildValidRecord(): PredictionRecord {
  return {
    prediction_id: "pred_test_1",
    game_id: "game-1",
    market: "moneyline",
    selection: "HOME_WIN:game",
    model_votes: [
      { model_id: "pattern_match_model", decision: "yes", confidence: 0.61 },
      { model_id: "aggregation_model", decision: "yes", confidence: 0.61 },
      { model_id: "meta_model", decision: "abstain", confidence: null },
      { model_id: "confidence_model", decision: "yes", confidence: 0.61 },
      { model_id: "player_points_ml_model", decision: "abstain", confidence: null },
    ],
    confidence_score: 0.61,
    edge_estimate: 0.04,
    supporting_patterns: ["pat-1", "pat-2"],
    prediction_contract_version: PREDICTION_CONTRACT_VERSION,
    ranking_policy_version: RANKING_POLICY_VERSION,
    aggregation_policy_version: AGGREGATION_POLICY_VERSION,
    model_bundle_version: DEFAULT_MODEL_BUNDLE_VERSION,
    feature_schema_version: FEATURE_SCHEMA_VERSION,
    feature_snapshot_id: "fs_abc123",
    feature_snapshot_payload: {
      game_id: "game-1",
      season: 2026,
      feature_schema_version: FEATURE_SCHEMA_VERSION,
      tokens: ["season:2026"],
      generated_from: "pregame_token_set",
      odds_timestamp_used: new Date("2026-03-21T00:00:00.000Z").toISOString(),
      stats_snapshot_cutoff: new Date("2026-03-21T00:00:00.000Z").toISOString(),
      injury_lineup_cutoff: new Date("2026-03-21T00:00:00.000Z").toISOString(),
    },
    generated_at: new Date("2026-03-21T00:00:00.000Z").toISOString(),
  };
}

describe("Prediction contract", () => {
  test("accepts canonical schema shape", () => {
    const errors = validatePredictionRecord(buildValidRecord());
    expect(errors).toHaveLength(0);
  });

  test("rejects invalid confidence bounds", () => {
    const invalid = buildValidRecord();
    invalid.confidence_score = 1.2;
    invalid.model_votes[0]!.confidence = -0.1;
    const errors = validatePredictionRecord(invalid);
    expect(errors.some((e) => e.includes("confidence_score"))).toBeTrue();
    expect(errors.some((e) => e.includes("model_votes[0].confidence"))).toBeTrue();
  });

  test("rejects invalid abstain vote payload", () => {
    const invalid = buildValidRecord();
    invalid.model_votes[2] = {
      model_id: "meta_model",
      decision: "abstain",
      confidence: 0.1,
    };
    const errors = validatePredictionRecord(invalid);
    expect(errors.some((e) => e.includes("null for abstain"))).toBeTrue();
  });

  test("rejects missing required model IDs", () => {
    const invalid = buildValidRecord();
    invalid.model_votes = [
      { model_id: "pattern_match_model", decision: "yes", confidence: 0.61 },
    ];
    const errors = validatePredictionRecord(invalid);
    expect(errors.some((e) => e.includes("aggregation_model"))).toBeTrue();
    expect(errors.some((e) => e.includes("meta_model"))).toBeTrue();
    expect(errors.some((e) => e.includes("confidence_model"))).toBeTrue();
    expect(errors.some((e) => e.includes("player_points_ml_model"))).toBeTrue();
  });
});

function buildReplayInput(): PredictionInput {
  return {
    season: 2026,
    gameContext: {
      id: "ctx-1",
      gameId: "game-replay-1",
      homeWins: 20,
      homeLosses: 10,
      homePpg: 112,
      homeOppg: 108,
      homePace: 99,
      homeRebPg: 44,
      homeAstPg: 25,
      homeFg3Pct: 0.37,
      homeFtPct: 0.79,
      homeRankOff: 6,
      homeRankDef: 11,
      homeRankPace: 15,
      homeStreak: 3,
      awayWins: 18,
      awayLosses: 12,
      awayPpg: 109,
      awayOppg: 110,
      awayPace: 98,
      awayRebPg: 42,
      awayAstPg: 23,
      awayFg3Pct: 0.35,
      awayFtPct: 0.76,
      awayRankOff: 10,
      awayRankDef: 16,
      awayRankPace: 18,
      awayStreak: -1,
      homeRestDays: 1,
      awayRestDays: 0,
      homeIsB2b: false,
      awayIsB2b: true,
      homeInjuryOutCount: 0,
      homeInjuryDoubtfulCount: 0,
      homeInjuryQuestionableCount: 1,
      homeInjuryProbableCount: 0,
      awayInjuryOutCount: 1,
      awayInjuryDoubtfulCount: 0,
      awayInjuryQuestionableCount: 0,
      awayInjuryProbableCount: 0,
      homeLineupCertainty: 0.9,
      awayLineupCertainty: 0.8,
      homeLateScratchRisk: 0.1,
      awayLateScratchRisk: 0.2,
      h2hHomeWins: 0,
      h2hAwayWins: 0,
    } as any,
    odds: {
      spreadHome: -3.5,
      totalOver: 224.5,
      mlHome: -145,
      mlAway: 125,
    },
    deployedV2Patterns: [
      {
        id: "pat-replay-1",
        outcomeType: "HOME_WIN:game",
        conditions: ["season:2026", "away_is_b2b:true"],
        posteriorHitRate: 0.62,
        edge: 0.08,
        score: 0.68,
        n: 140,
      },
    ],
    featureBins: new Map(),
    metaModel: null,
    gamePlayerContext: {
      homeTopScorer: { id: 1, name: "Home Star", stat: 27 },
      homeTopRebounder: { id: 2, name: "Home Big", stat: 11 },
      homeTopPlaymaker: { id: 3, name: "Home Guard", stat: 8 },
      awayTopScorer: { id: 4, name: "Away Star", stat: 25 },
      awayTopRebounder: { id: 5, name: "Away Big", stat: 10 },
      awayTopPlaymaker: { id: 6, name: "Away Guard", stat: 7 },
    },
    propsForGame: [],
    maxBetPicksPerGame: 3,
    fallbackAmericanOdds: -110,
    predictionGeneratedAtIso: new Date("2026-03-21T00:00:00.000Z").toISOString(),
    modelBundleVersion: "test-bundle-v1",
    sourceTimestamps: {
      oddsTimestampUsed: new Date("2026-03-20T21:00:00.000Z").toISOString(),
      statsSnapshotCutoff: new Date("2026-03-21T00:00:00.000Z").toISOString(),
      injuryLineupCutoff: new Date("2026-03-21T00:00:00.000Z").toISOString(),
    },
  };
}

describe("Replayability", () => {
  test("fixed inputs produce stable canonical replay payload", () => {
    const outputA = generateGamePredictions(buildReplayInput());
    const outputB = generateGamePredictions(buildReplayInput());
    expect(outputA.canonicalPredictions.length).toBeGreaterThan(0);
    expect(outputB.canonicalPredictions.length).toBeGreaterThan(0);

    const a = { ...outputA.canonicalPredictions[0]! };
    const b = { ...outputB.canonicalPredictions[0]! };
    a.generated_at = "IGNORED";
    b.generated_at = "IGNORED";

    expect(a).toEqual(b);
    expect(a.feature_snapshot_id).toBe(outputA.featureSnapshotId);
    expect(outputA.rejectedPatternDiagnostics).toHaveLength(0);
  });

  test("golden canonical record stays stable", () => {
    const output = generateGamePredictions(buildReplayInput());
    const canonical = output.canonicalPredictions[0]!;
    expect(canonical).toMatchObject({
      prediction_id: expect.stringMatching(/^pred_/),
      game_id: "game-replay-1",
      market: "moneyline",
      selection: "HOME_WIN:game",
      model_votes: [
        { model_id: "pattern_match_model", decision: "yes", confidence: 0.62 },
        { model_id: "aggregation_model", decision: "yes", confidence: 0.62 },
        { model_id: "meta_model", decision: "abstain", confidence: null },
        { model_id: "confidence_model", decision: "yes", confidence: 0.68 },
        { model_id: "player_points_ml_model", decision: "abstain", confidence: null },
      ],
      confidence_score: 0.5800000000000001,
      edge_estimate: 0.01886938775510205,
      supporting_patterns: ["pat-replay-1"],
      prediction_contract_version: PREDICTION_CONTRACT_VERSION,
      ranking_policy_version: "rank_v1",
      aggregation_policy_version: "agg_v2",
      model_bundle_version: "test-bundle-v1",
      feature_schema_version: "v1",
      feature_snapshot_id: "fs_64c407de",
      feature_snapshot_payload: {
        game_id: "game-replay-1",
        season: 2026,
        feature_schema_version: "v1",
        tokens: ["away_is_b2b:true", "home_is_b2b:false", "season:2026"],
        generated_from: "pregame_token_set",
        odds_timestamp_used: "2026-03-20T21:00:00.000Z",
        stats_snapshot_cutoff: "2026-03-21T00:00:00.000Z",
        injury_lineup_cutoff: "2026-03-21T00:00:00.000Z",
      },
      quality_context: {
        raw_win_probability: expect.any(Number),
        calibrated_win_probability: expect.any(Number),
        market_type: "moneyline",
        lane_tag: "moneyline",
        regime_tags: expect.any(Array),
      },
      generated_at: "2026-03-21T00:00:00.000Z",
    });
  });
});

describe("Feature snapshot determinism", () => {
  test("is deterministic regardless of token insertion order", () => {
    const a = createFeatureSnapshotId({
      gameId: "g1",
      season: 2026,
      tokens: new Set(["home_streak:Q4", "away_streak:Q2", "season:2026"]),
    });
    const b = createFeatureSnapshotId({
      gameId: "g1",
      season: 2026,
      tokens: new Set(["season:2026", "away_streak:Q2", "home_streak:Q4"]),
    });
    expect(a).toBe(b);
  });

  test("changes when features change", () => {
    const a = createFeatureSnapshotId({
      gameId: "g1",
      season: 2026,
      tokens: new Set(["a", "b"]),
    });
    const b = createFeatureSnapshotId({
      gameId: "g1",
      season: 2026,
      tokens: new Set(["a", "c"]),
    });
    expect(a).not.toBe(b);
  });
});

describe("Pattern validity gate", () => {
  test("passes with default thresholds and safety checks", () => {
    const result = evaluatePatternValidity({
      sampleSize: 120,
      posteriorHitRate: 0.56,
      hasOutOfSampleEvidence: true,
      hasLeakageRisk: false,
    });
    expect(result.pass).toBeTrue();
    expect(result.reasons).toHaveLength(0);
  });

  test("fails when below threshold and leakage check fails", () => {
    const result = evaluatePatternValidity({
      sampleSize: 25,
      posteriorHitRate: 0.51,
      hasOutOfSampleEvidence: false,
      hasLeakageRisk: true,
    });
    expect(result.pass).toBeFalse();
    expect(result.reasons).toContain("insufficient_sample_size");
    expect(result.reasons).toContain("below_min_win_rate");
    expect(result.reasons).toContain("missing_out_of_sample_evidence");
    expect(result.reasons).toContain("leakage_check_failed");
  });
});
