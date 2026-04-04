import { describe, expect, test } from "bun:test";
import type { ModelVote } from "./predictionContract";
import { computeWeightedVotes } from "./voteWeighting";

function vote(model_id: string, decision: "yes" | "no" | "abstain", confidence: number | null): ModelVote {
  return { model_id, decision, confidence };
}

describe("voteWeighting v1", () => {
  test("missing reliability stays near neutral", () => {
    const result = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0,
      sourceReliabilityMap: {},
    });
    expect(result.voteWeightBreakdown[0]?.finalVoteWeight).toBeCloseTo(1, 6);
  });

  test("low sample shrinks reliability effect", () => {
    const result = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0,
      sourceReliabilityMap: {
        "pattern::global": { hitRate: 0.8, sampleSize: 5 },
      },
      sampleThreshold: 100,
    });
    const w = result.voteWeightBreakdown[0]?.finalVoteWeight ?? 0;
    expect(w).toBeGreaterThan(1);
    expect(w).toBeLessThan(1.05);
  });

  test("strong reliable source outweighs weak sources", () => {
    const result = computeWeightedVotes({
      votes: [
        vote("meta_model", "yes", 0.75),
        vote("aggregation_model", "yes", 0.55),
        vote("confidence_model", "yes", 0.55),
      ],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0,
      sourceReliabilityMap: {
        "meta::global": { hitRate: 0.72, sampleSize: 200 },
        "heuristic::global": { hitRate: 0.52, sampleSize: 200 },
      },
    });
    expect(result.weightedConsensusScore).toBeGreaterThan(0.6);
    const metaRow = result.voteWeightBreakdown.find((r) => r.sourceFamily === "meta");
    const heuristicRows = result.voteWeightBreakdown.filter((r) => r.sourceFamily === "heuristic");
    expect(metaRow?.finalVoteWeight ?? 0).toBeGreaterThan(heuristicRows[0]?.finalVoteWeight ?? 0);
  });

  test("disagreement reduces consensus", () => {
    const agree = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.75), vote("meta_model", "yes", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0.1,
      sourceReliabilityMap: {
        "pattern::global": { hitRate: 0.62, sampleSize: 120 },
        "meta::global": { hitRate: 0.62, sampleSize: 120 },
      },
    });
    const disagree = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.75), vote("meta_model", "no", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0.1,
      sourceReliabilityMap: {
        "pattern::global": { hitRate: 0.62, sampleSize: 120 },
        "meta::global": { hitRate: 0.62, sampleSize: 120 },
      },
    });
    expect(disagree.weightedDisagreementPenalty).toBeGreaterThan(agree.weightedDisagreementPenalty);
    expect(disagree.weightedConsensusScore).toBeLessThan(agree.weightedConsensusScore);
  });

  test("duplicate family votes are family-averaged (capped impact)", () => {
    const oneVote = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0,
      sourceReliabilityMap: { "pattern::global": { hitRate: 0.65, sampleSize: 200 } },
    });
    const twoVotesSameFamily = computeWeightedVotes({
      votes: [vote("pattern_match_model", "yes", 0.7), vote("pattern_match_model_v2", "yes", 0.7)],
      laneTag: "moneyline",
      regimeTags: [],
      uncertaintyScore: 0,
      sourceReliabilityMap: { "pattern::global": { hitRate: 0.65, sampleSize: 200 } },
    });
    expect(twoVotesSameFamily.weightedSupportScore).toBeCloseTo(oneVote.weightedSupportScore, 6);
  });
});
