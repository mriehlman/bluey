import type { ModelVote } from "./predictionContract";

export type VoteSourceFamily =
  | "pattern"
  | "ml_model"
  | "injury_lineup"
  | "market_signal"
  | "heuristic"
  | "meta"
  | "unknown";

export type VoteReliabilityInfo = {
  hitRate: number;
  sampleSize: number;
  laneKey?: string;
};

export type SourceReliabilityMap = Record<string, VoteReliabilityInfo | undefined>;

export type VoteWeightBreakdownRow = {
  modelId: string;
  decision: "yes" | "no" | "abstain";
  sourceFamily: VoteSourceFamily;
  laneKeyUsed: string | null;
  confidence: number | null;
  baseWeight: number;
  reliabilityWeight: number;
  sampleConfidenceWeight: number;
  uncertaintyDiscount: number;
  laneFitWeight: number;
  preStrengthVoteWeight: number;
  voteWeightingStrength: number;
  finalVoteWeight: number;
  weightedContribution: number;
};

export type WeightedVoteResult = {
  weightedSupportScore: number;
  weightedOppositionScore: number;
  weightedConsensusScore: number;
  weightedDisagreementPenalty: number;
  voteWeightBreakdown: VoteWeightBreakdownRow[];
  dominantSourceFamily: VoteSourceFamily | null;
};

function clamp01(n: number): number {
  return Math.max(0, Math.min(1, n));
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

export function normalizeVoteSourceFromModelId(modelId: string): VoteSourceFamily {
  const id = modelId.toLowerCase();
  if (id.includes("pattern")) return "pattern";
  if (id.includes("meta")) return "meta";
  if (id.includes("ml")) return "ml_model";
  if (id.includes("injury") || id.includes("lineup")) return "injury_lineup";
  if (id.includes("market")) return "market_signal";
  if (id.includes("confidence_vote") || id.includes("aggregation_vote")) return "heuristic";
  if (id.includes("other_vote")) return "unknown";
  if (id.includes("aggregation") || id.includes("confidence")) return "heuristic";
  return "unknown";
}

export function normalizeVoteSource(vote: ModelVote): VoteSourceFamily {
  return normalizeVoteSourceFromModelId(vote.model_id);
}

export function laneKeyFromTags(args: { laneTag: string; regimeTags: string[] }): string {
  const regimeKey = args.regimeTags.length > 0 ? args.regimeTags.slice(0, 2).join("+") : "any";
  return `${args.laneTag}|${regimeKey}`;
}

export function reliabilityWeightFromHitRate(hitRate: number): number {
  // Keep first version conservative: 0.7..1.3
  return clamp(1 + (hitRate - 0.5) * 1.5, 0.7, 1.3);
}

export function computeWeightedVotes(args: {
  votes: ModelVote[];
  laneTag: string;
  regimeTags: string[];
  uncertaintyScore: number;
  sourceReliabilityMap: SourceReliabilityMap;
  sampleThreshold?: number;
  voteWeightingStrength?: number;
}): WeightedVoteResult {
  const sampleThreshold = Math.max(1, args.sampleThreshold ?? 50);
  const laneKey = laneKeyFromTags({ laneTag: args.laneTag, regimeTags: args.regimeTags });
  const voteWeightingStrength = Math.max(0.1, args.voteWeightingStrength ?? 1);
  const rows: VoteWeightBreakdownRow[] = [];

  type FamilyAgg = { support: number; opposition: number; totalWeight: number };
  const familyAgg = new Map<VoteSourceFamily, FamilyAgg>();

  for (const vote of args.votes) {
    const sourceFamily = normalizeVoteSource(vote);
    const confidence = vote.confidence == null ? null : clamp01(vote.confidence);
    const baseWeight = 1;

    const laneSpecificKey = `${sourceFamily}::${laneKey}`;
    const laneWideKey = `${sourceFamily}::${args.laneTag}`;
    const globalKey = `${sourceFamily}::global`;
    const laneSpecific = args.sourceReliabilityMap[laneSpecificKey];
    const laneWide = args.sourceReliabilityMap[laneWideKey];
    const global = args.sourceReliabilityMap[globalKey];
    const rel = laneSpecific ?? laneWide ?? global ?? null;

    const rawReliabilityWeight = rel ? reliabilityWeightFromHitRate(rel.hitRate) : 1;
    const sampleConfidenceWeight =
      rel == null ? 1 : clamp(rel.sampleSize / sampleThreshold, 0, 1);

    // Low-sample reliability gets shrunk toward neutral (=1).
    const reliabilityWeight = 1 + (rawReliabilityWeight - 1) * sampleConfidenceWeight;
    const uncertaintyDiscount = clamp(1 - clamp01(args.uncertaintyScore) * 0.35, 0.65, 1);
    const laneFitWeight = laneSpecific || laneWide ? 1.05 : 1;
    const preStrengthVoteWeight =
      baseWeight *
      reliabilityWeight *
      uncertaintyDiscount *
      laneFitWeight;
    // Strength tuning only scales distance from neutral weighting using exponentiation.
    const finalVoteWeight = Math.pow(preStrengthVoteWeight, voteWeightingStrength);

    let weightedContribution = 0;
    if (vote.decision !== "abstain" && confidence != null) {
      weightedContribution = finalVoteWeight * confidence;
      const agg = familyAgg.get(sourceFamily) ?? { support: 0, opposition: 0, totalWeight: 0 };
      if (vote.decision === "yes") agg.support += weightedContribution;
      if (vote.decision === "no") agg.opposition += weightedContribution;
      agg.totalWeight += finalVoteWeight;
      familyAgg.set(sourceFamily, agg);
    }

    rows.push({
      modelId: vote.model_id,
      decision: vote.decision,
      sourceFamily,
      laneKeyUsed: rel ? (laneSpecific ? laneSpecificKey : laneWide ? laneWideKey : globalKey) : null,
      confidence,
      baseWeight,
      reliabilityWeight,
      sampleConfidenceWeight,
      uncertaintyDiscount,
      laneFitWeight,
      preStrengthVoteWeight,
      voteWeightingStrength,
      finalVoteWeight,
      weightedContribution,
    });
  }

  // Family-level anti-domination: average by family then sum
  let weightedSupportScore = 0;
  let weightedOppositionScore = 0;
  let dominantSourceFamily: VoteSourceFamily | null = null;
  let dominantScore = -Infinity;
  for (const [family, agg] of familyAgg.entries()) {
    const avgSupport = agg.totalWeight > 0 ? agg.support / agg.totalWeight : 0;
    const avgOpp = agg.totalWeight > 0 ? agg.opposition / agg.totalWeight : 0;
    weightedSupportScore += avgSupport;
    weightedOppositionScore += avgOpp;
    const familyPower = avgSupport + avgOpp;
    if (familyPower > dominantScore) {
      dominantScore = familyPower;
      dominantSourceFamily = family;
    }
  }

  const totalSignal = weightedSupportScore + weightedOppositionScore;
  const supportRatio = totalSignal > 0 ? weightedSupportScore / totalSignal : 0;
  const disagreementRatio =
    totalSignal > 0 ? Math.min(weightedSupportScore, weightedOppositionScore) / totalSignal : 0;
  const weightedDisagreementPenalty = disagreementRatio * 0.25;
  const weightedConsensusScore = clamp01(supportRatio - weightedDisagreementPenalty);

  return {
    weightedSupportScore,
    weightedOppositionScore,
    weightedConsensusScore,
    weightedDisagreementPenalty,
    voteWeightBreakdown: rows,
    dominantSourceFamily,
  };
}
