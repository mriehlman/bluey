import {
  buildPregameTokenSet,
  matchDeployedPatterns,
  type DeployedPatternV2,
  type FeatureBinDef,
  type OddsLite,
} from "./v2PregameMatching";
import {
  betFamilyForOutcome,
  evaluateSuggestedPlayQualityGate,
  isOutcomeActionableForMarket,
  outcomeDedupFamily,
  selectDiversifiedBetPicks,
  impliedProbFromAmerican,
  payoutFromAmerican,
} from "./productionPickSelection";
import { scoreMetaModel, isLowSpecificityPattern, type MetaModel, type GameContextSignals } from "../patterns/metaModelCore";
import { PICK_QUALITY_TUNING, PREDICTION_TUNING } from "../config/tuning";
import type { GameContext } from "@prisma/client";
import {
  buildPickQualityContext,
  calibrateProbability,
  computeSourceReliabilityScore,
  deriveLaneTag,
  deriveMarketFamily,
  marketFamilyFromBetFamily,
  sourceFamilyFromModelId,
  type CalibrationArtifact,
  type SourceReliabilitySnapshot,
} from "./pickQuality";
import { computeWeightedVotes, normalizeVoteSourceFromModelId, type SourceReliabilityMap } from "./voteWeighting";
import {
  AGGREGATION_POLICY_VERSION,
  createFeatureSnapshotId,
  createFeatureSnapshotPayload,
  createPredictionId,
  DEFAULT_MODEL_BUNDLE_VERSION,
  FEATURE_SCHEMA_VERSION,
  PREDICTION_CONTRACT_VERSION,
  REQUIRED_MODEL_VOTE_IDS,
  RANKING_POLICY_VERSION,
  type ModelVote,
  type PredictionMarket,
  type PredictionRecord,
} from "./predictionContract";
import {
  evaluatePatternValidity,
  extractFeatureKeysFromConditions,
  inferLeakageRiskFromConditions,
  inferLeakageRiskFromFeatureMetadata,
} from "../patterns/patternValidity";

// ── Re-exports for callers ─────────────────────────────────────────────────────

export type { DeployedPatternV2, FeatureBinDef, OddsLite } from "./v2PregameMatching";
export type { MetaModel } from "../patterns/metaModelCore";
export { loadMetaModel } from "../patterns/metaModelCore";
export { buildPregameTokenSet, loadLatestFeatureBins, matchDeployedPatterns } from "./v2PregameMatching";
export { impliedProbFromAmerican, payoutFromAmerican } from "./productionPickSelection";

// ── Types ───────────────────────────────────────────────────────────────────────

export type PlayerInfo = { id: number; name: string; stat: number };

export type GamePlayerContext = {
  homeTopScorer: PlayerInfo | null;
  homeTopRebounder: PlayerInfo | null;
  homeTopPlaymaker: PlayerInfo | null;
  awayTopScorer: PlayerInfo | null;
  awayTopRebounder: PlayerInfo | null;
  awayTopPlaymaker: PlayerInfo | null;
};

export type PlayerPropRow = {
  gameId: string;
  playerId: number;
  market: string;
  line: number | null;
  overPrice: number | null;
  underPrice: number | null;
  player: { firstname: string | null; lastname: string | null };
};

export type V2PlayerTarget = {
  id: number;
  name: string;
  stat: "ppg" | "rpg" | "apg";
  statValue: number;
  rationale: string;
};

export type SuggestedMarketPick = {
  playerId: number;
  playerName: string;
  market: string;
  line: number;
  overPrice: number;
  impliedProb: number;
  estimatedProb: number;
  modelEstimatedProb?: number;
  edge: number;
  ev: number;
  label: string;
  marketType: string;
  marketSubType: string | null;
  selectionSide: "home" | "away" | "over" | "under" | "player_over" | "other";
  lineSnapshot: number | null;
  priceSnapshot: number | null;
};

export type SuggestedPlay = {
  outcomeType: string;
  displayLabel: string | null;
  confidence: number;
  posteriorHitRate: number;
  edge: number;
  metaScore: number | null;
  votes: number;
  playerTarget: V2PlayerTarget | null;
  marketPick: SuggestedMarketPick | null;
  rawWinProbability: number;
  calibratedWinProbability: number;
  impliedMarketProbability: number | null;
  edgeVsMarket: number | null;
  expectedValueScore: number | null;
  marketType: string;
  marketSubType: string | null;
  selectionSide: "home" | "away" | "over" | "under" | "player_over" | "other";
  lineSnapshot: number | null;
  priceSnapshot: number | null;
  laneTag: string;
  regimeTags: string[];
  sourceReliabilityScore: number | null;
  uncertaintyScore: number;
  uncertaintyPenaltyApplied: number;
  adjustedEdgeScore: number | null;
  weightedSupportScore?: number;
  weightedOppositionScore?: number;
  weightedConsensusScore?: number;
  weightedDisagreementPenalty?: number;
  voteWeightBreakdown?: Array<{
    modelId: string;
    sourceFamily: string;
    decision: "yes" | "no" | "abstain";
    confidence: number | null;
    baseWeight?: number;
    reliabilityWeight?: number;
    sampleConfidenceWeight?: number;
    uncertaintyDiscount?: number;
    laneFitWeight?: number;
    preStrengthVoteWeight?: number;
    voteWeightingStrength?: number;
    finalVoteWeight: number;
    weightedContribution: number;
  }>;
  dominantSourceFamily?: string | null;
};

type GateStageStats = {
  considered: number;
  passed: number;
  rejected: number;
  reasons: Record<string, number>;
};

export type GateDiagnostics = {
  quality: GateStageStats;
  bettable: GateStageStats;
};

export type DiscoveryMatch = DeployedPatternV2 & {
  playerTarget: V2PlayerTarget | null;
};

export type PredictionInput = {
  season: number;
  gameContext: GameContext;
  odds: OddsLite | null;
  deployedV2Patterns: DeployedPatternV2[];
  featureBins: Map<string, FeatureBinDef>;
  metaModel: MetaModel | null;
  gamePlayerContext: GamePlayerContext;
  propsForGame: PlayerPropRow[];
  maxBetPicksPerGame: number;
  fallbackAmericanOdds?: number;
  /** If false, disable fallback -110 style market pricing when source prices are missing. */
  allowFallbackMarketOdds?: boolean;
  includeDebugPlays?: boolean;
  /** Override excludeFamilies (e.g. [] for backfill to capture all pick types). */
  overrideExcludeFamilies?: readonly string[];
  predictionGeneratedAtIso?: string;
  modelBundleVersion?: string;
  sourceTimestamps?: {
    oddsTimestampUsed?: string | null;
    statsSnapshotCutoff?: string | null;
    injuryLineupCutoff?: string | null;
  };
  dynamicVoteWeightingEnabled?: boolean;
  voteWeightingStrength?: number;
  voteWeightingVersion?: "legacy" | "weighted_v1";
  calibrationArtifacts?: CalibrationArtifact[];
  sourceReliabilitySnapshot?: SourceReliabilitySnapshot | null;
  strictActionabilityGatesEnabled?: boolean;
};

export type ModelPick = {
  outcomeType: string;
  displayLabel: string;
  modelProbability: number;
  posteriorHitRate: number;
  metaScore: number | null;
  confidence: number;
  agreementCount: number;
  playerTarget: V2PlayerTarget | null;
  marketPick: SuggestedMarketPick | null;
  rawWinProbability: number;
  calibratedWinProbability: number;
  impliedMarketProbability: number | null;
  edgeVsMarket: number | null;
  expectedValueScore: number | null;
  laneTag: string;
  uncertaintyScore: number;
  adjustedEdgeScore: number | null;
  weightedConsensusScore?: number;
};

export type PredictionOutput = {
  discoveryV2Matches: DiscoveryMatch[];
  suggestedPlays: SuggestedPlay[];
  suggestedBetPicks: SuggestedPlay[];
  modelPicks: ModelPick[];
  canonicalPredictions: PredictionRecord[];
  featureSnapshotId: string;
  rejectedPatternDiagnostics: Array<{ patternId: string; reasons: string[] }>;
  gateDiagnostics?: GateDiagnostics;
};

// ── Suppression / curation helpers ──────────────────────────────────────────────

function isGenericPlayerOutcome(outcomeType: string): boolean {
  const base = outcomeType.replace(/:.*$/, "");
  return [
    "PLAYER_DOUBLE_DOUBLE",
    "PLAYER_TRIPLE_DOUBLE",
    "PLAYER_10_PLUS_REBOUNDS",
    "PLAYER_10_PLUS_ASSISTS",
    "PLAYER_30_PLUS",
    "PLAYER_40_PLUS",
    "PLAYER_5_PLUS_THREES",
  ].includes(base);
}

// ── Player-outcome requirement parsing ──────────────────────────────────────────

export function parsePlayerOutcomeRequirement(
  outcomeType: string,
): { actualStat: "points" | "rebounds" | "assists" | "fg3m"; line: number; label: string } | null {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base === "PLAYER_10_PLUS_ASSISTS" ||
    base === "HOME_TOP_ASSIST_10_PLUS" ||
    base === "AWAY_TOP_ASSIST_10_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_10_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_10_PLUS"
  ) {
    return { actualStat: "assists", line: 10, label: "ast" };
  }
  if (
    base === "HOME_TOP_ASSIST_8_PLUS" ||
    base === "AWAY_TOP_ASSIST_8_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_8_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_8_PLUS"
  ) {
    return { actualStat: "assists", line: 8, label: "ast" };
  }
  if (
    base === "PLAYER_10_PLUS_REBOUNDS" ||
    base === "HOME_TOP_REBOUNDER_10_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_10_PLUS"
  ) {
    return { actualStat: "rebounds", line: 10, label: "reb" };
  }
  if (base === "HOME_TOP_REBOUNDER_12_PLUS" || base === "AWAY_TOP_REBOUNDER_12_PLUS") {
    return { actualStat: "rebounds", line: 12, label: "reb" };
  }
  if (
    base === "PLAYER_30_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS"
  ) {
    return { actualStat: "points", line: 30, label: "pts" };
  }
  if (base === "PLAYER_40_PLUS") return { actualStat: "points", line: 40, label: "pts" };
  if (base === "HOME_TOP_SCORER_25_PLUS" || base === "AWAY_TOP_SCORER_25_PLUS") {
    return { actualStat: "points", line: 25, label: "pts" };
  }
  if (base === "PLAYER_5_PLUS_THREES") {
    return { actualStat: "fg3m", line: 5, label: "3PM" };
  }
  return null;
}

// ── Market-spec helpers ─────────────────────────────────────────────────────────

function marketSpecForOutcome(
  outcomeType: string,
  target: V2PlayerTarget | null,
): { market: string; requiredActual: "points" | "rebounds" | "assists" | "fg3m"; requiredLine: number } | null {
  const req = parsePlayerOutcomeRequirement(outcomeType);
  if (req) {
    const market =
      req.actualStat === "points"
        ? "player_points"
        : req.actualStat === "rebounds"
          ? "player_rebounds"
          : req.actualStat === "assists"
            ? "player_assists"
            : "player_threes";
    return { market, requiredActual: req.actualStat, requiredLine: req.line };
  }
  const base = outcomeType.replace(/:.*$/, "");
  const inferredActual =
    base.includes("ASSIST") || base.includes("PLAYMAKER")
      ? "assists"
      : base.includes("REBOUND")
        ? "rebounds"
        : base.includes("SCORER") || base.includes("POINT")
          ? "points"
          : base.includes("THREE")
            ? "fg3m"
            : null;
  if (!inferredActual) return null;
  const market =
    inferredActual === "points"
      ? "player_points"
      : inferredActual === "rebounds"
        ? "player_rebounds"
        : inferredActual === "assists"
          ? "player_assists"
          : "player_threes";
  const targetBaseline =
    inferredActual === "points" && target?.stat === "ppg"
      ? target.statValue
      : inferredActual === "rebounds" && target?.stat === "rpg"
        ? target.statValue
        : inferredActual === "assists" && target?.stat === "apg"
          ? target.statValue
          : null;
  const requiredLine = targetBaseline != null && Number.isFinite(targetBaseline)
    ? Math.max(0.5, targetBaseline)
    : 0.5;
  return { market, requiredActual: inferredActual, requiredLine };
}

function labelForMarketPick(
  playerName: string,
  market: string,
  line: number,
  overPrice: number,
  side: "over" | "under" = "over",
): string {
  if (market === "moneyline_home") {
    const oddsLabel = overPrice > 0 ? `+${overPrice}` : `${overPrice}`;
    return `Home ML @ ${oddsLabel}`;
  }
  if (market === "moneyline_away") {
    const oddsLabel = overPrice > 0 ? `+${overPrice}` : `${overPrice}`;
    return `Away ML @ ${oddsLabel}`;
  }
  const threshold = Math.floor(line) + 1;
  const suffix =
    market === "player_points"
      ? "points"
      : market === "player_rebounds"
        ? "rebounds"
        : market === "player_assists"
          ? "assists"
          : "threes";
  const oddsLabel = overPrice > 0 ? `+${overPrice}` : `${overPrice}`;
  if (side === "under") return `${playerName} under ${line} ${suffix} @ ${oddsLabel}`;
  return `${playerName} ${threshold}+ ${suffix} @ ${oddsLabel}`;
}

// ── Prop-based player pick helper ───────────────────────────────────────────────

function pickFromProps(
  propsForGame: PlayerPropRow[],
  market: string,
  minLine: number | null,
): { id: number; name: string; line: number | null; pOver: number | null } | null {
  const candidates = propsForGame
    .filter((r) => r.market === market)
    .filter((r) => (minLine == null ? true : (r.line ?? -Infinity) >= minLine));
  if (candidates.length === 0) return null;
  let best: (typeof candidates)[number] | null = null;
  let bestScore = -Infinity;
  for (const r of candidates) {
    const pOver = impliedProbFromAmerican(r.overPrice);
    const score = (pOver ?? 0) + (r.line ?? 0) * 1e-6;
    if (score > bestScore) {
      bestScore = score;
      best = r;
    }
  }
  if (!best) return null;
  const name = `${best.player.firstname} ${best.player.lastname}`.trim();
  return {
    id: best.playerId,
    name,
    line: best.line ?? null,
    pOver: impliedProbFromAmerican(best.overPrice),
  };
}

function targetStatFromGameCtx(args: {
  playerId: number;
  stat: "ppg" | "rpg" | "apg";
  gamePlayerCtx: GamePlayerContext;
}): number | null {
  const { playerId, stat, gamePlayerCtx } = args;
  const candidates = [
    gamePlayerCtx.homeTopScorer,
    gamePlayerCtx.homeTopRebounder,
    gamePlayerCtx.homeTopPlaymaker,
    gamePlayerCtx.awayTopScorer,
    gamePlayerCtx.awayTopRebounder,
    gamePlayerCtx.awayTopPlaymaker,
  ];
  const match = candidates.find((p) => p?.id === playerId) ?? null;
  if (!match) return null;
  if (stat === "ppg" || stat === "rpg" || stat === "apg") return match.stat;
  return null;
}

// ── Player target resolution ────────────────────────────────────────────────────

export function pickV2PlayerTarget(
  outcomeKey: string,
  gamePlayerCtx: GamePlayerContext,
  propsForGame: PlayerPropRow[],
): V2PlayerTarget | null {
  if (!gamePlayerCtx) return null;
  const outcome = outcomeKey.replace(/:.*$/, "");
  const pickBest = (
    a: { id: number; name: string; stat: number } | null,
    b: { id: number; name: string; stat: number } | null,
  ) => {
    if (!a && !b) return null;
    if (!a) return b;
    if (!b) return a;
    return a.stat >= b.stat ? a : b;
  };

  if (outcome === "PLAYER_10_PLUS_REBOUNDS") {
    const prop = pickFromProps(propsForGame, "player_rebounds", 9.5);
    if (prop) {
      const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
      const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
      const baselineFromCtx = targetStatFromGameCtx({
        playerId: prop.id,
        stat: "rpg",
        gamePlayerCtx,
      });
      return {
        id: prop.id,
        name: prop.name,
        stat: "rpg",
        // Use selected player's own baseline when known; otherwise fall back to offered line.
        statValue: baselineFromCtx ?? prop.line ?? 10,
        rationale: `Best prop over implied (${pct}) for 10+ rebounds (${lineStr})`,
      };
    }
    const p = pickBest(gamePlayerCtx.homeTopRebounder, gamePlayerCtx.awayTopRebounder);
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Top projected rebounder in this matchup" } : null;
  }
  if (outcome === "PLAYER_10_PLUS_ASSISTS") {
    const prop = pickFromProps(propsForGame, "player_assists", 9.5);
    if (prop) {
      const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
      const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
      const baselineFromCtx = targetStatFromGameCtx({
        playerId: prop.id,
        stat: "apg",
        gamePlayerCtx,
      });
      return {
        id: prop.id,
        name: prop.name,
        stat: "apg",
        statValue: baselineFromCtx ?? prop.line ?? 10,
        rationale: `Best prop over implied (${pct}) for 10+ assists (${lineStr})`,
      };
    }
    const p = pickBest(gamePlayerCtx.homeTopPlaymaker, gamePlayerCtx.awayTopPlaymaker);
    return p ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Top projected playmaker in this matchup" } : null;
  }
  if (
    outcome === "HOME_TOP_ASSIST_8_PLUS" || outcome === "HOME_TOP_ASSIST_10_PLUS" ||
    outcome === "HOME_TOP_PLAYMAKER_8_PLUS" || outcome === "HOME_TOP_PLAYMAKER_10_PLUS"
  ) {
    const p = gamePlayerCtx.homeTopPlaymaker;
    return p ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Home top playmaker by pregame context" } : null;
  }
  if (
    outcome === "AWAY_TOP_ASSIST_8_PLUS" || outcome === "AWAY_TOP_ASSIST_10_PLUS" ||
    outcome === "AWAY_TOP_PLAYMAKER_8_PLUS" || outcome === "AWAY_TOP_PLAYMAKER_10_PLUS"
  ) {
    const p = gamePlayerCtx.awayTopPlaymaker;
    return p ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Away top playmaker by pregame context" } : null;
  }
  if (outcome === "HOME_TOP_SCORER_25_PLUS" || outcome === "HOME_TOP_SCORER_30_PLUS") {
    const p = gamePlayerCtx.homeTopScorer;
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Home top scorer by pregame context" } : null;
  }
  if (outcome === "HOME_TOP_SCORER_EXCEEDS_AVG") {
    const p = gamePlayerCtx.homeTopScorer;
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Home top scorer baseline from pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_SCORER_25_PLUS" || outcome === "AWAY_TOP_SCORER_30_PLUS") {
    const p = gamePlayerCtx.awayTopScorer;
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Away top scorer by pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_SCORER_EXCEEDS_AVG") {
    const p = gamePlayerCtx.awayTopScorer;
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Away top scorer baseline from pregame context" } : null;
  }
  if (outcome === "HOME_TOP_REBOUNDER_10_PLUS" || outcome === "HOME_TOP_REBOUNDER_12_PLUS") {
    const p = gamePlayerCtx.homeTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Home top rebounder by pregame context" } : null;
  }
  if (outcome === "HOME_TOP_REBOUNDER_EXCEEDS_AVG") {
    const p = gamePlayerCtx.homeTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Home top rebounder baseline from pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_REBOUNDER_10_PLUS" || outcome === "AWAY_TOP_REBOUNDER_12_PLUS") {
    const p = gamePlayerCtx.awayTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Away top rebounder by pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_REBOUNDER_EXCEEDS_AVG") {
    const p = gamePlayerCtx.awayTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Away top rebounder baseline from pregame context" } : null;
  }
  if (outcome === "HOME_TOP_ASSIST_EXCEEDS_AVG") {
    const p = gamePlayerCtx.homeTopPlaymaker;
    return p ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Home top playmaker baseline from pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_ASSIST_EXCEEDS_AVG") {
    const p = gamePlayerCtx.awayTopPlaymaker;
    return p ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Away top playmaker baseline from pregame context" } : null;
  }
  if (outcome === "PLAYER_30_PLUS" || outcome === "PLAYER_40_PLUS" || outcome === "PLAYER_5_PLUS_THREES") {
    if (outcome === "PLAYER_30_PLUS") {
      const prop = pickFromProps(propsForGame, "player_points", 29.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
        const baselineFromCtx = targetStatFromGameCtx({
          playerId: prop.id,
          stat: "ppg",
          gamePlayerCtx,
        });
        return { id: prop.id, name: prop.name, stat: "ppg", statValue: baselineFromCtx ?? prop.line ?? 30, rationale: `Best prop over implied (${pct}) for 30+ points (${lineStr})` };
      }
    }
    if (outcome === "PLAYER_40_PLUS") {
      const prop = pickFromProps(propsForGame, "player_points", 39.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
        const baselineFromCtx = targetStatFromGameCtx({
          playerId: prop.id,
          stat: "ppg",
          gamePlayerCtx,
        });
        return { id: prop.id, name: prop.name, stat: "ppg", statValue: baselineFromCtx ?? prop.line ?? 40, rationale: `Best prop over implied (${pct}) for 40+ points (${lineStr})` };
      }
    }
    if (outcome === "PLAYER_5_PLUS_THREES") {
      const prop = pickFromProps(propsForGame, "player_threes", 4.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
        return { id: prop.id, name: prop.name, stat: "ppg", statValue: Math.max(gamePlayerCtx.homeTopScorer?.stat ?? 0, gamePlayerCtx.awayTopScorer?.stat ?? 0), rationale: `Best prop over implied (${pct}) for 5+ threes (${lineStr})` };
      }
    }
    const p = pickBest(gamePlayerCtx.homeTopScorer, gamePlayerCtx.awayTopScorer);
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Top projected scorer in this matchup" } : null;
  }
  if (outcome === "PLAYER_DOUBLE_DOUBLE" || outcome === "PLAYER_TRIPLE_DOUBLE") {
    if (outcome === "PLAYER_DOUBLE_DOUBLE") {
      const prop = pickFromProps(propsForGame, "player_double_double", 0.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        return { id: prop.id, name: prop.name, stat: "rpg", statValue: Math.max(gamePlayerCtx.homeTopRebounder?.stat ?? 0, gamePlayerCtx.awayTopRebounder?.stat ?? 0), rationale: `Best prop implied (${pct}) for double-double` };
      }
    }
    if (outcome === "PLAYER_TRIPLE_DOUBLE") {
      const prop = pickFromProps(propsForGame, "player_triple_double", 0.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        return { id: prop.id, name: prop.name, stat: "apg", statValue: Math.max(gamePlayerCtx.homeTopPlaymaker?.stat ?? 0, gamePlayerCtx.awayTopPlaymaker?.stat ?? 0), rationale: `Best prop implied (${pct}) for triple-double` };
      }
    }
    const p = pickBest(gamePlayerCtx.homeTopRebounder, gamePlayerCtx.awayTopRebounder);
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Double-double proxy target (top rebound profile)" } : null;
  }
  return null;
}

// ── Best market-backed pick selection ───────────────────────────────────────────

function selectBestMarketBackedPick(args: {
  outcomeType: string;
  target: V2PlayerTarget | null;
  propsForGame: PlayerPropRow[];
  baseProb: number;
  confidence?: number;
  supportN?: number;
  gameOdds?: OddsLite | null;
  defaultMarketPrice?: number;
  allowFallbackMarketOdds?: boolean;
}): SuggestedMarketPick | null {
  const allowFallbackMarketOdds = args.allowFallbackMarketOdds !== false;
  const { outcomeType, target, propsForGame, baseProb } = args;
  const confidence = Math.max(0, Math.min(1, args.confidence ?? 0));
  const support = Math.max(1, args.supportN ?? 1);
  const baseOutcome = outcomeType.replace(/:.*$/, "");

  if (baseOutcome === "HOME_WIN" || baseOutcome === "AWAY_WIN") {
    const consensusPrice = baseOutcome === "HOME_WIN" ? args.gameOdds?.mlHome : args.gameOdds?.mlAway;
    const fallbackPrice =
      allowFallbackMarketOdds &&
      Number.isFinite(args.defaultMarketPrice ?? NaN) && (args.defaultMarketPrice ?? 0) !== 0
        ? Number(args.defaultMarketPrice)
        : null;
    const price = consensusPrice != null && Number.isFinite(consensusPrice) && consensusPrice !== 0
      ? consensusPrice
      : fallbackPrice;
    if (price == null || !Number.isFinite(price) || price === 0) return null;
    if (price < PREDICTION_TUNING.maxNegativeAmericanOdds) return null;
    const implied = impliedProbFromAmerican(price);
    if (implied == null) return null;
    const mwMl = Math.min(0.85, 0.5 + confidence * 0.25);
    const est = Math.max(0.05, Math.min(0.95, mwMl * baseProb + (1 - mwMl) * implied));
    const ev = est * payoutFromAmerican(price) - (1 - est);
    const market = baseOutcome === "HOME_WIN" ? "moneyline_home" : "moneyline_away";
    return {
      playerId: 0,
      playerName: baseOutcome === "HOME_WIN" ? "Home" : "Away",
      market,
      line: 0,
      overPrice: price,
      impliedProb: implied,
      estimatedProb: est,
      modelEstimatedProb: baseProb,
      edge: est - implied,
      ev,
      label: labelForMarketPick(baseOutcome === "HOME_WIN" ? "Home" : "Away", market, 0, price),
      marketType: "moneyline",
      marketSubType: market,
      selectionSide: baseOutcome === "HOME_WIN" ? "home" : "away",
      lineSnapshot: 0,
      priceSnapshot: price,
    };
  }

  if (
    baseOutcome === "HOME_COVERED" || baseOutcome === "AWAY_COVERED" ||
    baseOutcome === "FAVORITE_COVERED" || baseOutcome === "UNDERDOG_COVERED"
  ) {
    const spread = args.gameOdds?.spreadHome;
    if (spread == null || !Number.isFinite(spread)) return null;
    const explicitFallbackPrice =
      Number.isFinite(args.defaultMarketPrice ?? NaN) && (args.defaultMarketPrice ?? 0) !== 0
        ? Number(args.defaultMarketPrice)
        : null;
    const price = explicitFallbackPrice ?? (allowFallbackMarketOdds ? -110 : null);
    if (price == null || !Number.isFinite(price) || price === 0) return null;
    if (price < PREDICTION_TUNING.maxNegativeAmericanOdds) return null;
    const implied = impliedProbFromAmerican(price);
    if (implied == null) return null;
    const mw = Math.min(0.85, 0.5 + confidence * 0.25);
    const est = Math.max(0.05, Math.min(0.95, mw * baseProb + (1 - mw) * implied));
    const ev = est * payoutFromAmerican(price) - (1 - est);
    const market =
      baseOutcome === "HOME_COVERED"
        ? "spread_home"
        : baseOutcome === "AWAY_COVERED"
          ? "spread_away"
          : baseOutcome === "FAVORITE_COVERED"
            ? "spread_favorite"
            : "spread_underdog";
    return {
      playerId: 0,
      playerName: "Game",
      market,
      line: spread,
      overPrice: price,
      impliedProb: implied,
      estimatedProb: est,
      modelEstimatedProb: baseProb,
      edge: est - implied,
      ev,
      label: `${baseOutcome.replaceAll("_", " ")} @ ${price > 0 ? `+${price}` : `${price}`}`,
      marketType: "spread",
      marketSubType: market,
      selectionSide:
        baseOutcome === "HOME_COVERED"
          ? "home"
          : baseOutcome === "AWAY_COVERED"
            ? "away"
            : "other",
      lineSnapshot: spread,
      priceSnapshot: price,
    };
  }

  if (
    baseOutcome === "OVER_HIT" || baseOutcome === "UNDER_HIT" ||
    baseOutcome.startsWith("TOTAL_OVER_") || baseOutcome.startsWith("TOTAL_UNDER_")
  ) {
    const total = args.gameOdds?.totalOver;
    if (total == null || !Number.isFinite(total)) return null;
    const explicitFallbackPrice =
      Number.isFinite(args.defaultMarketPrice ?? NaN) && (args.defaultMarketPrice ?? 0) !== 0
        ? Number(args.defaultMarketPrice)
        : null;
    const price = explicitFallbackPrice ?? (allowFallbackMarketOdds ? -110 : null);
    if (price == null || !Number.isFinite(price) || price === 0) return null;
    if (price < PREDICTION_TUNING.maxNegativeAmericanOdds) return null;
    const implied = impliedProbFromAmerican(price);
    if (implied == null) return null;
    const mwTotal = Math.min(0.85, 0.5 + confidence * 0.25);
    const est = Math.max(0.05, Math.min(0.95, mwTotal * baseProb + (1 - mwTotal) * implied));
    const ev = est * payoutFromAmerican(price) - (1 - est);
    const market = baseOutcome.includes("UNDER") ? "total_under" : "total_over";
    return {
      playerId: 0,
      playerName: "Game",
      market,
      line: total,
      overPrice: price,
      impliedProb: implied,
      estimatedProb: est,
      modelEstimatedProb: baseProb,
      edge: est - implied,
      ev,
      label: `${market === "total_under" ? "Under" : "Over"} ${total} @ ${price > 0 ? `+${price}` : `${price}`}`,
      marketType: "total",
      marketSubType: market,
      selectionSide: market === "total_under" ? "under" : "over",
      lineSnapshot: total,
      priceSnapshot: price,
    };
  }

  if (!target) return null;
  const spec = marketSpecForOutcome(outcomeType, target);
  if (!spec) return null;

  const allCandidates = propsForGame.filter(
    (r) =>
      r.playerId === target.id &&
      r.market === spec.market &&
      r.line != null &&
      (r.overPrice != null || r.underPrice != null),
  );
  if (allCandidates.length === 0) return null;

  // Use realistic local alt-line windows per market so picks stay near actionable books.
  // If no line exists in-range, fall back to all offered lines.
  const ladderHalfSpan =
    spec.market === "player_threes"
      ? 1.5 // roughly +/- 3 made-threes ladder steps at half-point lines
      : spec.market === "player_assists" || spec.market === "player_rebounds"
        ? 2.5 // nearby assist/rebound ladder around baseline
        : 3.5; // points can support a slightly wider local ladder
  const minLine = Math.max(0.5, spec.requiredLine - ladderHalfSpan);
  const maxLine = spec.requiredLine + ladderHalfSpan;
  const nearCandidates = allCandidates.filter((r) => (r.line ?? 0) >= minLine && (r.line ?? 0) <= maxLine);
  const candidates = nearCandidates.length > 0 ? nearCandidates : allCandidates;

  const slope =
    spec.requiredActual === "fg3m"
      ? 0.09
      : spec.requiredActual === "assists" || spec.requiredActual === "rebounds"
        ? 0.07
        : 0.04;
  let best: SuggestedMarketPick | null = null;
  let bestEv = -Infinity;

  for (const c of candidates) {
    const offeredThreshold = Math.floor((c.line ?? 0) + 0.5);
    const lineDelta = spec.requiredLine - offeredThreshold;
    const statBonus =
      target.stat === "ppg" && spec.requiredActual === "points"
        ? (target.statValue - offeredThreshold) * 0.02
        : target.stat === "rpg" && spec.requiredActual === "rebounds"
          ? (target.statValue - offeredThreshold) * 0.025
          : target.stat === "apg" && spec.requiredActual === "assists"
            ? (target.statValue - offeredThreshold) * 0.03
            : 0;
    const estModel = Math.max(0.05, Math.min(0.95, baseProb + lineDelta * slope + statBonus));
    const modelWeight = Math.min(0.85, 0.5 + confidence * 0.25 + Math.min(0.1, (support - 1) * 0.02));
    const sideCandidates: Array<{ side: "over" | "under"; price: number | null; modelProb: number }> = [
      { side: "over", price: c.overPrice, modelProb: estModel },
      { side: "under", price: c.underPrice, modelProb: 1 - estModel },
    ];
    for (const sideCandidate of sideCandidates) {
      const price = sideCandidate.price;
      if (price == null || !Number.isFinite(price) || price === 0) continue;
      if (price < PREDICTION_TUNING.maxNegativeAmericanOdds) continue;
      const implied = impliedProbFromAmerican(price);
      if (implied == null) continue;
      const est = Math.max(
        0.05,
        Math.min(0.95, modelWeight * sideCandidate.modelProb + (1 - modelWeight) * implied),
      );
      const ev = est * payoutFromAmerican(price) - (1 - est);
      if (ev <= bestEv) continue;
      bestEv = ev;
      best = {
        playerId: c.playerId,
        playerName: `${c.player.firstname} ${c.player.lastname}`.trim(),
        market: c.market,
        line: c.line ?? 0,
        overPrice: price,
        impliedProb: implied,
        estimatedProb: est,
        modelEstimatedProb: sideCandidate.modelProb,
        edge: est - implied,
        ev,
        label: labelForMarketPick(
          `${c.player.firstname} ${c.player.lastname}`.trim(),
          c.market,
          c.line ?? 0,
          price,
          sideCandidate.side,
        ),
        marketType: "player_prop",
        marketSubType: `${c.market}_${sideCandidate.side}`,
        selectionSide: sideCandidate.side,
        lineSnapshot: c.line ?? 0,
        priceSnapshot: price,
      };
    }
  }

  return best;
}

// ── Gate diagnostics ────────────────────────────────────────────────────────────

function newGateStageStats(): GateStageStats {
  return { considered: 0, passed: 0, rejected: 0, reasons: {} };
}

function newGateDiagnostics(): GateDiagnostics {
  return { quality: newGateStageStats(), bettable: newGateStageStats() };
}

function recordGateEval(
  stage: GateStageStats,
  evalResult: { pass: boolean; reason?: string },
): void {
  stage.considered += 1;
  if (evalResult.pass) {
    stage.passed += 1;
    return;
  }
  stage.rejected += 1;
  const reason = evalResult.reason ?? "unknown";
  stage.reasons[reason] = (stage.reasons[reason] ?? 0) + 1;
}

function inferPredictionMarket(outcomeType: string, marketPick: SuggestedMarketPick | null): PredictionMarket {
  if (marketPick?.market?.startsWith("moneyline")) return "moneyline";
  if (marketPick?.market?.startsWith("spread")) return "spread";
  if (marketPick?.market?.startsWith("total")) return "total";
  if (marketPick?.market?.startsWith("player_")) return "player_prop";
  const base = outcomeType.replace(/:.*$/, "");
  if (base.includes("WIN")) return "moneyline";
  if (base.includes("COVERED")) return "spread";
  if (base.includes("OVER") || base.includes("UNDER")) return "total";
  if (base.includes("PLAYER") || base.includes("TOP_")) return "player_prop";
  return "other";
}

function clamp01(n: number): number {
  return Math.max(0, Math.min(1, n));
}

function buildVoteReliabilityMap(input: SourceReliabilitySnapshot | null): SourceReliabilityMap {
  const map: SourceReliabilityMap = {};
  if (!input) return map;
  for (const row of input.rows) {
    const rawSourceFamily = String(row.sourceFamily ?? "").trim();
    const sf = rawSourceFamily ? normalizeVoteSourceFromModelId(rawSourceFamily) : "";
    const mf = String(row.marketFamily ?? "global").trim() || "global";
    const laneTag = row.laneTag ?? null;
    if (!sf) continue;
    map[`${sf}::${mf}`] = {
      hitRate: Number(row.hitRate),
      sampleSize: Number(row.sampleSize),
      laneKey: laneTag ?? undefined,
    };
    if (laneTag) {
      map[`${sf}::${laneTag}`] = {
        hitRate: Number(row.hitRate),
        sampleSize: Number(row.sampleSize),
        laneKey: laneTag,
      };
    }
  }
  return map;
}

function parsePlayerPointsThreshold(outcomeType: string): number | null {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base === "PLAYER_30_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS"
  ) return 30;
  if (base === "PLAYER_40_PLUS") return 40;
  if (base === "HOME_TOP_SCORER_25_PLUS" || base === "AWAY_TOP_SCORER_25_PLUS") return 25;
  return null;
}

function inferPlayerSide(args: {
  playerId: number;
  gamePlayerCtx: GamePlayerContext;
}): "home" | "away" | null {
  const { playerId, gamePlayerCtx } = args;
  const homeIds = [
    gamePlayerCtx.homeTopScorer?.id,
    gamePlayerCtx.homeTopRebounder?.id,
    gamePlayerCtx.homeTopPlaymaker?.id,
  ];
  if (homeIds.includes(playerId)) return "home";
  const awayIds = [
    gamePlayerCtx.awayTopScorer?.id,
    gamePlayerCtx.awayTopRebounder?.id,
    gamePlayerCtx.awayTopPlaymaker?.id,
  ];
  if (awayIds.includes(playerId)) return "away";
  return null;
}

function buildPlayerPointsMlVote(args: {
  outcomeType: string;
  playerTarget: V2PlayerTarget | null;
  marketPick: SuggestedMarketPick | null;
  gameContext: GameContext;
  gamePlayerCtx: GamePlayerContext;
}): ModelVote {
  const threshold = parsePlayerPointsThreshold(args.outcomeType);
  if (threshold == null) {
    return { model_id: "player_points_ml_model", decision: "abstain", confidence: null };
  }
  if (!args.playerTarget || args.playerTarget.stat !== "ppg") {
    return { model_id: "player_points_ml_model", decision: "abstain", confidence: null };
  }

  const side = inferPlayerSide({ playerId: args.playerTarget.id, gamePlayerCtx: args.gamePlayerCtx });
  const lineupCertainty = side === "home"
    ? args.gameContext.homeLineupCertainty
    : side === "away"
      ? args.gameContext.awayLineupCertainty
      : null;
  const lateScratchRisk = side === "home"
    ? args.gameContext.homeLateScratchRisk
    : side === "away"
      ? args.gameContext.awayLateScratchRisk
      : null;
  const offeredThreshold = args.marketPick?.line != null ? Math.floor(args.marketPick.line) + 1 : null;
  const projectedPoints = args.playerTarget.statValue;

  // Distilled signal from the standalone points model lane: edge vs threshold first,
  // then adjust for market line and lineup volatility.
  let probability = 0.5 + (projectedPoints - threshold) * 0.07;
  if (offeredThreshold != null) {
    probability += (threshold - offeredThreshold) * 0.03;
  }
  if (lineupCertainty != null) {
    probability += (lineupCertainty - 0.75) * 0.2;
  }
  if (lateScratchRisk != null) {
    probability -= lateScratchRisk * 0.25;
  }
  probability = clamp01(Math.max(0.05, Math.min(0.95, probability)));

  if (probability >= 0.53) {
    return {
      model_id: "player_points_ml_model",
      decision: "yes",
      confidence: clamp01(0.5 + (probability - 0.5)),
    };
  }
  if (probability <= 0.47) {
    return {
      model_id: "player_points_ml_model",
      decision: "no",
      confidence: clamp01(0.5 + (0.5 - probability)),
    };
  }
  return { model_id: "player_points_ml_model", decision: "abstain", confidence: null };
}

export function evaluatePlayerPointsMlVote(args: {
  outcomeType: string;
  playerTarget: V2PlayerTarget | null;
  marketPick: SuggestedMarketPick | null;
  gameContext: GameContext;
  gamePlayerCtx: GamePlayerContext;
}): ModelVote {
  return buildPlayerPointsMlVote(args);
}

function buildModelVotes(args: {
  outcomeType: string;
  playerTarget: V2PlayerTarget | null;
  marketPick: SuggestedMarketPick | null;
  gameContext: GameContext;
  gamePlayerCtx: GamePlayerContext;
  confidence: number;
  posteriorHitRate: number;
  modelProbability: number;
  metaScore: number | null;
}): ModelVote[] {
  const voteById: Record<string, ModelVote> = {
    pattern_match_model: {
      model_id: "pattern_match_model",
      decision: args.posteriorHitRate >= 0.5 ? "yes" : "no",
      confidence: clamp01(args.posteriorHitRate),
    },
    aggregation_model: {
      model_id: "aggregation_model",
      decision: args.modelProbability >= 0.5 ? "yes" : "no",
      confidence: clamp01(args.modelProbability),
    },
    meta_model:
      args.metaScore == null
        ? {
            model_id: "meta_model",
            decision: "abstain",
            confidence: null,
          }
        : {
            model_id: "meta_model",
            decision: args.metaScore >= 0.5 ? "yes" : "no",
            confidence: clamp01(args.metaScore),
          },
    confidence_model: {
      model_id: "confidence_model",
      decision: args.confidence >= 0.5 ? "yes" : "no",
      confidence: clamp01(args.confidence),
    },
    player_points_ml_model: buildPlayerPointsMlVote({
      outcomeType: args.outcomeType,
      playerTarget: args.playerTarget,
      marketPick: args.marketPick,
      gameContext: args.gameContext,
      gamePlayerCtx: args.gamePlayerCtx,
    }),
  };
  const votes: ModelVote[] = REQUIRED_MODEL_VOTE_IDS.map((id) => voteById[id]);
  return votes;
}

function aggregateVotes(votes: ModelVote[]): {
  pass: boolean;
  confidence: number;
  reason: string | null;
  weightedSupportScore?: number;
  weightedOppositionScore?: number;
  weightedConsensusScore?: number;
  weightedDisagreementPenalty?: number;
  voteWeightBreakdown?: Array<{
    modelId: string;
    sourceFamily: string;
    decision: "yes" | "no" | "abstain";
    confidence: number | null;
    finalVoteWeight: number;
    weightedContribution: number;
  }>;
  dominantSourceFamily?: string | null;
} {
  const activeVotes = votes.filter((vote) => vote.decision !== "abstain");
  const abstains = votes.length - activeVotes.length;
  if (activeVotes.length < 2) {
    return { pass: false, confidence: 0, reason: "insufficient_non_abstain_votes" };
  }
  const highConfidenceNo = activeVotes.some(
    (vote) => vote.decision === "no" && (vote.confidence ?? 0) >= 0.75,
  );
  if (highConfidenceNo) {
    return { pass: false, confidence: 0, reason: "high_confidence_no_veto" };
  }
  const yesVotes = activeVotes.filter((vote) => vote.decision === "yes");
  const yesMean =
    yesVotes.length > 0
      ? yesVotes.reduce((sum, vote) => sum + (vote.confidence ?? 0), 0) / yesVotes.length
      : 0;
  const abstainPenalty = votes.length > 0 ? (abstains / votes.length) * 0.15 : 0;
  const confidence = clamp01(yesMean - abstainPenalty);
  return {
    pass: confidence >= 0.55,
    confidence,
    reason: confidence >= 0.55 ? null : "confidence_below_aggregation_threshold",
  };
}

function aggregateVotesDynamic(args: {
  votes: ModelVote[];
  laneTag: string;
  regimeTags: string[];
  uncertaintyScore: number;
  sourceReliabilityMap: SourceReliabilityMap;
  voteWeightingStrength: number;
}): {
  pass: boolean;
  confidence: number;
  reason: string | null;
  weightedSupportScore: number;
  weightedOppositionScore: number;
  weightedConsensusScore: number;
  weightedDisagreementPenalty: number;
  voteWeightBreakdown: Array<{
    modelId: string;
    sourceFamily: string;
    decision: "yes" | "no" | "abstain";
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
  }>;
  dominantSourceFamily: string | null;
} {
  const activeVotes = args.votes.filter((vote) => vote.decision !== "abstain");
  const abstains = args.votes.length - activeVotes.length;
  if (activeVotes.length < 2) {
    return {
      pass: false,
      confidence: 0,
      reason: "insufficient_non_abstain_votes",
      weightedSupportScore: 0,
      weightedOppositionScore: 0,
      weightedConsensusScore: 0,
      weightedDisagreementPenalty: 0,
      voteWeightBreakdown: [],
      dominantSourceFamily: null,
    };
  }
  const highConfidenceNo = activeVotes.some(
    (vote) => vote.decision === "no" && (vote.confidence ?? 0) >= 0.75,
  );
  if (highConfidenceNo) {
    return {
      pass: false,
      confidence: 0,
      reason: "high_confidence_no_veto",
      weightedSupportScore: 0,
      weightedOppositionScore: 0,
      weightedConsensusScore: 0,
      weightedDisagreementPenalty: 0,
      voteWeightBreakdown: [],
      dominantSourceFamily: null,
    };
  }
  const weighted = computeWeightedVotes({
    votes: args.votes,
    laneTag: args.laneTag,
    regimeTags: args.regimeTags,
    uncertaintyScore: args.uncertaintyScore,
    sourceReliabilityMap: args.sourceReliabilityMap,
    sampleThreshold: PICK_QUALITY_TUNING.sourceReliabilityMinSample,
    voteWeightingStrength: args.voteWeightingStrength,
  });
  const abstainPenalty = args.votes.length > 0 ? (abstains / args.votes.length) * 0.1 : 0;
  const confidence = clamp01(weighted.weightedConsensusScore - abstainPenalty);
  return {
    pass: confidence >= 0.55,
    confidence,
    reason: confidence >= 0.55 ? null : "confidence_below_aggregation_threshold",
    weightedSupportScore: weighted.weightedSupportScore,
    weightedOppositionScore: weighted.weightedOppositionScore,
    weightedConsensusScore: weighted.weightedConsensusScore,
    weightedDisagreementPenalty: weighted.weightedDisagreementPenalty,
    voteWeightBreakdown: weighted.voteWeightBreakdown.map((r) => ({
      modelId: r.modelId,
      sourceFamily: r.sourceFamily,
      decision: r.decision,
      confidence: r.confidence,
      baseWeight: r.baseWeight,
      reliabilityWeight: r.reliabilityWeight,
      sampleConfidenceWeight: r.sampleConfidenceWeight,
      uncertaintyDiscount: r.uncertaintyDiscount,
      laneFitWeight: r.laneFitWeight,
      preStrengthVoteWeight: r.preStrengthVoteWeight,
      voteWeightingStrength: r.voteWeightingStrength,
      finalVoteWeight: r.finalVoteWeight,
      weightedContribution: r.weightedContribution,
    })),
    dominantSourceFamily: weighted.dominantSourceFamily,
  };
}

// ── Main engine ─────────────────────────────────────────────────────────────────

export function generateGamePredictions(input: PredictionInput): PredictionOutput {
  const {
    season,
    gameContext,
    odds,
    deployedV2Patterns,
    featureBins,
    metaModel,
    gamePlayerContext: gamePlayerCtx,
    propsForGame,
    maxBetPicksPerGame,
    fallbackAmericanOdds,
    allowFallbackMarketOdds = true,
    includeDebugPlays,
    overrideExcludeFamilies,
    predictionGeneratedAtIso,
    modelBundleVersion,
    sourceTimestamps,
    dynamicVoteWeightingEnabled = PICK_QUALITY_TUNING.enableDynamicVoteWeighting,
    voteWeightingStrength = PICK_QUALITY_TUNING.voteWeightingStrength,
    voteWeightingVersion = (dynamicVoteWeightingEnabled ? "weighted_v1" : "legacy"),
    calibrationArtifacts = [],
    sourceReliabilitySnapshot = null,
    strictActionabilityGatesEnabled = PICK_QUALITY_TUNING.enableStrictActionabilityGates,
  } = input;

  const gateDiagnostics = includeDebugPlays ? newGateDiagnostics() : undefined;
  const voteReliabilityMap = buildVoteReliabilityMap(sourceReliabilitySnapshot);

  const gameCtxSignals: GameContextSignals = {
    restAdvantage: (gameContext.homeRestDays ?? 1) - (gameContext.awayRestDays ?? 1),
    rankDiffOff: (gameContext.awayRankOff ?? 15) - (gameContext.homeRankOff ?? 15),
    rankDiffDef: (gameContext.homeRankDef ?? 15) - (gameContext.awayRankDef ?? 15),
    paceDelta: (gameContext.homePace ?? 100) - (gameContext.awayPace ?? 100),
    injuryImpact: Math.max(
      (gameContext.homeInjuryOutCount ?? 0) + (gameContext.homeInjuryDoubtfulCount ?? 0) * 0.5,
      (gameContext.awayInjuryOutCount ?? 0) + (gameContext.awayInjuryDoubtfulCount ?? 0) * 0.5,
    ),
  };

  const pregameTokenSet = buildPregameTokenSet({
    season,
    context: gameContext,
    odds,
    bins: featureBins,
    playerContext: {
      homeTopScorer: gamePlayerCtx.homeTopScorer,
      awayTopScorer: gamePlayerCtx.awayTopScorer,
    },
  });
  const featureSnapshotId = createFeatureSnapshotId({
    gameId: gameContext.gameId,
    season,
    tokens: pregameTokenSet,
  });
  const featureSnapshotPayload = createFeatureSnapshotPayload({
    gameId: gameContext.gameId,
    season,
    tokens: pregameTokenSet,
    oddsTimestampUsed: sourceTimestamps?.oddsTimestampUsed ?? null,
    statsSnapshotCutoff: sourceTimestamps?.statsSnapshotCutoff ?? null,
    injuryLineupCutoff: sourceTimestamps?.injuryLineupCutoff ?? null,
  });
  const generatedAt = predictionGeneratedAtIso ?? new Date().toISOString();

  const discoveryV2RawMatches = matchDeployedPatterns(
    pregameTokenSet,
    deployedV2Patterns,
    50,
  );

  const rejectedPatternDiagnostics: Array<{ patternId: string; reasons: string[] }> = [];
  const discoveryV2Matches = discoveryV2RawMatches.filter((p) => {
    if (isLowSpecificityPattern(p.conditions ?? [])) return false;
    const conditionLeakageRisk = inferLeakageRiskFromConditions(p.conditions ?? []);
    const featureLeakageRisk = inferLeakageRiskFromFeatureMetadata(
      extractFeatureKeysFromConditions(p.conditions ?? []),
    );
    const validity = evaluatePatternValidity({
      sampleSize: p.n,
      posteriorHitRate: p.posteriorHitRate,
      hasOutOfSampleEvidence: true,
      hasLeakageRisk: conditionLeakageRisk || featureLeakageRisk,
    });
    if (!validity.pass) {
      rejectedPatternDiagnostics.push({
        patternId: p.id,
        reasons: validity.reasons,
      });
    }
    return validity.pass;
  });

  // Model-only aggregation: ALL matched patterns, no market filter
  const allPlayMap = new Map<
    string,
    {
      dedupKey: string;
      outcomeType: string;
      bestConditions: string[];
      scoreSum: number;
      count: number;
      bestPosterior: number;
      bestEdge: number;
      bestN: number;
      playerTarget: V2PlayerTarget | null;
      supportingPatternIds: Set<string>;
    }
  >();

  for (const p of discoveryV2Matches) {
    const target = pickV2PlayerTarget(p.outcomeType, gamePlayerCtx, propsForGame);
    const dedupKey = `${outcomeDedupFamily(p.outcomeType)}|${target?.id ?? "none"}`;
    const existing = allPlayMap.get(dedupKey);
    if (!existing) {
      allPlayMap.set(dedupKey, {
        dedupKey,
        outcomeType: p.outcomeType,
        bestConditions: p.conditions ?? [],
        scoreSum: p.score,
        count: 1,
        bestPosterior: p.posteriorHitRate,
        bestEdge: p.edge,
        bestN: p.n,
        playerTarget: target,
        supportingPatternIds: new Set([p.id]),
      });
    } else {
      existing.scoreSum += p.score;
      existing.count += 1;
      if (p.posteriorHitRate > existing.bestPosterior) existing.bestPosterior = p.posteriorHitRate;
      if (p.edge > existing.bestEdge) existing.bestEdge = p.edge;
      if (p.n > existing.bestN) existing.bestN = p.n;
      if (p.score > existing.scoreSum / Math.max(1, existing.count)) {
        existing.outcomeType = p.outcomeType;
        existing.bestConditions = p.conditions ?? [];
      }
      existing.supportingPatternIds.add(p.id);
    }
  }

  const allRankedPlays = [...allPlayMap.values()]
    .map((r) => {
      const confidence = r.scoreSum / r.count;
      const agreementBonus = Math.min(0.3, (r.count - 1) * 0.05);
      const metaScore = metaModel
        ? scoreMetaModel(metaModel, {
            outcomeType: r.outcomeType,
            conditions: r.bestConditions,
            posteriorHitRate: r.bestPosterior,
            edge: r.bestEdge,
            score: confidence,
            n: r.bestN,
            gameContext: gameCtxSignals,
          })
        : null;
      const modelProbability = Math.min(0.95, (metaScore ?? r.bestPosterior) + agreementBonus);
      const marketPick = selectBestMarketBackedPick({
        outcomeType: r.outcomeType,
        target: r.playerTarget,
        propsForGame,
        baseProb: modelProbability,
        confidence,
        supportN: r.bestN,
        gameOdds: odds,
        defaultMarketPrice: fallbackAmericanOdds,
        allowFallbackMarketOdds,
      });
      const laneTag = deriveLaneTag(r.outcomeType, marketPick?.market ?? null);
      const marketFamily = deriveMarketFamily(marketPick?.marketType ?? "other", r.outcomeType);
      const calibrated = calibrateProbability({
        rawProbability: modelProbability,
        laneTag,
        marketFamily,
        artifacts: calibrationArtifacts,
        minSample: PICK_QUALITY_TUNING.calibrationMinSample,
      });
      const sourceFamilies = [
        "pattern_logic",
        "aggregation_vote",
        "confidence_vote",
        ...(metaScore != null ? ["meta_model"] : []),
        ...(parsePlayerPointsThreshold(r.outcomeType) != null ? ["ml_vote"] : []),
      ];
      const sourceReliabilityScore = computeSourceReliabilityScore({
        sourceFamilies,
        marketFamily,
        snapshot: sourceReliabilitySnapshot,
        minSample: PICK_QUALITY_TUNING.sourceReliabilityMinSample,
      });
      const activeVotesProxy = 3 + (metaScore != null ? 1 : 0) + (parsePlayerPointsThreshold(r.outcomeType) != null ? 1 : 0);
      const yesVotesProxy = [r.bestPosterior, modelProbability, confidence, metaScore ?? 0.5].filter((p) => p >= 0.5).length;
      const quality = buildPickQualityContext({
        outcomeType: r.outcomeType,
        market: marketPick?.market ?? null,
        marketSubType: marketPick?.marketSubType ?? null,
        rawWinProbability: modelProbability,
        calibratedWinProbability: calibrated.calibratedProbability,
        marketPriceAmerican: marketPick?.overPrice ?? null,
        lineSnapshot: marketPick?.lineSnapshot ?? marketPick?.line ?? null,
        gameContext,
        season,
        sourceReliabilityScore,
        supportCount: r.bestN,
        activeVotes: activeVotesProxy,
        yesVotes: yesVotesProxy,
        uncertaintyPenaltyScale: PICK_QUALITY_TUNING.uncertaintyPenaltyScale,
      });
      const outcomeLabel = r.outcomeType.replace(/:.*$/, "").replaceAll("_", " ");
      const targetLabel = r.playerTarget ? ` (${r.playerTarget.name})` : "";
      return {
        outcomeType: r.outcomeType,
        displayLabel: marketPick?.label ?? `${outcomeLabel}${targetLabel}`,
        confidence,
        posteriorHitRate: r.bestPosterior,
        edge: r.bestEdge,
        metaScore,
        votes: r.count,
        agreementCount: r.count,
        modelProbability,
        playerTarget: r.playerTarget,
        marketPick,
        supportingPatterns: [...r.supportingPatternIds],
        rawWinProbability: quality.rawWinProbability,
        calibratedWinProbability: quality.calibratedWinProbability,
        impliedMarketProbability: quality.impliedMarketProbability,
        edgeVsMarket: quality.edgeVsMarket,
        expectedValueScore: quality.expectedValueScore,
        marketType: quality.marketType,
        marketSubType: quality.marketSubType,
        selectionSide: marketPick?.selectionSide ?? quality.selectionSide,
        lineSnapshot: quality.lineSnapshot,
        priceSnapshot: quality.priceSnapshot,
        laneTag: quality.laneTag,
        regimeTags: quality.regimeTags,
        sourceReliabilityScore: quality.sourceReliabilityScore,
        uncertaintyScore: quality.uncertaintyScore,
        uncertaintyPenaltyApplied: quality.uncertaintyPenaltyApplied,
        adjustedEdgeScore: quality.adjustedEdgeScore,
      };
    })
    .sort(
      (a, b) =>
        b.modelProbability - a.modelProbability ||
        b.agreementCount - a.agreementCount ||
        (b.metaScore ?? -1) - (a.metaScore ?? -1),
    );

  // Model picks: top picks by pure model confidence, no market requirement
  const modelPicks: ModelPick[] = allRankedPlays
    .filter((p) => p.modelProbability >= 0.52)
    .slice(0, 15)
    .map((p) => ({
      outcomeType: p.outcomeType,
      displayLabel: p.displayLabel,
      modelProbability: p.modelProbability,
      posteriorHitRate: p.posteriorHitRate,
      metaScore: p.metaScore,
      confidence: p.confidence,
      agreementCount: p.agreementCount,
      playerTarget: p.playerTarget,
      marketPick: p.marketPick,
      rawWinProbability: p.rawWinProbability,
      calibratedWinProbability: p.calibratedWinProbability,
      impliedMarketProbability: p.impliedMarketProbability,
      edgeVsMarket: p.edgeVsMarket,
      expectedValueScore: p.expectedValueScore,
      laneTag: p.laneTag,
      uncertaintyScore: p.uncertaintyScore,
      adjustedEdgeScore: p.adjustedEdgeScore,
    }));

  // Bettable path: filter to actionable outcomes for market-based picks
  const actionableDiscoveryV2Matches = discoveryV2Matches.filter((p) =>
    isOutcomeActionableForMarket(p.outcomeType, odds as { spreadHome: number | null; totalOver: number | null } | null, p.posteriorHitRate),
  );

  const actionableNonGenericV2Matches = actionableDiscoveryV2Matches.filter(
    (p) => !isGenericPlayerOutcome(p.outcomeType),
  );
  const actionableGenericV2Matches = actionableDiscoveryV2Matches.filter((p) =>
    isGenericPlayerOutcome(p.outcomeType),
  );

  const curatedDiscoveryV2Matches =
    actionableNonGenericV2Matches.length > 0
      ? [...actionableNonGenericV2Matches, ...actionableGenericV2Matches.slice(0, 1)].slice(0, 12)
      : actionableGenericV2Matches.slice(0, 2);

  const enrichedDiscoveryV2Matches: DiscoveryMatch[] = curatedDiscoveryV2Matches.map((p) => ({
    ...p,
    playerTarget: pickV2PlayerTarget(p.outcomeType, gamePlayerCtx, propsForGame),
  }));

  const rankedSuggestedPlays = allRankedPlays.filter((p) =>
    isOutcomeActionableForMarket(p.outcomeType, odds as { spreadHome: number | null; totalOver: number | null } | null, p.posteriorHitRate),
  );

  const excludeFamilies = overrideExcludeFamilies ?? PREDICTION_TUNING.excludeFamilies ?? [];
  const excludedFamilies = new Set(excludeFamilies);
  const familyFilteredPlays =
    excludedFamilies.size > 0
      ? rankedSuggestedPlays.filter((p) => !excludedFamilies.has(betFamilyForOutcome(p.outcomeType)))
      : rankedSuggestedPlays;

  // Build the baseline (legacy) pick universe first. Strict mode should be subtractive-only.
  const legacyQualitySuggestedPlays = familyFilteredPlays.filter((p) => {
    const family = betFamilyForOutcome(p.outcomeType);
    const strictCfg = PICK_QUALITY_TUNING.strictGateByFamily[family];
    const evalResult = evaluateSuggestedPlayQualityGate({
      ...p,
      requireMarketLine: false,
      strictGateEnabled: false,
      calibratedWinProbability: p.calibratedWinProbability,
      impliedMarketProbability: p.impliedMarketProbability,
      edgeVsMarket: p.adjustedEdgeScore ?? p.edgeVsMarket,
      uncertaintyScore: p.uncertaintyScore,
      strictMinCalibratedEdge: strictCfg.minCalibratedEdge,
      strictMaxUncertaintyScore: strictCfg.maxUncertaintyScore,
    });
    return evalResult.pass;
  });

  const legacyBettableSuggestedPlays = legacyQualitySuggestedPlays.filter((p) => {
    const family = betFamilyForOutcome(p.outcomeType);
    const strictCfg = PICK_QUALITY_TUNING.strictGateByFamily[family];
    const evalResult = evaluateSuggestedPlayQualityGate({
      ...p,
      requireMarketLine: true,
      strictGateEnabled: false,
      calibratedWinProbability: p.calibratedWinProbability,
      impliedMarketProbability: p.impliedMarketProbability,
      edgeVsMarket: p.adjustedEdgeScore ?? p.edgeVsMarket,
      uncertaintyScore: p.uncertaintyScore,
      strictMinCalibratedEdge: strictCfg.minCalibratedEdge,
      strictMaxUncertaintyScore: strictCfg.maxUncertaintyScore,
    });
    return evalResult.pass;
  });

  const legacySuggestedBetPicks =
    legacyBettableSuggestedPlays.length > 0
      ? selectDiversifiedBetPicks(legacyBettableSuggestedPlays, maxBetPicksPerGame)
      : [];

  const suggestedBetPicks = strictActionabilityGatesEnabled
    ? legacySuggestedBetPicks.filter((p) => {
        if (p.laneTag === "other_prop") {
          if (gateDiagnostics) {
            recordGateEval(gateDiagnostics.bettable, {
              pass: false,
              reason: "strict_other_prop_block",
            });
          }
          return false;
        }
        if (p.marketType === "player_prop" && !p.playerTarget) {
          if (gateDiagnostics) {
            recordGateEval(gateDiagnostics.bettable, {
              pass: false,
              reason: "strict_missing_player_target",
            });
          }
          return false;
        }
        if (p.marketType === "player_prop" && isGenericPlayerOutcome(p.outcomeType)) {
          if (gateDiagnostics) {
            recordGateEval(gateDiagnostics.bettable, {
              pass: false,
              reason: "strict_generic_player_outcome",
            });
          }
          return false;
        }
        const family = betFamilyForOutcome(p.outcomeType);
        const strictCfg = PICK_QUALITY_TUNING.strictGateByFamily[family];
        const moneylineStrict = strictCfg as {
          minMoneylineImpliedProbability?: number;
          minMoneylineLongshotEdgeOverride?: number;
        };
        const evalResult = evaluateSuggestedPlayQualityGate({
          ...p,
          requireMarketLine: true,
          strictGateEnabled: true,
          calibratedWinProbability: p.calibratedWinProbability,
          impliedMarketProbability: p.impliedMarketProbability,
          edgeVsMarket: p.adjustedEdgeScore ?? p.edgeVsMarket,
          uncertaintyScore: p.uncertaintyScore,
          strictMinCalibratedEdge: strictCfg.minCalibratedEdge,
          strictMaxUncertaintyScore: strictCfg.maxUncertaintyScore,
          strictMinMoneylineImpliedProbability: moneylineStrict.minMoneylineImpliedProbability ?? null,
          strictMinMoneylineLongshotEdgeOverride: moneylineStrict.minMoneylineLongshotEdgeOverride ?? null,
        });
        if (gateDiagnostics) recordGateEval(gateDiagnostics.bettable, evalResult);
        return evalResult.pass;
      })
    : legacySuggestedBetPicks;

  const suggestedPlays =
    includeDebugPlays && legacyQualitySuggestedPlays.length > 0
      ? selectDiversifiedBetPicks(legacyQualitySuggestedPlays, Math.max(3, maxBetPicksPerGame))
      : [];

  const canonicalPredictions: PredictionRecord[] = allRankedPlays
    .filter((p) => p.modelProbability >= 0.52)
    .slice(0, 20)
    .map((p) => {
      const market = inferPredictionMarket(p.outcomeType, p.marketPick);
      const modelVotes = buildModelVotes({
        outcomeType: p.outcomeType,
        playerTarget: p.playerTarget,
        marketPick: p.marketPick,
        gameContext,
        gamePlayerCtx,
        confidence: p.confidence,
        posteriorHitRate: p.posteriorHitRate,
        modelProbability: p.modelProbability,
        metaScore: p.metaScore,
      });
      const aggregation = dynamicVoteWeightingEnabled
        ? aggregateVotesDynamic({
            votes: modelVotes,
            laneTag: p.laneTag,
            regimeTags: p.regimeTags,
            uncertaintyScore: p.uncertaintyScore,
            sourceReliabilityMap: voteReliabilityMap,
            voteWeightingStrength,
          })
        : aggregateVotes(modelVotes);
      if (!aggregation.pass) return null;
      const sourceFamilies = modelVotes.map((v) => sourceFamilyFromModelId(v.model_id));
      const sourceReliabilityScore = computeSourceReliabilityScore({
        sourceFamilies,
        marketFamily: marketFamilyFromBetFamily(betFamilyForOutcome(p.outcomeType)),
        snapshot: sourceReliabilitySnapshot,
        minSample: PICK_QUALITY_TUNING.sourceReliabilityMinSample,
      });
      return {
        prediction_id: createPredictionId({
          gameId: gameContext.gameId,
          market,
          selection: p.outcomeType,
          predictionContractVersion: PREDICTION_CONTRACT_VERSION,
          modelBundleVersion: modelBundleVersion ?? DEFAULT_MODEL_BUNDLE_VERSION,
          rankingPolicyVersion: RANKING_POLICY_VERSION,
          aggregationPolicyVersion: AGGREGATION_POLICY_VERSION,
          featureSnapshotId,
          voteWeightingVersion,
        }),
        game_id: gameContext.gameId,
        market,
        selection: p.outcomeType,
        model_votes: modelVotes,
        confidence_score: aggregation.confidence,
        edge_estimate: p.marketPick?.edge ?? p.edge,
        supporting_patterns: p.supportingPatterns,
        prediction_contract_version: PREDICTION_CONTRACT_VERSION,
        ranking_policy_version: RANKING_POLICY_VERSION,
        aggregation_policy_version: AGGREGATION_POLICY_VERSION,
        model_bundle_version: modelBundleVersion ?? DEFAULT_MODEL_BUNDLE_VERSION,
        feature_schema_version: FEATURE_SCHEMA_VERSION,
        feature_snapshot_id: featureSnapshotId,
        feature_snapshot_payload: featureSnapshotPayload,
        vote_weighting_version: voteWeightingVersion,
        quality_context: {
          raw_win_probability: p.rawWinProbability,
          calibrated_win_probability: p.calibratedWinProbability,
          implied_market_probability: p.impliedMarketProbability,
          edge_vs_market: p.edgeVsMarket,
          expected_value_score: p.expectedValueScore,
          market_type: p.marketType,
          market_sub_type: p.marketSubType,
          selection_side: p.selectionSide,
          line_snapshot: p.lineSnapshot,
          price_snapshot: p.priceSnapshot,
          lane_tag: p.laneTag,
          regime_tags: p.regimeTags,
          source_reliability_score: sourceReliabilityScore,
          uncertainty_score: p.uncertaintyScore,
          uncertainty_penalty_applied: p.uncertaintyPenaltyApplied,
          adjusted_edge_score: p.adjustedEdgeScore,
          weighted_support_score: aggregation.weightedSupportScore,
          weighted_opposition_score: aggregation.weightedOppositionScore,
          weighted_consensus_score: aggregation.weightedConsensusScore,
          weighted_disagreement_penalty: aggregation.weightedDisagreementPenalty,
          vote_weight_breakdown: aggregation.voteWeightBreakdown?.map((row) => ({
            model_id: row.modelId,
            source_family: row.sourceFamily,
            decision: row.decision,
            confidence: row.confidence,
            base_weight: row.baseWeight,
            reliability_weight: row.reliabilityWeight,
            sample_confidence_weight: row.sampleConfidenceWeight,
            uncertainty_discount: row.uncertaintyDiscount,
            lane_fit_weight: row.laneFitWeight,
            pre_strength_vote_weight: row.preStrengthVoteWeight,
            vote_weighting_strength: row.voteWeightingStrength,
            final_vote_weight: row.finalVoteWeight,
            weighted_contribution: row.weightedContribution,
          })),
          dominant_source_family: aggregation.dominantSourceFamily ?? null,
        },
        generated_at: generatedAt,
      };
    })
    .filter((record): record is PredictionRecord => record != null);

  return {
    discoveryV2Matches: enrichedDiscoveryV2Matches,
    suggestedPlays,
    suggestedBetPicks,
    modelPicks,
    canonicalPredictions,
    featureSnapshotId,
    rejectedPatternDiagnostics,
    ...(gateDiagnostics ? { gateDiagnostics } : {}),
  };
}
