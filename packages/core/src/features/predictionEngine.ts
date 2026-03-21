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
import { PREDICTION_TUNING } from "../config/tuning";
import type { GameContext } from "@prisma/client";
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
  return base === "PLAYER_DOUBLE_DOUBLE" || base === "PLAYER_10_PLUS_REBOUNDS";
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
): { market: string; requiredActual: "points" | "rebounds" | "assists" | "fg3m"; requiredLine: number } | null {
  const req = parsePlayerOutcomeRequirement(outcomeType);
  if (!req) return null;
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

function labelForMarketPick(
  playerName: string,
  market: string,
  line: number,
  overPrice: number,
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
      return {
        id: prop.id,
        name: prop.name,
        stat: "rpg",
        statValue: gamePlayerCtx.homeTopRebounder?.name === prop.name
          ? gamePlayerCtx.homeTopRebounder.stat
          : gamePlayerCtx.awayTopRebounder?.name === prop.name
            ? gamePlayerCtx.awayTopRebounder.stat
            : Math.max(gamePlayerCtx.homeTopRebounder?.stat ?? 0, gamePlayerCtx.awayTopRebounder?.stat ?? 0),
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
      return {
        id: prop.id,
        name: prop.name,
        stat: "apg",
        statValue: Math.max(gamePlayerCtx.homeTopPlaymaker?.stat ?? 0, gamePlayerCtx.awayTopPlaymaker?.stat ?? 0),
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
  if (outcome === "AWAY_TOP_SCORER_25_PLUS" || outcome === "AWAY_TOP_SCORER_30_PLUS") {
    const p = gamePlayerCtx.awayTopScorer;
    return p ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Away top scorer by pregame context" } : null;
  }
  if (outcome === "HOME_TOP_REBOUNDER_10_PLUS" || outcome === "HOME_TOP_REBOUNDER_12_PLUS") {
    const p = gamePlayerCtx.homeTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Home top rebounder by pregame context" } : null;
  }
  if (outcome === "AWAY_TOP_REBOUNDER_10_PLUS" || outcome === "AWAY_TOP_REBOUNDER_12_PLUS") {
    const p = gamePlayerCtx.awayTopRebounder;
    return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Away top rebounder by pregame context" } : null;
  }
  if (outcome === "PLAYER_30_PLUS" || outcome === "PLAYER_40_PLUS" || outcome === "PLAYER_5_PLUS_THREES") {
    if (outcome === "PLAYER_30_PLUS") {
      const prop = pickFromProps(propsForGame, "player_points", 29.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
        return { id: prop.id, name: prop.name, stat: "ppg", statValue: Math.max(gamePlayerCtx.homeTopScorer?.stat ?? 0, gamePlayerCtx.awayTopScorer?.stat ?? 0), rationale: `Best prop over implied (${pct}) for 30+ points (${lineStr})` };
      }
    }
    if (outcome === "PLAYER_40_PLUS") {
      const prop = pickFromProps(propsForGame, "player_points", 39.5);
      if (prop) {
        const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
        const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
        return { id: prop.id, name: prop.name, stat: "ppg", statValue: Math.max(gamePlayerCtx.homeTopScorer?.stat ?? 0, gamePlayerCtx.awayTopScorer?.stat ?? 0), rationale: `Best prop over implied (${pct}) for 40+ points (${lineStr})` };
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
}): SuggestedMarketPick | null {
  const { outcomeType, target, propsForGame, baseProb } = args;
  const confidence = Math.max(0, Math.min(1, args.confidence ?? 0));
  const support = Math.max(1, args.supportN ?? 1);
  const baseOutcome = outcomeType.replace(/:.*$/, "");

  if (baseOutcome === "HOME_WIN" || baseOutcome === "AWAY_WIN") {
    const consensusPrice = baseOutcome === "HOME_WIN" ? args.gameOdds?.mlHome : args.gameOdds?.mlAway;
    const fallbackPrice =
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
    };
  }

  if (
    baseOutcome === "HOME_COVERED" || baseOutcome === "AWAY_COVERED" ||
    baseOutcome === "FAVORITE_COVERED" || baseOutcome === "UNDERDOG_COVERED"
  ) {
    const spread = args.gameOdds?.spreadHome;
    if (spread == null || !Number.isFinite(spread)) return null;
    const price = Number.isFinite(args.defaultMarketPrice ?? NaN) && (args.defaultMarketPrice ?? 0) !== 0
      ? Number(args.defaultMarketPrice)
      : -110;
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
    };
  }

  if (
    baseOutcome === "OVER_HIT" || baseOutcome === "UNDER_HIT" ||
    baseOutcome.startsWith("TOTAL_OVER_") || baseOutcome.startsWith("TOTAL_UNDER_")
  ) {
    const total = args.gameOdds?.totalOver;
    if (total == null || !Number.isFinite(total)) return null;
    const price = Number.isFinite(args.defaultMarketPrice ?? NaN) && (args.defaultMarketPrice ?? 0) !== 0
      ? Number(args.defaultMarketPrice)
      : -110;
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
    };
  }

  if (!target) return null;
  const spec = marketSpecForOutcome(outcomeType);
  if (!spec) return null;

  const candidates = propsForGame.filter(
    (r) =>
      r.playerId === target.id &&
      r.market === spec.market &&
      r.line != null &&
      r.overPrice != null,
  );
  if (candidates.length === 0) return null;

  const slope =
    spec.requiredActual === "fg3m"
      ? 0.09
      : spec.requiredActual === "assists" || spec.requiredActual === "rebounds"
        ? 0.07
        : 0.04;
  let best: SuggestedMarketPick | null = null;
  let bestEv = -Infinity;

  for (const c of candidates) {
    const overPrice = c.overPrice ?? 0;
    if (overPrice < PREDICTION_TUNING.maxNegativeAmericanOdds) continue;
    const implied = impliedProbFromAmerican(c.overPrice);
    if (implied == null) continue;
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
    const est = Math.max(
      0.05,
      Math.min(0.95, modelWeight * estModel + (1 - modelWeight) * implied),
    );
    const ev = est * payoutFromAmerican(overPrice) - (1 - est);
    if (ev > bestEv) {
      bestEv = ev;
      best = {
        playerId: c.playerId,
        playerName: `${c.player.firstname} ${c.player.lastname}`.trim(),
        market: c.market,
        line: c.line ?? 0,
        overPrice: c.overPrice ?? 0,
        impliedProb: implied,
        estimatedProb: est,
        modelEstimatedProb: estModel,
        edge: est - implied,
        ev,
        label: labelForMarketPick(
          `${c.player.firstname} ${c.player.lastname}`.trim(),
          c.market,
          c.line ?? 0,
          c.overPrice ?? 0,
        ),
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

function buildModelVotes(args: {
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
  };
  const votes: ModelVote[] = REQUIRED_MODEL_VOTE_IDS.map((id) => voteById[id]);
  return votes;
}

function aggregateVotes(votes: ModelVote[]): {
  pass: boolean;
  confidence: number;
  reason: string | null;
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
    includeDebugPlays,
    overrideExcludeFamilies,
    predictionGeneratedAtIso,
    modelBundleVersion,
    sourceTimestamps,
  } = input;

  const gateDiagnostics = includeDebugPlays ? newGateDiagnostics() : undefined;

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

  const qualitySuggestedPlays = familyFilteredPlays.filter((p) => {
    const evalResult = evaluateSuggestedPlayQualityGate({
      ...p,
      requireMarketLine: false,
    });
    if (gateDiagnostics) recordGateEval(gateDiagnostics.quality, evalResult);
    return evalResult.pass;
  });

  const bettableSuggestedPlays = qualitySuggestedPlays.filter((p) => {
    const evalResult = evaluateSuggestedPlayQualityGate({
      ...p,
      requireMarketLine: true,
    });
    if (gateDiagnostics) recordGateEval(gateDiagnostics.bettable, evalResult);
    return evalResult.pass;
  });

  const suggestedBetPicks =
    bettableSuggestedPlays.length > 0
      ? selectDiversifiedBetPicks(bettableSuggestedPlays, maxBetPicksPerGame)
      : [];

  const suggestedPlays =
    includeDebugPlays && qualitySuggestedPlays.length > 0
      ? selectDiversifiedBetPicks(qualitySuggestedPlays, Math.max(3, maxBetPicksPerGame))
      : [];

  const canonicalPredictions: PredictionRecord[] = allRankedPlays
    .filter((p) => p.modelProbability >= 0.52)
    .slice(0, 20)
    .map((p) => {
      const market = inferPredictionMarket(p.outcomeType, p.marketPick);
      const modelVotes = buildModelVotes({
        confidence: p.confidence,
        posteriorHitRate: p.posteriorHitRate,
        modelProbability: p.modelProbability,
        metaScore: p.metaScore,
      });
      const aggregation = aggregateVotes(modelVotes);
      if (!aggregation.pass) return null;
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
