import { PREDICTION_TUNING } from "../config/tuning";
import { betFamilyForOutcome, type BetFamily } from "../patterns/metaModelCore";

export type SuggestedMarketPickLike = {
  overPrice: number;
  edge: number;
  ev: number;
};

export type GateRejectReason =
  | "posterior_below_min"
  | "meta_below_min"
  | "player_target_stat_mismatch"
  | "player_target_baseline_too_low"
  | "no_market_pick"
  | "ev_below_min"
  | "ev_too_negative"
  | "edge_too_low"
  | "odds_too_negative"
  | "calibrated_edge_below_min"
  | "uncertainty_above_max"
  | "missing_market_snapshot"
  | "moneyline_longshot_blocked";

function parseTotalThresholdOutcome(
  outcomeType: string,
): { direction: "over" | "under"; line: number } | null {
  const base = outcomeType.replace(/:.*$/, "");
  const over = base.match(/^TOTAL_OVER_(\d+(?:\.\d+)?)$/);
  if (over) return { direction: "over", line: Number(over[1]) };
  const under = base.match(/^TOTAL_UNDER_(\d+(?:\.\d+)?)$/);
  if (under) return { direction: "under", line: Number(under[1]) };
  return null;
}

function parsePlayerThresholdOutcome(
  outcomeType: string,
): { stat: "ppg" | "rpg" | "apg"; line: number } | null {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base === "PLAYER_10_PLUS_ASSISTS" ||
    base === "HOME_TOP_ASSIST_10_PLUS" ||
    base === "AWAY_TOP_ASSIST_10_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_10_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_10_PLUS"
  ) return { stat: "apg", line: 10 };
  if (
    base === "HOME_TOP_ASSIST_8_PLUS" ||
    base === "AWAY_TOP_ASSIST_8_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_8_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_8_PLUS"
  ) return { stat: "apg", line: 8 };
  if (
    base === "PLAYER_10_PLUS_REBOUNDS" ||
    base === "HOME_TOP_REBOUNDER_10_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_10_PLUS"
  ) return { stat: "rpg", line: 10 };
  if (base === "HOME_TOP_REBOUNDER_12_PLUS" || base === "AWAY_TOP_REBOUNDER_12_PLUS") {
    return { stat: "rpg", line: 12 };
  }
  if (
    base === "PLAYER_30_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS"
  ) return { stat: "ppg", line: 30 };
  if (base === "PLAYER_40_PLUS") return { stat: "ppg", line: 40 };
  if (base === "HOME_TOP_SCORER_25_PLUS" || base === "AWAY_TOP_SCORER_25_PLUS") {
    return { stat: "ppg", line: 25 };
  }
  return null;
}

export function isOutcomeActionableForMarket(
  outcomeType: string,
  odds?: { spreadHome: number | null; totalOver: number | null } | null,
  posteriorHitRate?: number,
): boolean {
  const totalThreshold = parseTotalThresholdOutcome(outcomeType);
  if (!totalThreshold) return true;
  const marketTotal = odds?.totalOver;
  if (marketTotal == null) return false;
  const delta = totalThreshold.line - marketTotal;
  const withinBand = totalThreshold.direction === "over" ? delta <= 8 : delta >= -8;
  if (!withinBand) return false;
  if (posteriorHitRate != null && posteriorHitRate < 0.5) return false;
  return true;
}

export function impliedProbFromAmerican(american: number | null | undefined): number | null {
  if (american == null) return null;
  const o = Number(american);
  if (!Number.isFinite(o) || o === 0) return null;
  return o < 0 ? -o / (-o + 100) : 100 / (o + 100);
}

export function payoutFromAmerican(american: number): number {
  return american > 0 ? american / 100 : 100 / Math.abs(american);
}

export function outcomeDedupFamily(outcomeType: string): string {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base === "PLAYER_10_PLUS_ASSISTS" ||
    base === "HOME_TOP_ASSIST_8_PLUS" ||
    base === "HOME_TOP_ASSIST_10_PLUS" ||
    base === "HOME_TOP_ASSIST_EXCEEDS_AVG" ||
    base === "AWAY_TOP_ASSIST_8_PLUS" ||
    base === "AWAY_TOP_ASSIST_10_PLUS" ||
    base === "AWAY_TOP_ASSIST_EXCEEDS_AVG" ||
    base === "HOME_TOP_PLAYMAKER_8_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_10_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_8_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_10_PLUS"
  ) return "ASSISTS_LADDER";
  if (
    base === "PLAYER_10_PLUS_REBOUNDS" ||
    base === "HOME_TOP_REBOUNDER_10_PLUS" ||
    base === "HOME_TOP_REBOUNDER_12_PLUS" ||
    base === "HOME_TOP_REBOUNDER_EXCEEDS_AVG" ||
    base === "AWAY_TOP_REBOUNDER_10_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_12_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_EXCEEDS_AVG"
  ) return "REBOUNDS_LADDER";
  if (
    base === "PLAYER_30_PLUS" ||
    base === "PLAYER_40_PLUS" ||
    base === "HOME_TOP_SCORER_25_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "HOME_TOP_SCORER_EXCEEDS_AVG" ||
    base === "AWAY_TOP_SCORER_25_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_EXCEEDS_AVG"
  ) return "POINTS_LADDER";
  return base;
}

export { betFamilyForOutcome, type BetFamily } from "../patterns/metaModelCore";

export function gateThresholdsForFamily(family: BetFamily): {
  minPosterior: number;
  minMeta: number;
  minEv: number;
} {
  if (family === "PLAYER") return PREDICTION_TUNING.familyGates.PLAYER;
  if (family === "TOTAL") return PREDICTION_TUNING.familyGates.TOTAL;
  if (family === "SPREAD") return PREDICTION_TUNING.familyGates.SPREAD;
  if (family === "MONEYLINE") return PREDICTION_TUNING.familyGates.MONEYLINE;
  return PREDICTION_TUNING.familyGates.OTHER;
}

export function evaluateSuggestedPlayQualityGate(play: {
  outcomeType: string;
  posteriorHitRate: number;
  metaScore: number | null;
  playerTarget?: { stat: "ppg" | "rpg" | "apg"; statValue: number } | null;
  marketPick?: SuggestedMarketPickLike | null;
  requireMarketLine?: boolean;
  strictGateEnabled?: boolean;
  calibratedWinProbability?: number | null;
  impliedMarketProbability?: number | null;
  edgeVsMarket?: number | null;
  uncertaintyScore?: number | null;
  strictMinCalibratedEdge?: number | null;
  strictMaxUncertaintyScore?: number | null;
  strictMinMoneylineImpliedProbability?: number | null;
  strictMinMoneylineLongshotEdgeOverride?: number | null;
}): { pass: boolean; reason?: GateRejectReason } {
  const family = betFamilyForOutcome(play.outcomeType);
  const gates = gateThresholdsForFamily(family);
  if (play.posteriorHitRate < gates.minPosterior) return { pass: false, reason: "posterior_below_min" };
  if (play.metaScore != null && play.metaScore < gates.minMeta) return { pass: false, reason: "meta_below_min" };

  const threshold = parsePlayerThresholdOutcome(play.outcomeType);
  if (threshold && play.playerTarget) {
    if (play.playerTarget.stat !== threshold.stat) return { pass: false, reason: "player_target_stat_mismatch" };
    const minBaseline = threshold.line * 0.65;
    if (play.playerTarget.statValue < minBaseline) return { pass: false, reason: "player_target_baseline_too_low" };
  }

  const requireMarketLine = play.requireMarketLine ?? true;
  if (requireMarketLine) {
    if (!play.marketPick) return { pass: false, reason: "no_market_pick" };
    if (play.marketPick.ev < gates.minEv) return { pass: false, reason: "ev_below_min" };
    if (play.marketPick.edge < -0.03) return { pass: false, reason: "edge_too_low" };
    if (play.marketPick.overPrice < PREDICTION_TUNING.maxNegativeAmericanOdds) {
      return { pass: false, reason: "odds_too_negative" };
    }
  } else if (play.marketPick) {
    if (play.marketPick.ev < -0.01) return { pass: false, reason: "ev_too_negative" };
    if (play.marketPick.overPrice < PREDICTION_TUNING.maxNegativeAmericanOdds) {
      return { pass: false, reason: "odds_too_negative" };
    }
  }

  if (play.strictGateEnabled) {
    if (play.impliedMarketProbability == null && requireMarketLine) {
      return { pass: false, reason: "missing_market_snapshot" };
    }
    if (play.strictMinCalibratedEdge != null && play.edgeVsMarket != null) {
      if (play.edgeVsMarket < play.strictMinCalibratedEdge) {
        return { pass: false, reason: "calibrated_edge_below_min" };
      }
    }
    if (play.strictMaxUncertaintyScore != null && play.uncertaintyScore != null) {
      if (play.uncertaintyScore > play.strictMaxUncertaintyScore) {
        return { pass: false, reason: "uncertainty_above_max" };
      }
    }
    if (
      family === "MONEYLINE" &&
      play.strictMinMoneylineImpliedProbability != null &&
      play.impliedMarketProbability != null &&
      play.impliedMarketProbability < play.strictMinMoneylineImpliedProbability
    ) {
      const edge = play.edgeVsMarket ?? Number.NEGATIVE_INFINITY;
      const override = play.strictMinMoneylineLongshotEdgeOverride;
      if (override == null || edge < override) {
        return { pass: false, reason: "moneyline_longshot_blocked" };
      }
    }
  }
  return { pass: true };
}

export function selectDiversifiedBetPicks<
  T extends {
    outcomeType: string;
    marketPick?: SuggestedMarketPickLike | null;
    metaScore?: number | null;
    posteriorHitRate: number;
    confidence: number;
  },
>(plays: T[], maxPicks: number): T[] {
  const selected: T[] = [];
  const usedFamilies = new Set<string>();
  const usedDedupFamilies = new Set<string>();
  const ranked = [...plays].sort(
    (a, b) =>
      (b.marketPick?.ev ?? -999) - (a.marketPick?.ev ?? -999) ||
      (b.metaScore ?? -1) - (a.metaScore ?? -1) ||
      b.posteriorHitRate - a.posteriorHitRate ||
      b.confidence - a.confidence,
  );
  for (const p of ranked) {
    const family = betFamilyForOutcome(p.outcomeType);
    const dedupFamily = outcomeDedupFamily(p.outcomeType);
    if (usedFamilies.has(family) || usedDedupFamilies.has(dedupFamily)) continue;
    selected.push(p);
    usedFamilies.add(family);
    usedDedupFamilies.add(dedupFamily);
    if (selected.length >= maxPicks) break;
  }
  return selected;
}

