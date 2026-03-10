import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { getEasternDateFromUtc } from "@/lib/format";
import { GAME_EVENT_CATALOG } from "../../../../../src/features/gameEventCatalog";
import type { GameEventContext } from "../../../../../src/features/gameEventCatalog";
import { loadMetaModel, scoreMetaModel } from "../../../../../src/patterns/metaModelCore";

export const dynamic = "force-dynamic";

let suggestedPlayLedgerAvailableCache: boolean | null = null;
type WagerSummary = {
  bets: number;
  settledBets: number;
  pendingBets: number;
  wins: number;
  losses: number;
  totalStaked: number;
  settledStaked: number;
  netPnl: number;
  roi: number | null;
};

interface TeamSnapshot {
  wins: number;
  losses: number;
  ppg: number;
  oppg: number;
  pace: number | null;
  rebPg: number | null;
  astPg: number | null;
  fg3Pct: number | null;
  ftPct: number | null;
  rankOff: number | null;
  rankDef: number | null;
  rankPace: number | null;
  streak: number;
  lastGameDate: Date | null;
}

type DiscoveryV2Pattern = {
  id: string;
  outcomeType: string;
  conditions: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
};

type OutcomeEval = {
  hit: boolean;
  explanation: string | null;
  scope: "target" | "outcome";
};

type V2PlayerTarget = {
  id: number;
  name: string;
  stat: "ppg" | "rpg" | "apg";
  statValue: number;
  rationale: string;
};

type SuggestedMarketPick = {
  playerId: number;
  playerName: string;
  market: string;
  line: number;
  overPrice: number;
  impliedProb: number;
  estimatedProb: number;
  edge: number;
  ev: number;
  label: string;
};

type PlayerPropRow = {
  gameId: string;
  playerId: number;
  market: string;
  line: number | null;
  overPrice: number | null;
  underPrice: number | null;
  player: { firstname: string; lastname: string };
};

const OUTCOME_EVENT_DEFS = GAME_EVENT_CATALOG.filter((d) => d.type === "outcome");

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function hasCompletedScore(game: {
  homeScore: number;
  awayScore: number;
  status: string | null;
}): boolean {
  const hasAnyScore = (game.homeScore ?? 0) > 0 || (game.awayScore ?? 0) > 0;
  return !!game.status?.includes("Final") && hasAnyScore;
}

/** Look up outcome in gameOutcomeMap. Keys are stored as eventKey:side (e.g. AWAY_WIN:game). */
function resolveOutcomeResult(
  gameOutcomeMap: Map<string, { hit: true; meta: unknown }>,
  outcome: string,
): { hit: true; meta: unknown } | undefined {
  if (gameOutcomeMap.has(outcome)) return gameOutcomeMap.get(outcome);
  const baseOutcome = outcome.replace(/:.*$/, "");
  for (const side of ["game", "home", "away"]) {
    const key = `${baseOutcome}:${side}`;
    if (gameOutcomeMap.has(key)) return gameOutcomeMap.get(key);
  }
  return undefined;
}

function buildOutcomeExplanation(
  meta: unknown,
  playerMap: Map<number, string>,
): string | null {
  if (!meta || typeof meta !== "object") return null;
  const obj = meta as Record<string, unknown>;
  const parts: string[] = [];
  if (obj.playerId && typeof obj.playerId === "number") {
    parts.push(playerMap.get(obj.playerId) ?? `Player ${obj.playerId}`);
  }
  if (obj.points !== undefined) parts.push(`${obj.points} pts`);
  if (obj.rebounds !== undefined) parts.push(`${obj.rebounds} reb`);
  if (obj.assists !== undefined) parts.push(`${obj.assists} ast`);
  if (obj.fg3m !== undefined) parts.push(`${obj.fg3m} 3PM`);
  if (obj.actual !== undefined && obj.line !== undefined) {
    parts.push(`${obj.actual} total (line: ${obj.line})`);
  }
  if (typeof obj.margin === "number" && typeof obj.spread === "number") {
    parts.push(
      `margin ${obj.margin > 0 ? "+" : ""}${obj.margin} (spread: ${obj.spread > 0 ? "+" : ""}${obj.spread})`,
    );
  }
  return parts.length > 0 ? parts.join(", ") : null;
}

function computeOutcomeFromFinalScore(
  outcomeKey: string,
  game: { homeScore: number; awayScore: number; status: string | null },
  odds?: { spreadHome: number | null; totalOver: number | null } | null,
): { hit: boolean; explanation: string } | null {
  const base = outcomeKey.replace(/:.*$/, "");
  const isFinal = hasCompletedScore(game);
  if (!isFinal) return null;

  const home = game.homeScore ?? 0;
  const away = game.awayScore ?? 0;
  const total = home + away;
  const marginHome = home - away; // positive => home won by X
  const absMargin = Math.abs(marginHome);

  // Moneyline
  if (base === "HOME_WIN") {
    const hit = home > away;
    return { hit, explanation: `Final score ${away}-${home}` };
  }
  if (base === "AWAY_WIN") {
    const hit = away > home;
    return { hit, explanation: `Final score ${away}-${home}` };
  }

  // Threshold totals (e.g., TOTAL_OVER_220)
  const mOver = base.match(/^TOTAL_OVER_(\d+(?:\.\d+)?)$/);
  if (mOver) {
    const line = Number(mOver[1]);
    const hit = total > line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }
  const mUnder = base.match(/^TOTAL_UNDER_(\d+(?:\.\d+)?)$/);
  if (mUnder) {
    const line = Number(mUnder[1]);
    const hit = total < line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }

  // Totals from market line (requires odds)
  if (base === "OVER_HIT" && odds?.totalOver != null) {
    const line = odds.totalOver;
    const hit = total > line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }
  if (base === "UNDER_HIT" && odds?.totalOver != null) {
    const line = odds.totalOver;
    const hit = total < line;
    return { hit, explanation: `Final total ${total} (line ${line})` };
  }

  // Spread outcomes (requires spreadHome; spreadHome applies to home team)
  if (odds?.spreadHome != null) {
    const spreadHome = odds.spreadHome;
    const homeCovers = home + spreadHome > away;
    const awayCovers = away > home + spreadHome;
    const push = !homeCovers && !awayCovers;

    if (base === "HOME_COVERED") {
      return { hit: homeCovers, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "AWAY_COVERED") {
      return { hit: awayCovers, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "FAVORITE_COVERED") {
      const homeFav = spreadHome < 0;
      const hit = homeFav ? homeCovers : awayCovers;
      return { hit, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
    if (base === "UNDERDOG_COVERED") {
      const homeDog = spreadHome > 0;
      const hit = homeDog ? homeCovers : awayCovers;
      return { hit, explanation: `Final ${away}-${home} (spreadHome ${spreadHome}${push ? ", push" : ""})` };
    }
  }

  // Margin outcomes (no odds required)
  if (base === "MARGIN_UNDER_5") return { hit: absMargin < 5, explanation: `Final margin ${absMargin}` };
  if (base === "MARGIN_UNDER_10") return { hit: absMargin < 10, explanation: `Final margin ${absMargin}` };
  if (base === "BLOWOUT_20_PLUS") return { hit: absMargin >= 20, explanation: `Final margin ${absMargin}` };

  return null;
}

function computeOutcomeFromCatalog(
  outcomeKey: string,
  ctx: GameEventContext,
): { hit: boolean; meta: unknown } | null {
  const base = outcomeKey.replace(/:.*$/, "");
  const sideMatch = outcomeKey.match(/:([^:]+)$/);
  const preferredSide = sideMatch?.[1];
  const defs = OUTCOME_EVENT_DEFS.filter((d) => d.key === base);
  if (defs.length === 0) return null;
  for (const def of defs) {
    const sides = preferredSide && def.sides.includes(preferredSide as "home" | "away" | "game")
      ? [preferredSide as "home" | "away" | "game", ...def.sides.filter((s) => s !== preferredSide)]
      : def.sides;
    for (const side of sides) {
      const computed = def.compute(ctx, side);
      if (computed.hit) return { hit: true, meta: computed.meta ?? null };
    }
  }
  return { hit: false, meta: null };
}

function isLowSpecificityConditionToken(token: string): boolean {
  return (
    token.startsWith("home_rest_days:") ||
    token.startsWith("away_rest_days:") ||
    token.startsWith("season:") ||
    token === "home_is_b2b:true" ||
    token === "away_is_b2b:true"
  );
}

function isLowSpecificityPattern(conditions: string[]): boolean {
  if (conditions.length !== 1) return false;
  const c = conditions[0];
  if (!c || c.startsWith("!")) return false;
  return isLowSpecificityConditionToken(c);
}

function isGenericPlayerOutcome(outcomeType: string): boolean {
  const base = outcomeType.replace(/:.*$/, "");
  return (
    base === "PLAYER_DOUBLE_DOUBLE" ||
    base === "PLAYER_10_PLUS_REBOUNDS"
  );
}

function parseTotalThresholdOutcome(
  outcomeType: string,
): { direction: "over" | "under"; line: number } | null {
  const base = outcomeType.replace(/:.*$/, "");
  const over = base.match(/^TOTAL_OVER_(\d+(?:\.\d+)?)$/);
  if (over) {
    return { direction: "over", line: Number(over[1]) };
  }
  const under = base.match(/^TOTAL_UNDER_(\d+(?:\.\d+)?)$/);
  if (under) {
    return { direction: "under", line: Number(under[1]) };
  }
  return null;
}

function outcomeDedupFamily(outcomeType: string): string {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base === "PLAYER_10_PLUS_ASSISTS" ||
    base === "HOME_TOP_ASSIST_8_PLUS" ||
    base === "HOME_TOP_ASSIST_10_PLUS" ||
    base === "AWAY_TOP_ASSIST_8_PLUS" ||
    base === "AWAY_TOP_ASSIST_10_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_8_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_10_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_8_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_10_PLUS"
  ) {
    return "ASSISTS_LADDER";
  }
  if (
    base === "PLAYER_10_PLUS_REBOUNDS" ||
    base === "HOME_TOP_REBOUNDER_10_PLUS" ||
    base === "HOME_TOP_REBOUNDER_12_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_10_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_12_PLUS"
  ) {
    return "REBOUNDS_LADDER";
  }
  if (
    base === "PLAYER_30_PLUS" ||
    base === "PLAYER_40_PLUS" ||
    base === "HOME_TOP_SCORER_25_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_25_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS"
  ) {
    return "POINTS_LADDER";
  }
  return base;
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
  ) {
    return { stat: "apg", line: 10 };
  }
  if (
    base === "HOME_TOP_ASSIST_8_PLUS" ||
    base === "AWAY_TOP_ASSIST_8_PLUS" ||
    base === "HOME_TOP_PLAYMAKER_8_PLUS" ||
    base === "AWAY_TOP_PLAYMAKER_8_PLUS"
  ) {
    return { stat: "apg", line: 8 };
  }
  if (
    base === "PLAYER_10_PLUS_REBOUNDS" ||
    base === "HOME_TOP_REBOUNDER_10_PLUS" ||
    base === "AWAY_TOP_REBOUNDER_10_PLUS"
  ) {
    return { stat: "rpg", line: 10 };
  }
  if (base === "HOME_TOP_REBOUNDER_12_PLUS" || base === "AWAY_TOP_REBOUNDER_12_PLUS") {
    return { stat: "rpg", line: 12 };
  }
  if (
    base === "PLAYER_30_PLUS" ||
    base === "HOME_TOP_SCORER_30_PLUS" ||
    base === "AWAY_TOP_SCORER_30_PLUS"
  ) {
    return { stat: "ppg", line: 30 };
  }
  if (base === "PLAYER_40_PLUS") return { stat: "ppg", line: 40 };
  if (base === "HOME_TOP_SCORER_25_PLUS" || base === "AWAY_TOP_SCORER_25_PLUS") {
    return { stat: "ppg", line: 25 };
  }
  return null;
}

function parsePlayerOutcomeRequirement(
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

function impliedProbFromAmerican(american: number | null): number | null {
  if (american == null) return null;
  const o = Number(american);
  if (!Number.isFinite(o) || o === 0) return null;
  return o < 0 ? (-o) / (-o + 100) : 100 / (o + 100);
}

function payoutFromAmerican(american: number): number {
  return american > 0 ? american / 100 : 100 / Math.abs(american);
}

function ledgerDedupKey(play: {
  outcomeType: string;
  playerTarget: V2PlayerTarget | null;
  marketPick?: SuggestedMarketPick | null;
}): string {
  const targetId = play.playerTarget?.id ?? 0;
  const market = play.marketPick?.market ?? "none";
  const line =
    typeof play.marketPick?.line === "number"
      ? play.marketPick.line.toFixed(3)
      : "none";
  const price = play.marketPick?.overPrice ?? 0;
  return `${play.outcomeType}|${targetId}|${market}|${line}|${price}`;
}

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

function selectBestMarketBackedPick(args: {
  outcomeType: string;
  target: V2PlayerTarget | null;
  propsForGame: PlayerPropRow[];
  baseProb: number;
  confidence?: number;
  supportN?: number;
}): SuggestedMarketPick | null {
  const { outcomeType, target, propsForGame, baseProb } = args;
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
  const support = Math.max(1, args.supportN ?? 1);
  const confidence = Math.max(0, Math.min(1, args.confidence ?? 0));

  for (const c of candidates) {
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
    const blendWeightModel = Math.max(
      0.35,
      Math.min(0.85, 0.45 + Math.log(support) / 10 + confidence * 0.15),
    );
    const est = Math.max(
      0.05,
      Math.min(0.95, blendWeightModel * estModel + (1 - blendWeightModel) * implied),
    );
    const ev = est * payoutFromAmerican(c.overPrice) - (1 - est);
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

function buildTargetOutcomeExplanation(
  outcomeType: string,
  target: V2PlayerTarget | null,
  stats: { playerId: number; points: number; rebounds: number; assists: number; fg3m: number | null }[],
): string | null {
  if (!target) return null;
  const req = parsePlayerOutcomeRequirement(outcomeType);
  if (!req) return null;
  const row = stats.find((s) => s.playerId === target.id);
  if (!row) return `No box score found for ${target.name}.`;
  const rawActual =
    req.actualStat === "points" ? row.points :
    req.actualStat === "rebounds" ? row.rebounds :
    req.actualStat === "assists" ? row.assists :
    (row.fg3m ?? 0);
  const actual = Number.isFinite(rawActual) ? Number(rawActual) : 0;
  if (actual >= req.line) {
    return `${target.name}, ${actual} ${req.label}`;
  }
  return `${target.name}, ${actual} ${req.label} (needed ${req.line}+)`;
}

function isOutcomeActionableForMarket(
  outcomeType: string,
  odds?: { spreadHome: number | null; totalOver: number | null } | null,
  posteriorHitRate?: number,
): boolean {
  const totalThreshold = parseTotalThresholdOutcome(outcomeType);
  if (!totalThreshold) {
    return true;
  }

  // Threshold total outcomes should be reasonably close to current market total.
  const marketTotal = odds?.totalOver;
  if (marketTotal == null) return false;

  const delta = totalThreshold.line - marketTotal;
  const withinBand =
    totalThreshold.direction === "over"
      ? delta <= 8
      : delta >= -8;
  if (!withinBand) return false;

  // Extra guard against low absolute-hit "looks good vs baseline" outcomes.
  if (posteriorHitRate != null && posteriorHitRate < 0.5) {
    return false;
  }

  return true;
}

type BetFamily = "PLAYER" | "TOTAL" | "SPREAD" | "MONEYLINE" | "OTHER";

function betFamilyForOutcome(outcomeType: string): BetFamily {
  const base = outcomeType.replace(/:.*$/, "");
  if (
    base.startsWith("PLAYER_") ||
    base.startsWith("HOME_TOP_") ||
    base.startsWith("AWAY_TOP_")
  ) {
    return "PLAYER";
  }
  if (
    base.startsWith("TOTAL_") ||
    base === "OVER_HIT" ||
    base === "UNDER_HIT"
  ) {
    return "TOTAL";
  }
  if (base.includes("COVERED")) {
    return "SPREAD";
  }
  if (base === "HOME_WIN" || base === "AWAY_WIN") {
    return "MONEYLINE";
  }
  return "OTHER";
}

function gateThresholdsForFamily(family: BetFamily): {
  minPosterior: number;
  minMeta: number;
  minEv: number;
} {
  if (family === "PLAYER") return { minPosterior: 0.54, minMeta: 0.58, minEv: 0.02 };
  if (family === "TOTAL") return { minPosterior: 0.535, minMeta: 0.56, minEv: 0.018 };
  if (family === "SPREAD") return { minPosterior: 0.53, minMeta: 0.55, minEv: 0.015 };
  if (family === "MONEYLINE") return { minPosterior: 0.525, minMeta: 0.54, minEv: 0.012 };
  return { minPosterior: 0.55, minMeta: 0.58, minEv: 0.02 };
}

function passesSuggestedPlayQualityGate(play: {
  outcomeType: string;
  posteriorHitRate: number;
  metaScore: number | null;
  playerTarget?: { stat: "ppg" | "rpg" | "apg"; statValue: number } | null;
  marketPick?: SuggestedMarketPick | null;
  requireMarketLine?: boolean;
}): boolean {
  const family = betFamilyForOutcome(play.outcomeType);
  const gates = gateThresholdsForFamily(family);
  if (play.posteriorHitRate < gates.minPosterior) return false;
  if (play.metaScore != null && play.metaScore < gates.minMeta) return false;

  // Guard against implausible target ladders (e.g. 10+ assists on a 4.3 APG target).
  const threshold = parsePlayerThresholdOutcome(play.outcomeType);
  if (threshold && play.playerTarget) {
    if (play.playerTarget.stat !== threshold.stat) return false;
    const minBaseline = threshold.line * 0.65;
    if (play.playerTarget.statValue < minBaseline) return false;
  }

  const requireMarketLine = play.requireMarketLine ?? true;
  // For live-bet suggestions, require real market pricing and a positive EV buffer.
  if (requireMarketLine) {
    if (!play.marketPick) return false;
    if (play.marketPick.ev < gates.minEv) return false;
    if (play.marketPick.edge <= 0) return false;
  } else if (play.marketPick) {
    // If market exists, still reject strongly negative EV.
    if (play.marketPick.ev < -0.01) return false;
  }

  return true;
}

function selectDiversifiedBetPicks<T extends {
  outcomeType: string;
  marketPick?: SuggestedMarketPick | null;
  metaScore?: number | null;
  posteriorHitRate: number;
  confidence: number;
}>(
  plays: T[],
  maxPicks: number,
): T[] {
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
    if (usedFamilies.has(family)) continue;
    if (usedDedupFamilies.has(dedupFamily)) continue;
    selected.push(p);
    usedFamilies.add(family);
    usedDedupFamilies.add(dedupFamily);
    if (selected.length >= maxPicks) break;
  }
  return selected;
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");
  const refreshLedger = url.searchParams.get("refreshLedger") === "1";

  if (!dateStr) {
    const today = new Date().toISOString().slice(0, 10);
    return NextResponse.redirect(new URL(`/api/predictions?date=${today}`, req.url));
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = getSeasonForDate(targetDate);
  const metaModel = await loadMetaModel();
  const defaultStake = Math.max(0, Number(process.env.SUGGESTED_PLAY_STAKE ?? 10));
  const bankrollStart = Math.max(0, Number(process.env.SUGGESTED_PLAY_BANKROLL ?? 1000));
  const maxBetPicksPerGame = Math.max(
    1,
    Math.min(2, Number(process.env.SUGGESTED_PLAY_MAX_PER_GAME ?? 2)),
  );
  const allowFallbackOddsForLedger = (process.env.SUGGESTED_PLAY_ALLOW_FALLBACK_ODDS ?? "false") === "true";
  const fallbackAmericanOdds = Number(
    process.env.SUGGESTED_PLAY_DEFAULT_ODDS ?? -110,
  );
  const seasonDateSql = targetDate.toISOString().slice(0, 10);
  const seasonToDateV2 = await prisma.$queryRawUnsafe<Array<{ hitBool: boolean; count: number }>>(
    `SELECT h."hitBool" as "hitBool", COUNT(*)::int as "count"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     JOIN "Game" g ON g."id" = h."gameId"
     WHERE p."status" = 'deployed'
       AND g."season" = ${season}
       AND g."date" <= '${seasonDateSql}'
     GROUP BY h."hitBool"`,
  );
  const seasonV2Hits = seasonToDateV2.find((r) => r.hitBool)?.count ?? 0;
  const seasonV2Total = seasonToDateV2.reduce((sum, r) => sum + r.count, 0);

  // Query ±1 day to catch games mis-dated during ingestion (e.g. March 2 games stored as March 1)
  const dayMs = 86400000;
  const rangeStart = new Date(targetDate.getTime() - dayMs);
  const rangeEnd = new Date(targetDate.getTime() + dayMs);

  const gamesRaw = await prisma.game.findMany({
    where: { date: { gte: rangeStart, lte: rangeEnd } },
    include: {
      homeTeam: true,
      awayTeam: true,
      odds: true,
      context: true,
      playerStats: true,
    },
    orderBy: { tipoffTimeUtc: "asc" },
  });

  // Filter to games whose Eastern tipoff date matches requested date (source of truth for game day)
  const gamesForDate = gamesRaw.filter((g) => {
    if (!g.tipoffTimeUtc) {
      // No tipoff time: trust stored date
      const storedDate = g.date instanceof Date ? g.date.toISOString().slice(0, 10) : String(g.date).slice(0, 10);
      return storedDate === dateStr;
    }
    const easternDate = getEasternDateFromUtc(g.tipoffTimeUtc);
    return easternDate === dateStr;
  });

  // Deduplicate by matchup (home/away team codes - handles duplicate team IDs)
  const seenMatchups = new Map<string, (typeof gamesForDate)[0]>();
  for (const g of gamesForDate) {
    const homeCode = g.homeTeam?.code ?? g.homeTeamId.toString();
    const awayCode = g.awayTeam?.code ?? g.awayTeamId.toString();
    const key = `${awayCode}@${homeCode}`;
    const existing = seenMatchups.get(key);
    if (!existing) {
      seenMatchups.set(key, g);
    } else {
      // Prefer game with context and odds
      const better =
        (g.context && !existing.context) || (g.odds?.length && !existing.odds?.length)
          ? g
          : existing;
      seenMatchups.set(key, better);
    }
  }
  const games = [...seenMatchups.values()].sort(
    (a, b) => (a.tipoffTimeUtc?.getTime() ?? 0) - (b.tipoffTimeUtc?.getTime() ?? 0),
  );

  let wagerTracking = await getWagerTrackingSummary({
    dateStr,
    season,
    stakePerPick: defaultStake,
    bankrollStart,
  });

  if (games.length === 0) {
    return NextResponse.json({
      date: dateStr,
      season,
      seasonToDate: {
        throughDate: dateStr,
        v2: {
          hits: seasonV2Hits,
          total: seasonV2Total,
          hitRate: seasonV2Total > 0 ? seasonV2Hits / seasonV2Total : null,
        },
      },
      wagerTracking,
      games: [],
      message: "No games found",
    });
  }

  const gameIds = games.map((g) => g.id);
  const playerPropsRaw = await prisma.playerPropOdds.findMany({
    where: { gameId: { in: gameIds } },
    select: {
      gameId: true,
      playerId: true,
      market: true,
      line: true,
      overPrice: true,
      underPrice: true,
      player: { select: { firstname: true, lastname: true } },
    },
  });
  const playerPropsByGame = new Map<string, PlayerPropRow[]>();
  for (const gid of gameIds) playerPropsByGame.set(gid, []);
  for (const r of playerPropsRaw) {
    playerPropsByGame.get(r.gameId)?.push(r);
  }

  const discoveryV2ByGame = await getDiscoveryV2MatchesByGame(
    games.map((g) => g.id),
  );

  const teamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const teamSnapshots = await computeTeamSnapshots(season, targetDate, teamIds);

  // For completed games, fetch outcome events to show hit/miss status
  const completedGameIds = games
    .filter((g) => hasCompletedScore(g))
    .map((g) => g.id);

  const [gameOutcomes] = await Promise.all([
    completedGameIds.length > 0
      ? prisma.gameEvent.findMany({
          where: {
            gameId: { in: completedGameIds },
            type: "outcome",
          },
          select: {
            gameId: true,
            eventKey: true,
            side: true,
            meta: true,
          },
        })
      : [],
  ]);

  // Build lookup: gameId -> Set of outcome keys that hit (from GameEvent)
  const outcomesByGame = new Map<string, Map<string, { hit: true; meta: unknown }>>();
  for (const ev of gameOutcomes) {
    if (!outcomesByGame.has(ev.gameId)) {
      outcomesByGame.set(ev.gameId, new Map());
    }
    const key = `${ev.eventKey}:${ev.side}`;
    outcomesByGame.get(ev.gameId)!.set(key, { hit: true, meta: ev.meta });
  }

  // For player outcomes, fetch player names
  const playerIds = new Set<number>();
  for (const ev of gameOutcomes) {
    const meta = ev.meta as Record<string, unknown> | null;
    if (meta?.playerId && typeof meta.playerId === "number") {
      playerIds.add(meta.playerId);
    }
  }
  const players = playerIds.size > 0
    ? await prisma.player.findMany({
        where: { id: { in: [...playerIds] } },
        select: { id: true, firstname: true, lastname: true },
      })
    : [];
  const playerMap = new Map(players.map((p) => [p.id, `${p.firstname} ${p.lastname}`]));

  // Fetch player contexts for games (from DB), or compute on-the-fly for upcoming games
  const playerContexts = await prisma.playerGameContext.findMany({
    where: { gameId: { in: gameIds } },
    include: { player: true },
  });

  // Build team code -> all team IDs (for fallback when PlayerGameContext is empty)
  const scheduledTeams = await prisma.team.findMany({
    where: { id: { in: teamIds } },
    select: { id: true, code: true },
  });
  const teamCodes = [...new Set(scheduledTeams.map((t) => t.code).filter(Boolean))] as string[];
  const allTeamsWithCodes = await prisma.team.findMany({
    where: { code: { in: teamCodes } },
    select: { id: true, code: true },
  });
  const teamIdToAllIds = new Map<number, number[]>();
  for (const t of scheduledTeams) {
    if (!t.code) continue;
    const ids = allTeamsWithCodes.filter((x) => x.code === t.code).map((x) => x.id);
    teamIdToAllIds.set(t.id, ids);
  }

  type PlayerInfo = { id: number; name: string; stat: number };
  type GamePlayerContext = {
    homeTopScorer: PlayerInfo | null;
    homeTopRebounder: PlayerInfo | null;
    homeTopPlaymaker: PlayerInfo | null;
    awayTopScorer: PlayerInfo | null;
    awayTopRebounder: PlayerInfo | null;
    awayTopPlaymaker: PlayerInfo | null;
  };
  const playerContextByGame = new Map<string, GamePlayerContext>();

  for (const game of games) {
    const contexts = playerContexts.filter((c) => c.gameId === game.id);
    const homeContexts = contexts.filter((c) => c.teamId === game.homeTeamId);
    const awayContexts = contexts.filter((c) => c.teamId === game.awayTeamId);

    const getTop = (list: typeof contexts, stat: "ppg" | "rpg" | "apg"): PlayerInfo | null => {
      const sorted = [...list].sort((a, b) => b[stat] - a[stat]);
      const top = sorted[0];
      return top ? { id: top.playerId, name: `${top.player.firstname} ${top.player.lastname}`, stat: top[stat] } : null;
    };

    // Use DB context if available; otherwise compute on-the-fly from PlayerGameStat
    if (contexts.length > 0) {
      playerContextByGame.set(game.id, {
        homeTopScorer: getTop(homeContexts, "ppg"),
        homeTopRebounder: getTop(homeContexts, "rpg"),
        homeTopPlaymaker: getTop(homeContexts, "apg"),
        awayTopScorer: getTop(awayContexts, "ppg"),
        awayTopRebounder: getTop(awayContexts, "rpg"),
        awayTopPlaymaker: getTop(awayContexts, "apg"),
      });
    } else {
      const fallback = await computePlayerContextFallback(
        prisma,
        season,
        targetDate,
        game.homeTeamId,
        game.awayTeamId,
        teamIdToAllIds,
      );
      playerContextByGame.set(game.id, fallback);
    }
  }

  const result = games.map((game) => {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    const consensus = game.odds.find((o) => o.source === "consensus") ?? game.odds[0];

    // Commercial odds excluded from public API; used internally for pattern matching only
    const context = {
      home: homeSnap
        ? {
            record: `${homeSnap.wins}-${homeSnap.losses}`,
            ppg: homeSnap.ppg,
            oppg: homeSnap.oppg,
            rankOff: homeSnap.rankOff,
            rankDef: homeSnap.rankDef,
            streak: homeSnap.streak,
          }
        : null,
      away: awaySnap
        ? {
            record: `${awaySnap.wins}-${awaySnap.losses}`,
            ppg: awaySnap.ppg,
            oppg: awaySnap.oppg,
            rankOff: awaySnap.rankOff,
            rankDef: awaySnap.rankDef,
            streak: awaySnap.streak,
          }
        : null,
    };

    const gamePlayerContexts = playerContexts.filter((c) => c.gameId === game.id);
    const gameEventContext = {
      game: game as unknown as GameEventContext["game"],
      context: (game.context ??
        {
          id: "",
          gameId: game.id,
          homeWins: homeSnap?.wins ?? 0,
          homeLosses: homeSnap?.losses ?? 0,
          homePpg: homeSnap?.ppg ?? 0,
          homeOppg: homeSnap?.oppg ?? 0,
          homePace: homeSnap?.pace ?? null,
          homeRebPg: homeSnap?.rebPg ?? null,
          homeAstPg: homeSnap?.astPg ?? null,
          homeFg3Pct: homeSnap?.fg3Pct ?? null,
          homeFtPct: homeSnap?.ftPct ?? null,
          homeRankOff: homeSnap?.rankOff ?? null,
          homeRankDef: homeSnap?.rankDef ?? null,
          homeRankPace: homeSnap?.rankPace ?? null,
          homeStreak: homeSnap?.streak ?? 0,
          awayWins: awaySnap?.wins ?? 0,
          awayLosses: awaySnap?.losses ?? 0,
          awayPpg: awaySnap?.ppg ?? 0,
          awayOppg: awaySnap?.oppg ?? 0,
          awayPace: awaySnap?.pace ?? null,
          awayRebPg: awaySnap?.rebPg ?? null,
          awayAstPg: awaySnap?.astPg ?? null,
          awayFg3Pct: awaySnap?.fg3Pct ?? null,
          awayFtPct: awaySnap?.ftPct ?? null,
          awayRankOff: awaySnap?.rankOff ?? null,
          awayRankDef: awaySnap?.rankDef ?? null,
          awayRankPace: awaySnap?.rankPace ?? null,
          awayStreak: awaySnap?.streak ?? 0,
          homeRestDays: null,
          awayRestDays: null,
          homeIsB2b: false,
          awayIsB2b: false,
          h2hHomeWins: 0,
          h2hAwayWins: 0,
        }) as GameEventContext["context"],
      playerContexts: gamePlayerContexts as unknown as GameEventContext["playerContexts"],
      stats: game.playerStats as unknown as GameEventContext["stats"],
      odds: (consensus ?? null) as GameEventContext["odds"],
    } as GameEventContext;
    const isFinal = hasCompletedScore(game);
    const gameOutcomeMap = outcomesByGame.get(game.id);
    const gamePlayerCtx = playerContextByGame.get(game.id);
    const propsForGame = playerPropsByGame.get(game.id) ?? [];
    const discoveryV2RawMatches = discoveryV2ByGame.get(game.id) ?? [];
    // Reduce "same pick every game" behavior by suppressing ultra-generic single-condition matches.
    const discoveryV2Matches = discoveryV2RawMatches.filter(
      (p) => !isLowSpecificityPattern(p.conditions ?? []),
    );
    const actionableDiscoveryV2Matches = discoveryV2Matches.filter((p) =>
      isOutcomeActionableForMarket(p.outcomeType, consensus ?? null, p.posteriorHitRate),
    );
    const nonGenericV2Matches = discoveryV2Matches.filter(
      (p) => !isGenericPlayerOutcome(p.outcomeType),
    );
    const genericV2Matches = discoveryV2Matches.filter((p) =>
      isGenericPlayerOutcome(p.outcomeType),
    );
    const actionableNonGenericV2Matches = actionableDiscoveryV2Matches.filter(
      (p) => !isGenericPlayerOutcome(p.outcomeType),
    );
    const actionableGenericV2Matches = actionableDiscoveryV2Matches.filter((p) =>
      isGenericPlayerOutcome(p.outcomeType),
    );
    // If we have diverse outcomes, keep generic rebound/double-double to at most one slot.
    const curatedDiscoveryV2Matches =
      actionableNonGenericV2Matches.length > 0
        ? [...actionableNonGenericV2Matches, ...actionableGenericV2Matches.slice(0, 1)].slice(0, 8)
        : actionableGenericV2Matches.slice(0, 1);
    const pickFromProps = (
      market: string,
      minLine: number | null,
    ): { id: number; name: string; line: number | null; pOver: number | null } | null => {
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
    };

    const pickV2PlayerTarget = (outcomeKey: string): V2PlayerTarget | null => {
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
        const prop = pickFromProps("player_rebounds", 9.5);
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
        const prop = pickFromProps("player_assists", 9.5);
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
      if (outcome === "HOME_TOP_ASSIST_8_PLUS" || outcome === "HOME_TOP_ASSIST_10_PLUS" || outcome === "HOME_TOP_PLAYMAKER_8_PLUS" || outcome === "HOME_TOP_PLAYMAKER_10_PLUS") {
        const p = gamePlayerCtx.homeTopPlaymaker;
        return p
          ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Home top playmaker by pregame context" }
          : null;
      }
      if (outcome === "AWAY_TOP_ASSIST_8_PLUS" || outcome === "AWAY_TOP_ASSIST_10_PLUS" || outcome === "AWAY_TOP_PLAYMAKER_8_PLUS" || outcome === "AWAY_TOP_PLAYMAKER_10_PLUS") {
        const p = gamePlayerCtx.awayTopPlaymaker;
        return p
          ? { id: p.id, name: p.name, stat: "apg", statValue: p.stat, rationale: "Away top playmaker by pregame context" }
          : null;
      }
      if (outcome === "HOME_TOP_SCORER_25_PLUS" || outcome === "HOME_TOP_SCORER_30_PLUS") {
        const p = gamePlayerCtx.homeTopScorer;
        return p
          ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Home top scorer by pregame context" }
          : null;
      }
      if (outcome === "AWAY_TOP_SCORER_25_PLUS" || outcome === "AWAY_TOP_SCORER_30_PLUS") {
        const p = gamePlayerCtx.awayTopScorer;
        return p
          ? { id: p.id, name: p.name, stat: "ppg", statValue: p.stat, rationale: "Away top scorer by pregame context" }
          : null;
      }
      if (outcome === "HOME_TOP_REBOUNDER_10_PLUS" || outcome === "HOME_TOP_REBOUNDER_12_PLUS") {
        const p = gamePlayerCtx.homeTopRebounder;
        return p
          ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Home top rebounder by pregame context" }
          : null;
      }
      if (outcome === "AWAY_TOP_REBOUNDER_10_PLUS" || outcome === "AWAY_TOP_REBOUNDER_12_PLUS") {
        const p = gamePlayerCtx.awayTopRebounder;
        return p
          ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Away top rebounder by pregame context" }
          : null;
      }
      if (outcome === "PLAYER_30_PLUS" || outcome === "PLAYER_40_PLUS" || outcome === "PLAYER_5_PLUS_THREES") {
        if (outcome === "PLAYER_30_PLUS") {
          const prop = pickFromProps("player_points", 29.5);
          if (prop) {
            const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
            const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
            return { id: prop.id, name: prop.name, stat: "ppg", statValue: Math.max(gamePlayerCtx.homeTopScorer?.stat ?? 0, gamePlayerCtx.awayTopScorer?.stat ?? 0), rationale: `Best prop over implied (${pct}) for 30+ points (${lineStr})` };
          }
        }
        if (outcome === "PLAYER_40_PLUS") {
          const prop = pickFromProps("player_points", 39.5);
          if (prop) {
            const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
            const lineStr = prop.line != null ? `line ${prop.line}` : "no line";
            return { id: prop.id, name: prop.name, stat: "ppg", statValue: Math.max(gamePlayerCtx.homeTopScorer?.stat ?? 0, gamePlayerCtx.awayTopScorer?.stat ?? 0), rationale: `Best prop over implied (${pct}) for 40+ points (${lineStr})` };
          }
        }
        if (outcome === "PLAYER_5_PLUS_THREES") {
          const prop = pickFromProps("player_threes", 4.5);
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
          const prop = pickFromProps("player_double_double", 0.5);
          if (prop) {
            const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
            return { id: prop.id, name: prop.name, stat: "rpg", statValue: Math.max(gamePlayerCtx.homeTopRebounder?.stat ?? 0, gamePlayerCtx.awayTopRebounder?.stat ?? 0), rationale: `Best prop implied (${pct}) for double-double` };
          }
        }
        if (outcome === "PLAYER_TRIPLE_DOUBLE") {
          const prop = pickFromProps("player_triple_double", 0.5);
          if (prop) {
            const pct = prop.pOver != null ? `${(prop.pOver * 100).toFixed(1)}%` : "n/a";
            return { id: prop.id, name: prop.name, stat: "apg", statValue: Math.max(gamePlayerCtx.homeTopPlaymaker?.stat ?? 0, gamePlayerCtx.awayTopPlaymaker?.stat ?? 0), rationale: `Best prop implied (${pct}) for triple-double` };
          }
        }
        const p = pickBest(gamePlayerCtx.homeTopRebounder, gamePlayerCtx.awayTopRebounder);
        return p ? { id: p.id, name: p.name, stat: "rpg", statValue: p.stat, rationale: "Double-double proxy target (top rebound profile)" } : null;
      }
      return null;
    };

    const evaluateOutcome = (outcomeKey: string, target: V2PlayerTarget | null): OutcomeEval | null => {
      if (!isFinal) return null;
      const outcomeResult = gameOutcomeMap ? resolveOutcomeResult(gameOutcomeMap, outcomeKey) : undefined;
      if (outcomeResult) {
        const meta = outcomeResult.meta as Record<string, unknown> | undefined;
        const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
        if (target && actualPlayerId != null) {
          const targetHit = actualPlayerId === target.id;
          const actualName = playerMap.get(actualPlayerId) ?? `Player ${actualPlayerId}`;
          return {
            hit: targetHit,
            scope: "target",
            explanation: targetHit
              ? buildOutcomeExplanation(outcomeResult.meta, playerMap)
              : `Outcome hit by ${actualName}, not predicted target ${target.name}.`,
          };
        }
        return {
          hit: true,
          scope: "outcome",
          explanation: buildOutcomeExplanation(outcomeResult.meta, playerMap),
        };
      }
      const catalogOutcome = computeOutcomeFromCatalog(outcomeKey, gameEventContext);
      if (catalogOutcome) {
        const meta = (catalogOutcome.meta ?? null) as Record<string, unknown> | null;
        const actualPlayerId = typeof meta?.playerId === "number" ? meta.playerId : null;
        if (target && actualPlayerId != null) {
          const targetHit = actualPlayerId === target.id;
          const actualName = playerMap.get(actualPlayerId) ?? `Player ${actualPlayerId}`;
          return {
            hit: targetHit,
            scope: "target",
            explanation: targetHit
              ? buildOutcomeExplanation(catalogOutcome.meta, playerMap) ??
                buildTargetOutcomeExplanation(outcomeKey, target, game.playerStats)
              : `Outcome hit by ${actualName}, not predicted target ${target.name}.`,
          };
        }
        const targetExplanation = buildTargetOutcomeExplanation(
          outcomeKey,
          target,
          game.playerStats,
        );
        return {
          hit: catalogOutcome.hit,
          scope: "outcome",
          explanation:
            targetExplanation ??
            buildOutcomeExplanation(catalogOutcome.meta, playerMap) ??
            computeOutcomeFromFinalScore(outcomeKey, game, consensus ?? null)?.explanation ??
            `Final score ${game.awayScore}-${game.homeScore}`,
        };
      }
      const computed = computeOutcomeFromFinalScore(outcomeKey, game, consensus ?? null);
      if (computed) {
        return {
          hit: computed.hit,
          scope: "outcome",
          explanation: computed.explanation,
        };
      }
      // Unknown: we don't have an outcome event (and can't infer from score/odds).
      return null;
    };
    const enrichedDiscoveryV2Matches = curatedDiscoveryV2Matches.map((p) => ({
      playerTarget: pickV2PlayerTarget(p.outcomeType),
      ...p,
      result: evaluateOutcome(p.outcomeType, pickV2PlayerTarget(p.outcomeType)),
    }));
    const suggestedPlayMap = new Map<
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
        result: OutcomeEval | null;
        playerTarget: V2PlayerTarget | null;
      }
    >();
    for (const p of curatedDiscoveryV2Matches) {
      const target = pickV2PlayerTarget(p.outcomeType);
      const dedupKey = `${outcomeDedupFamily(p.outcomeType)}|${target?.id ?? "none"}`;
      const existing = suggestedPlayMap.get(dedupKey);
      if (!existing) {
        suggestedPlayMap.set(dedupKey, {
          dedupKey,
          outcomeType: p.outcomeType,
          bestConditions: p.conditions ?? [],
          scoreSum: p.score,
          count: 1,
          bestPosterior: p.posteriorHitRate,
          bestEdge: p.edge,
          bestN: p.n,
          result: evaluateOutcome(p.outcomeType, target),
          playerTarget: target,
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
      }
    }
    const rankedSuggestedPlays = [...suggestedPlayMap.values()]
      .map((r) => {
        const confidence = r.scoreSum / r.count;
        const metaScore = metaModel
          ? scoreMetaModel(metaModel, {
              outcomeType: r.outcomeType,
              conditions: r.bestConditions,
              posteriorHitRate: r.bestPosterior,
              edge: r.bestEdge,
              score: confidence,
              n: r.bestN,
            })
          : null;
        const baseProb = metaScore ?? r.bestPosterior;
        const marketPick = selectBestMarketBackedPick({
          outcomeType: r.outcomeType,
          target: r.playerTarget,
          propsForGame,
          baseProb,
          confidence,
          supportN: r.bestN,
        });
        return {
          outcomeType: r.outcomeType,
          displayLabel: marketPick?.label ?? null,
          confidence,
          posteriorHitRate: r.bestPosterior,
          edge: r.bestEdge,
          metaScore,
          votes: r.count,
          result: r.result,
          playerTarget: r.playerTarget,
          marketPick,
        };
      })
      .sort(
        (a, b) =>
          (b.metaScore ?? -1) - (a.metaScore ?? -1) ||
          b.confidence - a.confidence ||
          b.posteriorHitRate - a.posteriorHitRate,
      );
    const qualitySuggestedPlays = rankedSuggestedPlays.filter((p) =>
      passesSuggestedPlayQualityGate({
        ...p,
        requireMarketLine: false,
      }),
    );
    const bettableSuggestedPlays = qualitySuggestedPlays.filter((p) =>
      passesSuggestedPlayQualityGate({
        ...p,
        requireMarketLine: true,
      }),
    );
    const suggestedPlays =
      qualitySuggestedPlays.length > 0
        ? selectDiversifiedBetPicks(
            qualitySuggestedPlays,
            Math.max(3, maxBetPicksPerGame),
          )
        : [];

    return {
      id: game.id,
      homeTeam: { id: game.homeTeamId, code: game.homeTeam.code, name: game.homeTeam.name },
      awayTeam: { id: game.awayTeamId, code: game.awayTeam.code, name: game.awayTeam.name },
      tipoff: game.tipoffTimeUtc,
      status: game.status,
      homeScore: game.homeScore,
      awayScore: game.awayScore,
      odds: null,
      context,
      discoveryV2Matches: enrichedDiscoveryV2Matches,
      suggestedPlays,
      suggestedBetPicks: bettableSuggestedPlays,
    };
  });

  try {
    const shouldUpsert = await shouldUpsertLedgerSnapshot(dateStr, refreshLedger);
    if (shouldUpsert) {
      await upsertSuggestedPlayLedger({
        dateStr,
        season,
        games: result,
        defaultStake,
        fallbackAmericanOdds,
        allowFallbackOddsForLedger,
      });
    }
    wagerTracking = await getWagerTrackingSummary({
      dateStr,
      season,
      stakePerPick: defaultStake,
      bankrollStart,
    });
  } catch (err) {
    // Ledger capture should not block predictions API responses.
    console.error("Failed to upsert SuggestedPlayLedger rows:", err);
  }

  return NextResponse.json({
    date: dateStr,
    season,
    seasonToDate: {
      throughDate: dateStr,
      v2: {
        hits: seasonV2Hits,
        total: seasonV2Total,
        hitRate: seasonV2Total > 0 ? seasonV2Hits / seasonV2Total : null,
      },
    },
    wagerTracking,
    games: result,
  });
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function sqlStr(v: string | null | undefined): string {
  return v == null ? "NULL" : `'${sqlEsc(v)}'`;
}

function sqlNum(v: number | null | undefined): string {
  return v == null || !Number.isFinite(v) ? "NULL" : String(v);
}

function sqlBool(v: boolean | null | undefined): string {
  return v == null ? "NULL" : v ? "TRUE" : "FALSE";
}

async function upsertSuggestedPlayLedger(args: {
  dateStr: string;
  season: number;
  defaultStake: number;
  fallbackAmericanOdds: number;
  allowFallbackOddsForLedger: boolean;
  games: Array<{
    id: string;
    suggestedPlays: Array<{
      outcomeType: string;
      displayLabel: string | null;
      confidence: number;
      posteriorHitRate: number;
      edge: number;
      metaScore: number | null;
      votes: number;
      result: OutcomeEval | null;
      playerTarget: V2PlayerTarget | null;
      marketPick?: SuggestedMarketPick | null;
    }>;
    suggestedBetPicks?: Array<{
      outcomeType: string;
      displayLabel: string | null;
      confidence: number;
      posteriorHitRate: number;
      edge: number;
      metaScore: number | null;
      votes: number;
      result: OutcomeEval | null;
      playerTarget: V2PlayerTarget | null;
      marketPick?: SuggestedMarketPick | null;
    }>;
  }>;
}): Promise<void> {
  if (!(await isSuggestedPlayLedgerAvailable())) return;
  const stake = Number.isFinite(args.defaultStake) ? args.defaultStake : 10;
  const fallbackOdds = Number.isFinite(args.fallbackAmericanOdds) &&
    args.fallbackAmericanOdds !== 0
    ? args.fallbackAmericanOdds
    : -110;
  const valueRows: string[] = [];
  for (const game of args.games) {
    const playsForLedger = game.suggestedBetPicks ?? game.suggestedPlays ?? [];
    for (const play of playsForLedger) {
      const dedupKey = ledgerDedupKey(play);
      const marketPick = play.marketPick ?? null;
      const settledHit =
        typeof play.result?.hit === "boolean" ? play.result.hit : null;
      const settledResult =
        settledHit == null ? "PENDING" : settledHit ? "HIT" : "MISS";
      const hasMarketPrice = marketPick?.overPrice != null && Number.isFinite(marketPick.overPrice);
      const priceAmerican = hasMarketPrice
        ? (marketPick?.overPrice ?? null)
        : args.allowFallbackOddsForLedger
          ? fallbackOdds
          : null;
      const isActionable = hasMarketPrice && Number.isFinite(priceAmerican) && priceAmerican !== 0;
      let payout: number | null = null;
      let profit: number | null = null;
      if (settledHit != null && priceAmerican != null && Number.isFinite(priceAmerican) && priceAmerican !== 0) {
        const decimalOdds = 1 + payoutFromAmerican(priceAmerican);
        payout = settledHit ? stake * decimalOdds : 0;
        profit = settledHit ? payout - stake : -stake;
      }
      const targetName = play.playerTarget?.name ?? marketPick?.playerName ?? null;
      valueRows.push(
        `('${crypto.randomUUID()}','${sqlEsc(args.dateStr)}',${args.season},'${sqlEsc(game.id)}','${sqlEsc(dedupKey)}',` +
          `'${sqlEsc(play.outcomeType)}',${sqlStr(play.displayLabel)},${sqlNum(play.playerTarget?.id ?? null)},${sqlStr(targetName)},` +
          `${sqlStr(marketPick?.market ?? null)},${sqlNum(marketPick?.line ?? null)},${sqlNum(priceAmerican)},${sqlNum(marketPick?.impliedProb ?? null)},` +
          `${sqlNum(marketPick?.impliedProb ?? null)},${sqlNum(marketPick?.estimatedProb ?? null)},${sqlNum(marketPick?.edge ?? null)},${sqlNum(marketPick?.ev ?? null)},` +
          `NULL,NULL,NULL,NULL,NULL,` +
          `${sqlNum(play.posteriorHitRate)},${sqlNum(play.metaScore)},${sqlNum(play.confidence)},${Math.max(1, play.votes ?? 1)},` +
          `${sqlNum(stake)},${isActionable ? "TRUE" : "FALSE"},'${settledResult}',${sqlBool(settledHit)},${sqlNum(payout)},${sqlNum(profit)},NOW(),NOW())`,
      );
    }
  }
  await prisma.$executeRawUnsafe(
    `DELETE FROM "SuggestedPlayLedger" WHERE "date" = '${sqlEsc(args.dateStr)}'`,
  );
  if (valueRows.length === 0) return;
  await prisma.$executeRawUnsafe(
    `INSERT INTO "SuggestedPlayLedger"
      ("id","date","season","gameId","dedupKey","outcomeType","displayLabel","targetPlayerId","targetPlayerName","market","line","priceAmerican","impliedProb","betImpliedProb","estimatedProb","modelEdge","ev","closePriceAmerican","closeImpliedProb","clvDeltaProb","clvDeltaCents","clvStatus","posteriorHitRate","metaScore","confidence","votes","stake","isActionable","settledResult","settledHit","payout","profit","capturedAt","updatedAt")
     VALUES ${valueRows.join(",")}
     ON CONFLICT ("date","gameId","dedupKey") DO UPDATE SET
       "displayLabel" = EXCLUDED."displayLabel",
       "targetPlayerId" = EXCLUDED."targetPlayerId",
       "targetPlayerName" = EXCLUDED."targetPlayerName",
       "market" = EXCLUDED."market",
       "line" = EXCLUDED."line",
       "priceAmerican" = EXCLUDED."priceAmerican",
       "impliedProb" = EXCLUDED."impliedProb",
      "betImpliedProb" = EXCLUDED."betImpliedProb",
       "estimatedProb" = EXCLUDED."estimatedProb",
       "modelEdge" = EXCLUDED."modelEdge",
       "ev" = EXCLUDED."ev",
      "closePriceAmerican" = EXCLUDED."closePriceAmerican",
      "closeImpliedProb" = EXCLUDED."closeImpliedProb",
      "clvDeltaProb" = EXCLUDED."clvDeltaProb",
      "clvDeltaCents" = EXCLUDED."clvDeltaCents",
      "clvStatus" = EXCLUDED."clvStatus",
       "posteriorHitRate" = EXCLUDED."posteriorHitRate",
       "metaScore" = EXCLUDED."metaScore",
       "confidence" = EXCLUDED."confidence",
       "votes" = EXCLUDED."votes",
       "stake" = EXCLUDED."stake",
       "isActionable" = EXCLUDED."isActionable",
       "settledResult" = EXCLUDED."settledResult",
       "settledHit" = EXCLUDED."settledHit",
       "payout" = EXCLUDED."payout",
       "profit" = EXCLUDED."profit",
       "updatedAt" = NOW()`,
  );
}

function toWagerSummary(rows: Array<{
  settledResult: string | null;
  count: number;
  totalStaked: number | null;
  settledStaked: number | null;
  netPnl: number | null;
}>): WagerSummary {
  const byResult = new Map(rows.map((r) => [r.settledResult ?? "PENDING", r]));
  const wins = byResult.get("HIT")?.count ?? 0;
  const losses = byResult.get("MISS")?.count ?? 0;
  const pendingBets = byResult.get("PENDING")?.count ?? 0;
  const bets = wins + losses + pendingBets;
  const settledBets = wins + losses;
  const totalStaked = rows.reduce((sum, r) => sum + Number(r.totalStaked ?? 0), 0);
  const settledStaked = rows.reduce((sum, r) => sum + Number(r.settledStaked ?? 0), 0);
  const netPnl = rows.reduce((sum, r) => sum + Number(r.netPnl ?? 0), 0);
  const roi = settledStaked > 0 ? netPnl / settledStaked : null;
  return {
    bets,
    settledBets,
    pendingBets,
    wins,
    losses,
    totalStaked,
    settledStaked,
    netPnl,
    roi,
  };
}

async function getWagerTrackingSummary(args: {
  dateStr: string;
  season: number;
  stakePerPick: number;
  bankrollStart: number;
}): Promise<{
  stakePerPick: number;
  bankrollStart: number;
  day: WagerSummary & { date: string };
  seasonToDate: WagerSummary & { throughDate: string; bankrollCurrent: number };
} | null> {
  if (!(await isSuggestedPlayLedgerAvailable())) return null;
  const [dayRows, seasonRows] = await Promise.all([
    prisma.$queryRawUnsafe<
      Array<{
        settledResult: string | null;
        count: number;
        totalStaked: number | null;
        settledStaked: number | null;
        netPnl: number | null;
      }>
    >(
      `SELECT
         COALESCE(l."settledResult", 'PENDING') as "settledResult",
         COUNT(*)::int as "count",
         SUM(l."stake")::float8 as "totalStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN l."stake" ELSE 0 END)::float8 as "settledStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN COALESCE(l."profit",0) ELSE 0 END)::float8 as "netPnl"
       FROM "SuggestedPlayLedger" l
       WHERE l."isActionable" = TRUE
         AND l."date" = '${sqlEsc(args.dateStr)}'
       GROUP BY COALESCE(l."settledResult", 'PENDING')`,
    ),
    prisma.$queryRawUnsafe<
      Array<{
        settledResult: string | null;
        count: number;
        totalStaked: number | null;
        settledStaked: number | null;
        netPnl: number | null;
      }>
    >(
      `SELECT
         COALESCE(l."settledResult", 'PENDING') as "settledResult",
         COUNT(*)::int as "count",
         SUM(l."stake")::float8 as "totalStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN l."stake" ELSE 0 END)::float8 as "settledStaked",
         SUM(CASE WHEN COALESCE(l."settledResult",'PENDING') IN ('HIT','MISS') THEN COALESCE(l."profit",0) ELSE 0 END)::float8 as "netPnl"
       FROM "SuggestedPlayLedger" l
       WHERE l."isActionable" = TRUE
         AND l."season" = ${args.season}
         AND l."date" <= '${sqlEsc(args.dateStr)}'
       GROUP BY COALESCE(l."settledResult", 'PENDING')`,
    ),
  ]);

  const day = toWagerSummary(dayRows);
  const seasonToDate = toWagerSummary(seasonRows);
  return {
    stakePerPick: args.stakePerPick,
    bankrollStart: args.bankrollStart,
    day: {
      date: args.dateStr,
      ...day,
    },
    seasonToDate: {
      throughDate: args.dateStr,
      bankrollCurrent: args.bankrollStart + seasonToDate.netPnl,
      ...seasonToDate,
    },
  };
}

async function isSuggestedPlayLedgerAvailable(): Promise<boolean> {
  if (suggestedPlayLedgerAvailableCache != null) {
    return suggestedPlayLedgerAvailableCache;
  }
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."SuggestedPlayLedger"') IS NOT NULL as "exists"`,
  );
  const exists = Boolean(rows[0]?.exists);
  suggestedPlayLedgerAvailableCache = exists;
  return exists;
}

async function hasLedgerRowsForDate(dateStr: string): Promise<boolean> {
  if (!(await isSuggestedPlayLedgerAvailable())) return false;
  const rows = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count" FROM "SuggestedPlayLedger" WHERE "date" = '${sqlEsc(dateStr)}'`,
  );
  return (rows[0]?.count ?? 0) > 0;
}

async function shouldUpsertLedgerSnapshot(
  dateStr: string,
  refreshLedger: boolean,
): Promise<boolean> {
  if (refreshLedger) return true;
  const today = new Date().toISOString().slice(0, 10);
  if (dateStr >= today) return true; // today/future can change as markets/results update
  const hasRows = await hasLedgerRowsForDate(dateStr);
  return !hasRows; // past dates are immutable once captured
}

function matchesConditions(tokens: Set<string>, conditions: string[]): boolean {
  for (const c of conditions) {
    if (c.startsWith("!")) {
      if (tokens.has(c.slice(1))) return false;
    } else if (!tokens.has(c)) {
      return false;
    }
  }
  return true;
}

async function getDiscoveryV2MatchesByGame(
  gameIds: string[],
): Promise<Map<string, DiscoveryV2Pattern[]>> {
  const out = new Map<string, DiscoveryV2Pattern[]>();
  for (const id of gameIds) out.set(id, []);
  if (gameIds.length === 0) return out;

  const inClause = gameIds.map((id) => `'${sqlEsc(id)}'`).join(",");
  const tokenRows = await prisma.$queryRawUnsafe<Array<{ gameId: string; tokens: string[] }>>(
    `SELECT "gameId", "tokens" FROM "GameFeatureToken" WHERE "gameId" IN (${inClause})`,
  );
  const patterns = await prisma.$queryRawUnsafe<DiscoveryV2Pattern[]>(
    `SELECT "id","outcomeType","conditions","posteriorHitRate","edge","score","n"
     FROM "PatternV2"
     WHERE "status" = 'deployed'
     ORDER BY "score" DESC`,
  );
  const tokensByGame = new Map(tokenRows.map((r) => [r.gameId, new Set(r.tokens ?? [])]));

  for (const p of patterns) {
    for (const gameId of gameIds) {
      const tokens = tokensByGame.get(gameId);
      if (!tokens) continue;
      if (!matchesConditions(tokens, p.conditions ?? [])) continue;
      out.get(gameId)?.push(p);
    }
  }
  for (const gameId of gameIds) {
    const list = out.get(gameId) ?? [];
    list.sort((a, b) => b.score - a.score || b.edge - a.edge);
    out.set(gameId, list.slice(0, 8));
  }
  return out;
}

async function computePlayerContextFallback(
  p: typeof prisma,
  season: number,
  beforeDate: Date,
  homeTeamId: number,
  awayTeamId: number,
  teamIdToAllIds: Map<number, number[]>,
): Promise<{
  homeTopScorer: { id: number; name: string; stat: number } | null;
  homeTopRebounder: { id: number; name: string; stat: number } | null;
  homeTopPlaymaker: { id: number; name: string; stat: number } | null;
  awayTopScorer: { id: number; name: string; stat: number } | null;
  awayTopRebounder: { id: number; name: string; stat: number } | null;
  awayTopPlaymaker: { id: number; name: string; stat: number } | null;
}> {
  const homeIds = teamIdToAllIds.get(homeTeamId) ?? [homeTeamId];
  const awayIds = teamIdToAllIds.get(awayTeamId) ?? [awayTeamId];

  const getTopForTeam = async (teamIds: number[]): Promise<{ ppg: { id: number; name: string; stat: number } | null; rpg: { id: number; name: string; stat: number } | null; apg: { id: number; name: string; stat: number } | null }> => {
    const stats = await p.playerGameStat.groupBy({
      by: ["playerId", "teamId"],
      where: {
        game: { season, date: { lt: beforeDate }, homeScore: { gt: 0 } },
        teamId: { in: teamIds },
      },
      _sum: { points: true, rebounds: true, assists: true },
      _count: true,
    });

    const playerAggs = stats
      .filter((s) => (s._count ?? 0) >= 5)
      .map((s) => ({
        playerId: s.playerId,
        ppg: ((s._sum?.points ?? 0) / (s._count ?? 1)),
        rpg: ((s._sum?.rebounds ?? 0) / (s._count ?? 1)),
        apg: ((s._sum?.assists ?? 0) / (s._count ?? 1)),
      }));

    if (playerAggs.length === 0) return { ppg: null, rpg: null, apg: null };

    const topPpg = playerAggs.sort((a, b) => b.ppg - a.ppg)[0];
    const topRpg = playerAggs.sort((a, b) => b.rpg - a.rpg)[0];
    const topApg = playerAggs.sort((a, b) => b.apg - a.apg)[0];

    const playerIds = [...new Set([topPpg?.playerId, topRpg?.playerId, topApg?.playerId].filter(Boolean))];
    const players = await p.player.findMany({
      where: { id: { in: playerIds } },
      select: { id: true, firstname: true, lastname: true },
    });
    const nameMap = new Map(players.map((pl) => [pl.id, `${pl.firstname} ${pl.lastname}`]));

    return {
      ppg: topPpg ? { id: topPpg.playerId, name: nameMap.get(topPpg.playerId) ?? `Player ${topPpg.playerId}`, stat: topPpg.ppg } : null,
      rpg: topRpg ? { id: topRpg.playerId, name: nameMap.get(topRpg.playerId) ?? `Player ${topRpg.playerId}`, stat: topRpg.rpg } : null,
      apg: topApg ? { id: topApg.playerId, name: nameMap.get(topApg.playerId) ?? `Player ${topApg.playerId}`, stat: topApg.apg } : null,
    };
  };

  const [home, away] = await Promise.all([getTopForTeam(homeIds), getTopForTeam(awayIds)]);

  return {
    homeTopScorer: home.ppg,
    homeTopRebounder: home.rpg,
    homeTopPlaymaker: home.apg,
    awayTopScorer: away.ppg,
    awayTopRebounder: away.rpg,
    awayTopPlaymaker: away.apg,
  };
}

async function computeTeamSnapshots(
  season: number,
  beforeDate: Date,
  teamIds: number[],
): Promise<Map<number, TeamSnapshot>> {
  const result = new Map<number, TeamSnapshot>();

  // Get team codes for the given IDs (scheduled games may use different IDs than historical data)
  const scheduledTeams = await prisma.team.findMany({
    where: { id: { in: teamIds } },
    select: { id: true, code: true },
  });

  // Build a map of team code -> all team IDs with that code (handles duplicate team entries)
  const teamCodes = [...new Set(scheduledTeams.map((t) => t.code).filter(Boolean))] as string[];
  const allTeamsWithCodes = await prisma.team.findMany({
    where: { code: { in: teamCodes } },
    select: { id: true, code: true },
  });

  // Map scheduled team ID -> all IDs that share its code (for historical lookups)
  const teamIdToAllIds = new Map<number, number[]>();
  for (const scheduled of scheduledTeams) {
    if (!scheduled.code) continue;
    const matchingIds = allTeamsWithCodes
      .filter((t) => t.code === scheduled.code)
      .map((t) => t.id);
    teamIdToAllIds.set(scheduled.id, matchingIds);
  }

  for (const teamId of teamIds) {
    const allMatchingIds = teamIdToAllIds.get(teamId) ?? [teamId];

    const games = await prisma.game.findMany({
      where: {
        season,
        date: { lt: beforeDate },
        OR: [
          { homeTeamId: { in: allMatchingIds } },
          { awayTeamId: { in: allMatchingIds } },
        ],
        homeScore: { gt: 0 },
      },
      orderBy: { date: "desc" },
      take: 82,
      select: {
        id: true,
        date: true,
        homeTeamId: true,
        awayTeamId: true,
        homeScore: true,
        awayScore: true,
      },
    });

    if (games.length < 5) continue;

    let wins = 0, losses = 0, pointsFor = 0, pointsAgainst = 0;
    let streak = 0, lastResult: "W" | "L" | null = null;

    for (let i = 0; i < games.length; i++) {
      const g = games[i];
      const isHome = allMatchingIds.includes(g.homeTeamId);
      const won = isHome ? g.homeScore > g.awayScore : g.awayScore > g.homeScore;
      const pf = isHome ? g.homeScore : g.awayScore;
      const pa = isHome ? g.awayScore : g.homeScore;

      if (won) wins++;
      else losses++;
      pointsFor += pf;
      pointsAgainst += pa;

      if (i === 0) {
        lastResult = won ? "W" : "L";
        streak = 1;
      } else if (i < 10 && lastResult && ((won && lastResult === "W") || (!won && lastResult === "L"))) {
        streak++;
      }
    }

    const gp = games.length;
    result.set(teamId, {
      wins,
      losses,
      ppg: pointsFor / gp,
      oppg: pointsAgainst / gp,
      pace: null,
      rebPg: null,
      astPg: null,
      fg3Pct: null,
      ftPct: null,
      rankOff: null,
      rankDef: null,
      rankPace: null,
      streak: lastResult === "W" ? streak : -streak,
      lastGameDate: games[0]?.date ?? null,
    });
  }

  const allTeamStats = [...result.entries()].map(([id, snap]) => ({
    id,
    ppg: snap.ppg,
    oppg: snap.oppg,
  }));

  allTeamStats.sort((a, b) => b.ppg - a.ppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankOff = i + 1;
  });

  allTeamStats.sort((a, b) => a.oppg - b.oppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankDef = i + 1;
  });

  return result;
}

