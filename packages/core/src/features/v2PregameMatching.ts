import type { GameContext } from "@prisma/client";
import { matchesConditions } from "../patterns/metaModelCore";

export type FeatureBinDef = {
  kind: "quantile" | "fixed";
  labels: string[];
  edges: number[];
};

export type DeployedPatternV2 = {
  id: string;
  outcomeType: string;
  conditions: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
};

export type OddsLite = {
  spreadHome?: number | null;
  totalOver?: number | null;
  mlHome?: number | null;
  mlAway?: number | null;
};

export type PregamePlayerContext = {
  homeTopScorer: { stat: number } | null;
  awayTopScorer: { stat: number } | null;
};

function bucketValue(value: number, def: FeatureBinDef): string {
  for (let i = 0; i < def.edges.length; i++) {
    if (value <= def.edges[i]) return def.labels[i] ?? `B${i + 1}`;
  }
  return def.labels[def.labels.length - 1] ?? `B${def.edges.length + 1}`;
}

export async function loadLatestFeatureBins(
  prisma: {
    $queryRawUnsafe: <T>(query: string) => Promise<T>;
  },
): Promise<Map<string, FeatureBinDef>> {
  const rows = await prisma.$queryRawUnsafe<Array<{ featureName: string; binEdges: unknown }>>(
    `SELECT DISTINCT ON ("featureName") "featureName", "binEdges"
     FROM "FeatureBin"
     ORDER BY "featureName", "createdAt" DESC`,
  );
  const out = new Map<string, FeatureBinDef>();
  for (const row of rows) {
    const parsed = row.binEdges as FeatureBinDef;
    if (!parsed || !Array.isArray(parsed.labels) || !Array.isArray(parsed.edges)) continue;
    out.set(row.featureName, parsed);
  }
  return out;
}

export function buildPregameTokenSet(input: {
  season: number;
  context: GameContext;
  odds: OddsLite | null;
  bins: Map<string, FeatureBinDef>;
  playerContext?: PregamePlayerContext;
}): Set<string> {
  const { season, context, odds, bins, playerContext } = input;
  const tokens = new Set<string>();
  tokens.add(`season:${season}`);

  const spreadHome = odds?.spreadHome ?? null;
  const totalLine = odds?.totalOver ?? null;
  const mlHome = odds?.mlHome ?? null;
  const mlAway = odds?.mlAway ?? null;

  if (spreadHome != null && Number.isFinite(spreadHome)) {
    const absDef = bins.get("spread_abs");
    const homeDef = bins.get("spread_home");
    if (absDef) tokens.add(`spread_abs:${bucketValue(Math.abs(spreadHome), absDef)}`);
    if (homeDef) tokens.add(`spread_home:${bucketValue(spreadHome, homeDef)}`);
  }
  if (totalLine != null && Number.isFinite(totalLine)) {
    const def = bins.get("total_line");
    if (def) tokens.add(`total_line:${bucketValue(totalLine, def)}`);
  }
  if (mlHome != null && Number.isFinite(mlHome)) {
    const def = bins.get("ml_home");
    if (def) tokens.add(`ml_home:${bucketValue(mlHome, def)}`);
  }
  if (mlAway != null && Number.isFinite(mlAway)) {
    const def = bins.get("ml_away");
    if (def) tokens.add(`ml_away:${bucketValue(mlAway, def)}`);
  }

  const addQ = (featureName: string, value: number | null | undefined) => {
    if (value == null || !Number.isFinite(value)) return;
    const def = bins.get(featureName);
    if (!def) return;
    tokens.add(`${featureName}:${bucketValue(value, def)}`);
  };

  const computePlaymakingResilience = (ast: number | null | undefined, oppDefRank: number | null | undefined) => {
    if (ast == null || !Number.isFinite(ast) || ast <= 0) return null;
    const pressure =
      oppDefRank != null && Number.isFinite(oppDefRank)
        ? (31 - oppDefRank) / 30
        : 0.5;
    return ast / (1 + pressure * 0.6);
  };

  // Discovery uses top-scorer share of top-3 scorers, but pregame has only top scorer.
  // Approximate dependency from top scorer share of team PPG with a scaling factor.
  const estimateRoleDependency = (
    topScorerPpg: number | null | undefined,
    teamPpg: number | null | undefined,
  ) => {
    if (
      topScorerPpg == null ||
      !Number.isFinite(topScorerPpg) ||
      topScorerPpg <= 0 ||
      teamPpg == null ||
      !Number.isFinite(teamPpg) ||
      teamPpg <= 0
    ) {
      return null;
    }
    const share = topScorerPpg / teamPpg;
    return Math.max(0.2, Math.min(0.75, share * 1.8));
  };

  addQ("home_oppg", context.homeOppg);
  addQ("away_oppg", context.awayOppg);
  addQ("home_ppg", context.homePpg);
  addQ("away_ppg", context.awayPpg);
  addQ("home_rank_def", context.homeRankDef);
  addQ("away_rank_def", context.awayRankDef);
  addQ("home_rank_off", context.homeRankOff);
  addQ("away_rank_off", context.awayRankOff);
  addQ("home_rest_days", context.homeRestDays);
  addQ("away_rest_days", context.awayRestDays);
  addQ("home_streak", context.homeStreak);
  addQ("away_streak", context.awayStreak);
  addQ("home_net_rating", Number.isFinite(context.homePpg) && Number.isFinite(context.homeOppg) ? context.homePpg - context.homeOppg : null);
  addQ("away_net_rating", Number.isFinite(context.awayPpg) && Number.isFinite(context.awayOppg) ? context.awayPpg - context.awayOppg : null);
  addQ("home_injury_out", context.homeInjuryOutCount);
  addQ("away_injury_out", context.awayInjuryOutCount);
  addQ(
    "injury_out_delta",
    Number.isFinite(context.homeInjuryOutCount ?? null) && Number.isFinite(context.awayInjuryOutCount ?? null)
      ? (context.homeInjuryOutCount ?? 0) - (context.awayInjuryOutCount ?? 0)
      : null,
  );
  addQ("home_injury_questionable", context.homeInjuryQuestionableCount);
  addQ("away_injury_questionable", context.awayInjuryQuestionableCount);
  addQ(
    "injury_questionable_delta",
    Number.isFinite(context.homeInjuryQuestionableCount ?? null) && Number.isFinite(context.awayInjuryQuestionableCount ?? null)
      ? (context.homeInjuryQuestionableCount ?? 0) - (context.awayInjuryQuestionableCount ?? 0)
      : null,
  );
  addQ("home_lineup_certainty", context.homeLineupCertainty);
  addQ("away_lineup_certainty", context.awayLineupCertainty);
  addQ(
    "lineup_certainty_delta",
    Number.isFinite(context.homeLineupCertainty ?? null) && Number.isFinite(context.awayLineupCertainty ?? null)
      ? (context.homeLineupCertainty ?? 0) - (context.awayLineupCertainty ?? 0)
      : null,
  );
  addQ("home_late_scratch_risk", context.homeLateScratchRisk);
  addQ("away_late_scratch_risk", context.awayLateScratchRisk);
  addQ(
    "home_playmaking_resilience",
    computePlaymakingResilience(context.homeAstPg, context.awayRankDef),
  );
  addQ(
    "away_playmaking_resilience",
    computePlaymakingResilience(context.awayAstPg, context.homeRankDef),
  );
  addQ(
    "home_role_dependency",
    estimateRoleDependency(playerContext?.homeTopScorer?.stat, context.homePpg),
  );
  addQ(
    "away_role_dependency",
    estimateRoleDependency(playerContext?.awayTopScorer?.stat, context.awayPpg),
  );
  addQ(
    "late_scratch_risk_delta",
    Number.isFinite(context.homeLateScratchRisk ?? null) && Number.isFinite(context.awayLateScratchRisk ?? null)
      ? (context.homeLateScratchRisk ?? 0) - (context.awayLateScratchRisk ?? 0)
      : null,
  );
  addQ("home_is_b2b", context.homeIsB2b ? 1 : 0);
  addQ("away_is_b2b", context.awayIsB2b ? 1 : 0);
  tokens.add(`home_is_b2b:${context.homeIsB2b ? "true" : "false"}`);
  tokens.add(`away_is_b2b:${context.awayIsB2b ? "true" : "false"}`);

  const homeGp = (context.homeWins ?? 0) + (context.homeLosses ?? 0);
  const awayGp = (context.awayWins ?? 0) + (context.awayLosses ?? 0);
  const avgGp = (homeGp + awayGp) / 2;
  addQ("season_progress", avgGp <= 20 ? 0 : avgGp <= 60 ? 1 : 2);

  if (context.homePace != null && context.awayPace != null) {
    addQ("pace_interaction", (context.homePace + context.awayPace) / 2);
  }

  if (context.homeRestDays != null && context.awayRestDays != null) {
    addQ("rest_advantage_delta", context.homeRestDays - context.awayRestDays);
  }

  if (context.homeStreak != null && context.awayStreak != null) {
    addQ("streak_delta", context.homeStreak - context.awayStreak);
  }

  addQ("home_fg3_rate", context.homeFg3Pct);
  addQ("away_fg3_rate", context.awayFg3Pct);

  return tokens;
}

export function matchDeployedPatterns(
  tokenSet: Set<string>,
  deployedPatterns: DeployedPatternV2[],
  limit = 8,
): DeployedPatternV2[] {
  const out: DeployedPatternV2[] = [];
  for (const p of deployedPatterns) {
    if (!matchesConditions(tokenSet, p.conditions ?? [])) continue;
    out.push(p);
  }
  out.sort((a, b) => b.score - a.score || b.edge - a.edge);
  return out.slice(0, limit);
}

