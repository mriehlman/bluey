import type { GameContext } from "@prisma/client";
import { matchesConditions } from "../patterns/metaModelCore";

export type FeatureBinDef = {
  kind: "quantile" | "fixed" | "hybrid";
  labels: string[];
  edges: number[];
  hybridLabels?: string[];
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

function bucketValue(value: number | string, def: FeatureBinDef): string {
  if (typeof value === "string") {
    if (def.kind === "hybrid") {
      if ((def.hybridLabels ?? []).includes(value)) return value;
      if (def.labels.includes(value)) return value;
    }
    if (def.kind === "fixed" && def.labels.includes(value)) return value;
    return value;
  }
  if (!Number.isFinite(value)) return def.labels[0] ?? "UNKNOWN";
  if (def.edges.length === 0) return def.labels[0] ?? "UNKNOWN";
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

  const addToken = (featureName: string, value: number | string | null | undefined) => {
    if (value == null) return;
    if (typeof value === "number" && !Number.isFinite(value)) return;
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

  const rankToStrength = (rank: number | null | undefined): number | null => {
    if (rank == null || !Number.isFinite(rank)) return null;
    return (31 - rank) / 30;
  };

  const semanticLineupCertainty = (v: number | null | undefined): string | null => {
    if (v == null || !Number.isFinite(v)) return null;
    if (v < 0.6) return "LOW";
    if (v < 0.82) return "MID";
    return "HIGH";
  };

  const semanticRoleDependency = (v: number | null | undefined): string | null => {
    if (v == null || !Number.isFinite(v)) return null;
    if (v < 0.36) return "BALANCED";
    if (v < 0.5) return "STAR_LED";
    return "EXTREME";
  };

  const semanticLineValue = (v: number | null | undefined): string | null => {
    if (v == null || !Number.isFinite(v)) return null;
    if (v < -0.9) return "UNDERPRICED";
    if (v > 0.9) return "OVERPRICED";
    return "FAIR";
  };

  const seasonPhase = () => {
    const homeGp = (context.homeWins ?? 0) + (context.homeLosses ?? 0);
    const awayGp = (context.awayWins ?? 0) + (context.awayLosses ?? 0);
    const avgGp = (homeGp + awayGp) / 2;
    if (avgGp <= 20) return "EARLY";
    if (avgGp <= 60) return "MID";
    return "LATE";
  };

  const weightedInjuryLoad = (
    outCount: number | null | undefined,
    doubtfulCount: number | null | undefined,
    questionableCount: number | null | undefined,
  ): number => (outCount ?? 0) + (doubtfulCount ?? 0) * 0.9 + (questionableCount ?? 0) * 0.55;

  addToken("home_oppg", context.homeOppg);
  addToken("away_oppg", context.awayOppg);
  addToken("home_ppg", context.homePpg);
  addToken("away_ppg", context.awayPpg);
  addToken("home_rank_def", context.homeRankDef);
  addToken("away_rank_def", context.awayRankDef);
  addToken("home_rank_off", context.homeRankOff);
  addToken("away_rank_off", context.awayRankOff);
  addToken("home_rest_days", context.homeRestDays);
  addToken("away_rest_days", context.awayRestDays);
  addToken("home_streak", context.homeStreak);
  addToken("away_streak", context.awayStreak);
  const homeNetRating = Number.isFinite(context.homePpg) && Number.isFinite(context.homeOppg) ? context.homePpg - context.homeOppg : null;
  const awayNetRating = Number.isFinite(context.awayPpg) && Number.isFinite(context.awayOppg) ? context.awayPpg - context.awayOppg : null;
  addToken("home_net_rating", homeNetRating);
  addToken("away_net_rating", awayNetRating);
  if (homeNetRating != null && awayNetRating != null) {
    addToken("net_rating_delta", homeNetRating - awayNetRating);
  }
  addToken("home_injury_out", context.homeInjuryOutCount);
  addToken("away_injury_out", context.awayInjuryOutCount);
  addToken(
    "injury_out_delta",
    Number.isFinite(context.homeInjuryOutCount ?? null) && Number.isFinite(context.awayInjuryOutCount ?? null)
      ? (context.homeInjuryOutCount ?? 0) - (context.awayInjuryOutCount ?? 0)
      : null,
  );
  addToken("home_injury_questionable", context.homeInjuryQuestionableCount);
  addToken("away_injury_questionable", context.awayInjuryQuestionableCount);
  addToken(
    "injury_questionable_delta",
    Number.isFinite(context.homeInjuryQuestionableCount ?? null) && Number.isFinite(context.awayInjuryQuestionableCount ?? null)
      ? (context.homeInjuryQuestionableCount ?? 0) - (context.awayInjuryQuestionableCount ?? 0)
      : null,
  );
  addToken("home_lineup_certainty", context.homeLineupCertainty);
  addToken("away_lineup_certainty", context.awayLineupCertainty);
  addToken(
    "lineup_certainty_delta",
    Number.isFinite(context.homeLineupCertainty ?? null) && Number.isFinite(context.awayLineupCertainty ?? null)
      ? (context.homeLineupCertainty ?? 0) - (context.awayLineupCertainty ?? 0)
      : null,
  );
  addToken("home_late_scratch_risk", context.homeLateScratchRisk);
  addToken("away_late_scratch_risk", context.awayLateScratchRisk);
  addToken(
    "home_playmaking_resilience",
    computePlaymakingResilience(context.homeAstPg, context.awayRankDef),
  );
  addToken(
    "away_playmaking_resilience",
    computePlaymakingResilience(context.awayAstPg, context.homeRankDef),
  );
  const homePlaymakingResilience = computePlaymakingResilience(context.homeAstPg, context.awayRankDef);
  const awayPlaymakingResilience = computePlaymakingResilience(context.awayAstPg, context.homeRankDef);
  if (homePlaymakingResilience != null && awayPlaymakingResilience != null) {
    addToken("playmaking_resilience_delta", homePlaymakingResilience - awayPlaymakingResilience);
  }
  const homeCreationBurden =
    playerContext?.homeTopScorer?.stat != null && context.homeAstPg != null && context.homeAstPg > 0
      ? playerContext.homeTopScorer.stat / context.homeAstPg
      : null;
  const awayCreationBurden =
    playerContext?.awayTopScorer?.stat != null && context.awayAstPg != null && context.awayAstPg > 0
      ? playerContext.awayTopScorer.stat / context.awayAstPg
      : null;
  addToken("home_creation_burden", homeCreationBurden);
  addToken("away_creation_burden", awayCreationBurden);
  if (homeCreationBurden != null && awayCreationBurden != null) {
    addToken("creation_burden_delta", homeCreationBurden - awayCreationBurden);
  }
  const homeRoleDependency = estimateRoleDependency(playerContext?.homeTopScorer?.stat, context.homePpg);
  const awayRoleDependency = estimateRoleDependency(playerContext?.awayTopScorer?.stat, context.awayPpg);
  addToken(
    "home_role_dependency",
    homeRoleDependency,
  );
  addToken(
    "away_role_dependency",
    awayRoleDependency,
  );
  if (homeRoleDependency != null && awayRoleDependency != null) {
    addToken("role_dependency_delta", homeRoleDependency - awayRoleDependency);
  }
  addToken(
    "late_scratch_risk_delta",
    Number.isFinite(context.homeLateScratchRisk ?? null) && Number.isFinite(context.awayLateScratchRisk ?? null)
      ? (context.homeLateScratchRisk ?? 0) - (context.awayLateScratchRisk ?? 0)
      : null,
  );
  addToken("home_is_b2b", context.homeIsB2b ? 1 : 0);
  addToken("away_is_b2b", context.awayIsB2b ? 1 : 0);
  tokens.add(`home_is_b2b:${context.homeIsB2b ? "true" : "false"}`);
  tokens.add(`away_is_b2b:${context.awayIsB2b ? "true" : "false"}`);

  const homeGp = (context.homeWins ?? 0) + (context.homeLosses ?? 0);
  const awayGp = (context.awayWins ?? 0) + (context.awayLosses ?? 0);
  const avgGp = (homeGp + awayGp) / 2;
  addToken("season_progress", avgGp <= 20 ? 0 : avgGp <= 60 ? 1 : 2);

  if (context.homePace != null && context.awayPace != null) {
    addToken("pace_interaction", (context.homePace + context.awayPace) / 2);
  }

  if (context.homeRestDays != null && context.awayRestDays != null) {
    addToken("rest_advantage_delta", context.homeRestDays - context.awayRestDays);
  }

  if (context.homeStreak != null && context.awayStreak != null) {
    addToken("streak_delta", context.homeStreak - context.awayStreak);
  }

  addToken("home_fg3_rate", context.homeFg3Pct);
  addToken("away_fg3_rate", context.awayFg3Pct);
  if (context.homeFg3Pct != null && context.awayFg3Pct != null) {
    addToken("fg3_rate_delta", context.homeFg3Pct - context.awayFg3Pct);
  }

  const offenseVsOppDef = (() => {
    const homeOff = rankToStrength(context.homeRankOff);
    const awayDef = rankToStrength(context.awayRankDef);
    const awayOff = rankToStrength(context.awayRankOff);
    const homeDef = rankToStrength(context.homeRankDef);
    if (homeOff == null || awayDef == null || awayOff == null || homeDef == null) return null;
    return (homeOff - awayDef) - (awayOff - homeDef);
  })();
  addToken("offense_vs_opp_def", offenseVsOppDef);

  const starDepPressure = (() => {
    const homePressure = rankToStrength(context.awayRankDef);
    const awayPressure = rankToStrength(context.homeRankDef);
    if (homeRoleDependency == null || awayRoleDependency == null || homePressure == null || awayPressure == null) {
      return null;
    }
    return homeRoleDependency * homePressure - awayRoleDependency * awayPressure;
  })();
  addToken("star_dependency_under_pressure", starDepPressure);

  const injuryFragility = (() => {
    if (
      homeRoleDependency == null ||
      awayRoleDependency == null ||
      context.homeLineupCertainty == null ||
      context.awayLineupCertainty == null
    ) return null;
    const homeLoad = weightedInjuryLoad(
      context.homeInjuryOutCount,
      context.homeInjuryDoubtfulCount,
      context.homeInjuryQuestionableCount,
    );
    const awayLoad = weightedInjuryLoad(
      context.awayInjuryOutCount,
      context.awayInjuryDoubtfulCount,
      context.awayInjuryQuestionableCount,
    );
    const homeFragility = homeLoad + homeRoleDependency * 2 - context.homeLineupCertainty * 1.25;
    const awayFragility = awayLoad + awayRoleDependency * 2 - context.awayLineupCertainty * 1.25;
    return homeFragility - awayFragility;
  })();
  addToken("injury_fragility", injuryFragility);

  const shotCreationStability = (() => {
    if (
      homePlaymakingResilience == null ||
      awayPlaymakingResilience == null ||
      homeCreationBurden == null ||
      awayCreationBurden == null
    ) return null;
    return (homePlaymakingResilience - homeCreationBurden) - (awayPlaymakingResilience - awayCreationBurden);
  })();
  addToken("shot_creation_stability", shotCreationStability);

  const marketOverpricingRisk = (() => {
    if (spreadHome == null || homeCreationBurden == null || awayCreationBurden == null) return null;
    return (homeCreationBurden - awayCreationBurden) + (-spreadHome * 0.15);
  })();
  addToken("market_overpricing_risk", marketOverpricingRisk);

  if (context.homeLineupCertainty != null && context.awayLineupCertainty != null) {
    addToken("lineup_certainty", semanticLineupCertainty(Math.min(context.homeLineupCertainty, context.awayLineupCertainty)));
  }
  if (homeRoleDependency != null && awayRoleDependency != null) {
    addToken("role_dependency_band", semanticRoleDependency(Math.max(homeRoleDependency, awayRoleDependency)));
  }
  addToken("line_value_state", semanticLineValue(marketOverpricingRisk));

  const injuryNoiseScore =
    (context.homeInjuryQuestionableCount ?? 0) +
    (context.awayInjuryQuestionableCount ?? 0) +
    (context.homeInjuryDoubtfulCount ?? 0) +
    (context.awayInjuryDoubtfulCount ?? 0) +
    ((context.homeLateScratchRisk ?? 0) + (context.awayLateScratchRisk ?? 0)) * 2;
  addToken("injury_noise", injuryNoiseScore < 1.5 ? "LOW" : injuryNoiseScore < 3.5 ? "MID" : "HIGH");

  const volatilityScore =
    Math.abs((context.homeStreak ?? 0) - (context.awayStreak ?? 0)) * 0.2 +
    Math.abs((context.homeLineupCertainty ?? 0.75) - (context.awayLineupCertainty ?? 0.75)) * 2;
  addToken("team_form_volatility", volatilityScore < 0.9 ? "LOW" : volatilityScore < 1.8 ? "MID" : "HIGH");

  const completenessChecks = [
    context.homePpg,
    context.awayPpg,
    context.homeRankOff,
    context.awayRankOff,
    context.homeRankDef,
    context.awayRankDef,
    context.homeLineupCertainty,
    context.awayLineupCertainty,
  ];
  const available = completenessChecks.filter((x) => x != null && Number.isFinite(x)).length;
  addToken("data_completeness", available >= 8 ? "HIGH" : available >= 6 ? "MID" : "LOW");

  if (totalLine != null && Number.isFinite(totalLine)) {
    addToken("market_total_band", totalLine < 216 ? "LOW" : totalLine <= 232 ? "MID" : "HIGH");
  }
  if (spreadHome != null && Number.isFinite(spreadHome)) {
    const abs = Math.abs(spreadHome);
    addToken("spread_band", abs <= 3 ? "CLOSE" : abs <= 8 ? "MEDIUM" : "WIDE");
  }
  addToken("season_phase", seasonPhase());
  const injuryEnvironmentLoad =
    weightedInjuryLoad(context.homeInjuryOutCount, context.homeInjuryDoubtfulCount, context.homeInjuryQuestionableCount) +
    weightedInjuryLoad(context.awayInjuryOutCount, context.awayInjuryDoubtfulCount, context.awayInjuryQuestionableCount);
  addToken("injury_environment", injuryEnvironmentLoad <= 1.2 ? "CLEAN" : injuryEnvironmentLoad <= 3.6 ? "MIXED" : "CHAOTIC");

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

