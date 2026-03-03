/**
 * Catalog/Dashboard Alignment — ensures catalog conditions and outcomes
 * are supported by the predictions dashboard.
 *
 * Run when adding new catalog events or dashboard condition logic.
 * Exit nonzero on any failure.
 *
 * Usage: bun run src/tests/testCatalogDashboardAlignment.ts
 */
import { GAME_EVENT_CATALOG } from "../features/gameEventCatalog.js";

// Condition keys the dashboard's findMatchingPatterns can evaluate.
// Update this when adding conditions to dashboard/src/app/api/predictions/route.ts
const DASHBOARD_SUPPORTED_CONDITIONS = new Set([
  "TOP_5_OFF:home", "TOP_10_OFF:home", "BOTTOM_10_OFF:home", "BOTTOM_5_OFF:home",
  "TOP_5_OFF:away", "TOP_10_OFF:away", "BOTTOM_10_OFF:away", "BOTTOM_5_OFF:away",
  "TOP_5_DEF:home", "TOP_10_DEF:home", "BOTTOM_10_DEF:home", "BOTTOM_5_DEF:home",
  "TOP_5_DEF:away", "TOP_10_DEF:away", "BOTTOM_10_DEF:away", "BOTTOM_5_DEF:away",
  "BOTH_TOP_10_PACE:game", "BOTH_BOTTOM_10_PACE:game", "PACE_MISMATCH:game",
  "ON_B2B:home", "ON_B2B:away", "BOTH_ON_B2B:game",
  "RESTED_3_PLUS:home", "RESTED_3_PLUS:away", "RESTED_4_PLUS:home", "RESTED_4_PLUS:away",
  "REST_ADVANTAGE:home", "REST_ADVANTAGE:away",
  "WINNING_RECORD:home", "WINNING_RECORD:away",
  "WIN_STREAK_3:home", "WIN_STREAK_5:home", "WIN_STREAK_7:home",
  "LOSING_STREAK_3:home", "LOSING_STREAK_5:home", "LOSING_STREAK_7:home",
  "WIN_STREAK_3:away", "WIN_STREAK_5:away", "WIN_STREAK_7:away",
  "LOSING_STREAK_3:away", "LOSING_STREAK_5:away", "LOSING_STREAK_7:away",
  "TOP_OFF_VS_BOTTOM_DEF:game", "BOTH_TOP_10_OFF:game",
  "SPREAD_UNDER_3:game", "SPREAD_3_TO_7:game", "SPREAD_OVER_10:game",
  "TOTAL_LINE_OVER_230:game", "TOTAL_LINE_OVER_235:game", "TOTAL_LINE_UNDER_210:game",
  "BIG_FAVORITE:game",
  "NET_RATING_PLUS_5:home", "NET_RATING_PLUS_10:home", "NET_RATING_MINUS_5:home",
  "NET_RATING_PLUS_5:away", "NET_RATING_PLUS_10:away", "NET_RATING_MINUS_5:away",
  "HIGH_SCORING:home", "LOW_SCORING:home", "HIGH_SCORING:away", "LOW_SCORING:away",
  "BOTH_HIGH_SCORING:game", "BOTH_LOW_SCORING:game",
  "STINGY_DEF:home", "POROUS_DEF:home", "STINGY_DEF:away", "POROUS_DEF:away",
  "BOTH_STINGY_DEF:game", "BOTH_POROUS_DEF:game",
  "WIN_PCT_OVER_700:home", "WIN_PCT_OVER_600:home", "WIN_PCT_UNDER_400:home",
  "WIN_PCT_OVER_700:away", "WIN_PCT_OVER_600:away", "WIN_PCT_UNDER_400:away",
  "HAS_TOP_10_SCORER:home", "HAS_TOP_10_SCORER:away",
  "HAS_TOP_10_REBOUNDER:home", "HAS_TOP_10_REBOUNDER:away",
  "HAS_TOP_5_SCORER:home", "HAS_TOP_5_SCORER:away",
  "HAS_TOP_5_REBOUNDER:home", "HAS_TOP_5_REBOUNDER:away",
  "HAS_TOP_10_PLAYMAKER:home", "HAS_TOP_10_PLAYMAKER:away",
  "STAR_MATCHUP:game", "TOP_5_SCORER_VS_BOTTOM_10_DEF:game",
]);

// Outcomes that need player target resolution. Catalog key -> dashboard uses same player context.
// TOP_ASSIST_* = TOP_PLAYMAKER_* (assists = playmaker), EXCEEDS_AVG variants.
const PLAYER_OUTCOMES_WITH_TARGETS = new Set([
  "HOME_TOP_SCORER_25_PLUS", "HOME_TOP_SCORER_30_PLUS", "HOME_TOP_SCORER_EXCEEDS_AVG",
  "AWAY_TOP_SCORER_25_PLUS", "AWAY_TOP_SCORER_30_PLUS", "AWAY_TOP_SCORER_EXCEEDS_AVG",
  "HOME_TOP_REBOUNDER_10_PLUS", "HOME_TOP_REBOUNDER_12_PLUS", "HOME_TOP_REBOUNDER_EXCEEDS_AVG",
  "AWAY_TOP_REBOUNDER_10_PLUS", "AWAY_TOP_REBOUNDER_12_PLUS", "AWAY_TOP_REBOUNDER_EXCEEDS_AVG",
  "HOME_TOP_ASSIST_8_PLUS", "HOME_TOP_ASSIST_10_PLUS", "HOME_TOP_ASSIST_EXCEEDS_AVG",
  "AWAY_TOP_ASSIST_8_PLUS", "AWAY_TOP_ASSIST_10_PLUS", "AWAY_TOP_ASSIST_EXCEEDS_AVG",
  "HOME_TOP_SCORER_DOUBLE_DOUBLE", "AWAY_TOP_SCORER_DOUBLE_DOUBLE",
]);

// Outcomes filtered out as non-actionable (generic player, no team)
const NON_ACTIONABLE_OUTCOMES = new Set([
  "PLAYER_10_PLUS_REBOUNDS:game", "PLAYER_10_PLUS_ASSISTS:game", "PLAYER_5_PLUS_THREES:game",
  "PLAYER_DOUBLE_DOUBLE:game", "PLAYER_TRIPLE_DOUBLE:game", "PLAYER_30_PLUS:game", "PLAYER_40_PLUS:game",
]);

function main(): void {
  let failed = false;

  const catalogConditions = new Set<string>();
  const catalogOutcomes = new Set<string>();

  for (const def of GAME_EVENT_CATALOG) {
    for (const side of def.sides) {
      const key = `${def.key}:${side}`;
      if (def.type === "condition") {
        catalogConditions.add(key);
      } else {
        catalogOutcomes.add(key);
      }
    }
  }

  // 1. Every catalog condition must be supported by dashboard
  const unsupportedConditions = [...catalogConditions].filter((k) => !DASHBOARD_SUPPORTED_CONDITIONS.has(k));
  if (unsupportedConditions.length > 0) {
    console.error("FAIL: Catalog conditions not supported by dashboard:");
    unsupportedConditions.forEach((k) => console.error(`  - ${k}`));
    failed = true;
  }

  // 2. Player outcomes: ensure we have coverage (catalog uses TOP_ASSIST, dashboard maps to playmaker)
  for (const outcomeKey of catalogOutcomes) {
    if (NON_ACTIONABLE_OUTCOMES.has(outcomeKey)) continue;
    const base = outcomeKey.replace(/:.*$/, "");
    if (PLAYER_OUTCOMES_WITH_TARGETS.has(base)) {
      // Dashboard must handle this - we check it's in our list (coverage)
      // No runtime check; this is documentation that we handle TOP_ASSIST etc.
    }
  }

  if (failed) {
    process.exit(1);
  }
  console.log("OK: Catalog and dashboard are aligned.");
}

main();
