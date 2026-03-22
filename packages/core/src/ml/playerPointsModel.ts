// @ts-ignore Runtime provides Node child_process; this package may not include node type defs.
import { spawn } from "child_process";
// @ts-ignore Runtime provides Node fs; this package may not include node type defs.
import { writeFileSync, unlinkSync } from "fs";
// @ts-ignore Runtime provides Node path; this package may not include node type defs.
import { join } from "path";
// @ts-ignore Runtime provides Node os; this package may not include node type defs.
import { tmpdir } from "os";
import { prisma } from "@bluey/db";

type CliFlags = Record<string, string>;

interface RawDatasetRow {
  game_id: string;
  player_id: number;
  date: Date | string;
  season: number;
  actual_points: number;
  line: number;
  target_over_hit: number;
  ppg: number | null;
  mpg: number | null;
  last5Ppg: number | null;
  rankPpg: number | null;
  oppRankDef: number | null;
  points_last_3_avg: number | null;
  points_last_5_avg: number | null;
  points_last_10_avg: number | null;
  points_std_5: number | null;
  points_std_10: number | null;
  minutes_last_3_avg: number | null;
  minutes_last_5_avg: number | null;
  minutes_last_10_avg: number | null;
  minutes_std_5: number | null;
  minutes_std_10: number | null;
  points_per_minute_last_5: number | null;
  fga_last_3_avg: number | null;
  fga_last_5_avg: number | null;
  fga_last_10_avg: number | null;
  fga_per_minute_last_5: number | null;
  starter_last_5_rate: number | null;
  history_games_10: number | null;
  line_minus_points_last_3_avg: number | null;
  line_minus_points_last_5_avg: number | null;
  line_minus_points_last_10_avg: number | null;
  line_minus_expected_points_per_minute: number | null;
  line_points_z_10: number | null;
  low_minutes_stability_flag: number | null;
  low_points_stability_flag: number | null;
  bench_risk_flag: number | null;
  low_recent_sample_flag: number | null;
  homeRankOff: number | null;
  awayRankOff: number | null;
  homeRankDef: number | null;
  awayRankDef: number | null;
  pace: number | null;
  homeRestDays: number | null;
  awayRestDays: number | null;
  homeIsB2b: boolean | null;
  awayIsB2b: boolean | null;
  homeLineupCertainty: number | null;
  awayLineupCertainty: number | null;
  homeInjuryOutCount: number | null;
  awayInjuryOutCount: number | null;
  spread_home: number | null;
  total_over: number | null;
  player_lineup_certainty: number | null;
  player_late_scratch_risk: number | null;
}

interface ModelInputRow {
  game_id: string;
  player_id: number;
  date: string;
  season: number;
  actual_points: number;
  line: number;
  target_over_hit: number;
  ppg: number | null;
  mpg: number | null;
  last5Ppg: number | null;
  rankPpg: number | null;
  oppRankDef: number | null;
  points_last_3_avg: number | null;
  points_last_5_avg: number | null;
  points_last_10_avg: number | null;
  points_std_5: number | null;
  points_std_10: number | null;
  minutes_last_3_avg: number | null;
  minutes_last_5_avg: number | null;
  minutes_last_10_avg: number | null;
  minutes_std_5: number | null;
  minutes_std_10: number | null;
  points_per_minute_last_5: number | null;
  fga_last_3_avg: number | null;
  fga_last_5_avg: number | null;
  fga_last_10_avg: number | null;
  fga_per_minute_last_5: number | null;
  starter_last_5_rate: number | null;
  history_games_10: number | null;
  line_minus_points_last_3_avg: number | null;
  line_minus_points_last_5_avg: number | null;
  line_minus_points_last_10_avg: number | null;
  line_minus_expected_points_per_minute: number | null;
  line_points_z_10: number | null;
  low_minutes_stability_flag: number | null;
  low_points_stability_flag: number | null;
  bench_risk_flag: number | null;
  low_recent_sample_flag: number | null;
  homeRankOff: number | null;
  awayRankOff: number | null;
  homeRankDef: number | null;
  awayRankDef: number | null;
  pace: number | null;
  homeRestDays: number | null;
  awayRestDays: number | null;
  homeIsB2b: number | null;
  awayIsB2b: number | null;
  homeLineupCertainty: number | null;
  awayLineupCertainty: number | null;
  homeInjuryOutCount: number | null;
  awayInjuryOutCount: number | null;
  spread_home: number | null;
  total_over: number | null;
  player_lineup_certainty: number | null;
  player_late_scratch_risk: number | null;
}

interface PythonMetric {
  accuracy: number | null;
  count: number;
}

interface VariantRunResult {
  variant: string;
  feature_count: number;
  metrics: {
    overall_accuracy: number | null;
    top20_confidence_accuracy: PythonMetric;
    top10_confidence_accuracy: PythonMetric;
    over_p_gt_0_65: PythonMetric;
    under_p_lt_0_35: PythonMetric;
    combined_strategy: PythonMetric;
  };
  threshold_search: {
    validation_window: {
      from_date: string | null;
      to_date: string | null;
      rows: number;
    };
    holdout_window: {
      from_date: string | null;
      to_date: string | null;
      rows: number;
    };
    best_thresholds: {
      over: number;
      under: number;
      validation_accuracy: number | null;
      validation_count: number;
    };
    holdout_metrics: {
      over_picks: PythonMetric;
      under_picks: PythonMetric;
      combined: PythonMetric;
    };
  };
}

interface PythonResult {
  dataset_size: number;
  train_size: number;
  test_size: number;
  class_balance: {
    over_count: number;
    under_count: number;
    over_rate: number | null;
  };
  variants: VariantRunResult[];
  conclusions: {
    under_improved_group: string;
    over_improved_group: string;
    selective_hurt_group: string;
    best_variant_for_hit_rate: string;
  };
  post_prediction_filtering: {
    baseline_variant: string;
    filter_thresholds: {
      minutes_std_p75: number | null;
      points_std_p75: number | null;
      line_p10: number | null;
      line_p90: number | null;
    };
    strategies: Array<{
      strategy: string;
      selection: string;
      filter_mode: string;
      filters_applied: string[];
      filter_count: number;
      picks: number;
      hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
    }>;
    single_filter_impact: {
      top10_conf: Array<{
        strategy: string;
        picks: number;
        hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
      }>;
      top20_conf: Array<{
        strategy: string;
        picks: number;
        hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
      }>;
    };
    leave_one_out: {
      top10_conf: Array<{
        strategy: string;
        picks: number;
        hit_rate: number | null;
        delta_vs_combined: number | null;
      }>;
      top20_conf: Array<{
        strategy: string;
        picks: number;
        hit_rate: number | null;
        delta_vs_combined: number | null;
      }>;
    };
    filter_importance: {
      largest_hit_rate_gain_alone: string;
      largest_under_gain_alone: string;
      biggest_drop_when_removed_from_combined: string;
      over_pruning_candidate: string;
    };
    best_strategy: {
      strategy: string;
      selection: string;
      filter_mode: string;
      filters_applied: string[];
      filter_count: number;
      picks: number;
      hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
    } | null;
    under_best_filter: {
      strategy: string;
      under_hit_rate: number | null;
      under_picks: number;
    } | null;
    best_reduced_strategy: {
      strategy: string;
      selection: string;
      filter_mode: string;
      filters_applied: string[];
      filter_count: number;
      picks: number;
      hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
      reference_best_strategy: string;
      reference_best_hit_rate: number | null;
      accuracy_delta_vs_best: number | null;
    } | null;
    confidence_tiers: {
      strategy: string;
      cumulative: Array<{
        tier: string;
        picks: number;
        hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
      }>;
      bands: Array<{
        tier: string;
        picks: number;
        hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
      }>;
      thresholds_crossings: {
        below_65_pct_at_tier: string | null;
        below_63_pct_at_tier: string | null;
        below_60_pct_at_tier: string | null;
      };
    } | null;
    rolling_window_stability: {
      strategy: string;
      window_size: number;
      stride: number;
      windows: Array<{
        start_index: number;
        end_index: number;
        picks: number;
        hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
      }>;
      summary: {
        average_hit_rate: number | null;
        min_hit_rate: number | null;
        max_hit_rate: number | null;
        hit_rate_stddev: number | null;
      };
      below_threshold_counts: {
        below_65_pct: number;
        below_63_pct: number;
        below_60_pct: number;
      };
    } | null;
    rolling_strategy_comparison: {
      window_size: number;
      stride: number;
      under_only_threshold: number;
      strategies: Array<{
        strategy: string;
        average_hit_rate: number | null;
        min_hit_rate: number | null;
        max_hit_rate: number | null;
        hit_rate_stddev: number | null;
        total_picks: number;
      }>;
      conclusions: {
        most_stable_strategy: string;
        best_accuracy_strategy: string;
        best_balance_strategy: string;
      };
    } | null;
    final_selection_strategy: {
      under_only_threshold: number;
      core_strategy: string;
      combined_strategy: string;
      strategies: Array<{
        strategy: string;
        total_picks: number;
        overall_hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
    } | null;
    controlled_under_addon_experiment: {
      benchmark_strategy: string;
      benchmark_metrics: {
        total_picks: number;
        overall_hit_rate: number | null;
        under_picks: number;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      };
      variants: Array<{
        variant: string;
        under_threshold: number;
        confidence_gate: string;
        daily_cap: string;
        total_picks: number;
        added_picks: number;
        overall_hit_rate: number | null;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
      frontier_sorted: Array<{
        variant: string;
        under_threshold: number;
        confidence_gate: string;
        daily_cap: string;
        total_picks: number;
        added_picks: number;
        overall_hit_rate: number | null;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
      constraints_passed: Array<{
        variant: string;
        under_threshold: number;
        confidence_gate: string;
        daily_cap: string;
        total_picks: number;
        added_picks: number;
        overall_hit_rate: number | null;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
      recommendation: {
        best_conservative_addon: string;
        best_balanced_addon: string;
        benchmark_beats_all_addons: boolean;
      };
    } | null;
    final_strategy_comparison: {
      ranking_priority: string;
      top3_addons: Array<{
        variant: string;
        total_picks: number;
        added_picks: number;
        overall_hit_rate: number | null;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
      comparison_rows: Array<{
        label: string;
        variant: string;
        total_picks: number;
        added_picks: number;
        overall_hit_rate: number | null;
        under_hit_rate: number | null;
        rolling_avg_hit_rate: number | null;
        rolling_min_hit_rate: number | null;
        rolling_max_hit_rate: number | null;
        rolling_hit_rate_stddev: number | null;
      }>;
      block_performance: {
        early: Array<{ label: string; picks: number; hit_rate: number | null }>;
        middle: Array<{ label: string; picks: number; hit_rate: number | null }>;
        late: Array<{ label: string; picks: number; hit_rate: number | null }>;
      };
      recommendation: {
        default_mode: string;
        conservative_mode: string;
        keep_benchmark_as_fallback: boolean;
      };
    } | null;
  };
}

function parseFlags(args: string[]): CliFlags {
  const flags: CliFlags = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--") continue;
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const maybeValue = args[i + 1];
    if (!maybeValue || maybeValue.startsWith("--")) {
      flags[key] = "true";
      continue;
    }
    flags[key] = maybeValue;
    i++;
  }
  return flags;
}

async function buildDatasetRows(): Promise<ModelInputRow[]> {
  const rows = await prisma.$queryRawUnsafe<RawDatasetRow[]>(`
    WITH prop_best AS (
      SELECT DISTINCT ON (p."gameId", p."playerId")
        p."gameId",
        p."playerId",
        p.line
      FROM "PlayerPropOdds" p
      WHERE p.market = 'player_points'
        AND p.line IS NOT NULL
      ORDER BY
        p."gameId",
        p."playerId",
        CASE WHEN p.source = 'consensus' THEN 0 ELSE 1 END,
        p."fetchedAt" DESC,
        p.id DESC
    ),
    player_hist_source AS (
      SELECT
        s."gameId",
        s."playerId",
        g.date,
        CASE WHEN s.points BETWEEN 0 AND 100 THEN s.points::float ELSE NULL END AS points_clean,
        CASE WHEN s.minutes BETWEEN 0 AND 60 THEN s.minutes::float ELSE NULL END AS minutes_clean,
        CASE WHEN s.fga IS NOT NULL AND s.fga BETWEEN 0 AND 60 THEN s.fga::float ELSE NULL END AS fga_clean,
        COALESCE(s.starter, false) AS starter_clean
      FROM "PlayerGameStat" s
      JOIN "Game" g ON g.id = s."gameId"
    ),
    player_hist AS (
      SELECT
        s."gameId",
        s."playerId",
        AVG(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS points_last_3_avg,
        AVG(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS points_last_5_avg,
        AVG(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS points_last_10_avg,
        STDDEV_SAMP(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS points_std_5,
        STDDEV_SAMP(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS points_std_10,
        AVG(s.minutes_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS minutes_last_3_avg,
        AVG(s.minutes_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS minutes_last_5_avg,
        AVG(s.minutes_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS minutes_last_10_avg,
        STDDEV_SAMP(s.minutes_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS minutes_std_5,
        STDDEV_SAMP(s.minutes_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS minutes_std_10,
        SUM(s.points_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) / NULLIF(
          SUM(s.minutes_clean) OVER (
            PARTITION BY s."playerId"
            ORDER BY g.date, s."gameId"
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
          ),
          0
        ) AS points_per_minute_last_5,
        AVG(s.fga_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS fga_last_3_avg,
        AVG(s.fga_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS fga_last_5_avg,
        AVG(s.fga_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS fga_last_10_avg,
        SUM(s.fga_clean) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) / NULLIF(
          SUM(s.minutes_clean) OVER (
            PARTITION BY s."playerId"
            ORDER BY g.date, s."gameId"
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
          ),
          0
        ) AS fga_per_minute_last_5,
        AVG(CASE WHEN s.starter_clean THEN 1.0 ELSE 0.0 END) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS starter_last_5_rate,
        COUNT(*) OVER (
          PARTITION BY s."playerId"
          ORDER BY g.date, s."gameId"
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        )::int AS history_games_10
      FROM player_hist_source s
      JOIN "Game" g ON g.id = s."gameId"
    )
    SELECT
      p."gameId" AS game_id,
      p."playerId" AS player_id,
      g.date AS date,
      g.season AS season,
      s.points AS actual_points,
      p.line AS line,
      CASE WHEN s.points > p.line THEN 1 ELSE 0 END AS target_over_hit,

      pgc.ppg AS ppg,
      pgc.mpg AS mpg,
      pgc."last5Ppg" AS "last5Ppg",
      pgc."rankPpg" AS "rankPpg",
      pgc."oppRankDef" AS "oppRankDef",
      ph.points_last_3_avg,
      ph.points_last_5_avg,
      ph.points_last_10_avg,
      ph.points_std_5,
      ph.points_std_10,
      ph.minutes_last_3_avg,
      ph.minutes_last_5_avg,
      ph.minutes_last_10_avg,
      ph.minutes_std_5,
      ph.minutes_std_10,
      ph.points_per_minute_last_5,
      ph.fga_last_3_avg,
      ph.fga_last_5_avg,
      ph.fga_last_10_avg,
      ph.fga_per_minute_last_5,
      ph.starter_last_5_rate,
      ph.history_games_10,
      p.line - ph.points_last_3_avg AS line_minus_points_last_3_avg,
      p.line - ph.points_last_5_avg AS line_minus_points_last_5_avg,
      p.line - ph.points_last_10_avg AS line_minus_points_last_10_avg,
      p.line - (ph.points_per_minute_last_5 * ph.minutes_last_5_avg) AS line_minus_expected_points_per_minute,
      CASE
        WHEN ph.points_std_10 IS NOT NULL AND ph.points_std_10 > 0
          THEN (p.line - ph.points_last_10_avg) / ph.points_std_10
        ELSE NULL
      END AS line_points_z_10,
      CASE WHEN ph.minutes_std_10 IS NOT NULL AND ph.minutes_std_10 >= 6 THEN 1 ELSE 0 END AS low_minutes_stability_flag,
      CASE WHEN ph.points_std_10 IS NOT NULL AND ph.points_std_10 >= 8 THEN 1 ELSE 0 END AS low_points_stability_flag,
      CASE WHEN ph.starter_last_5_rate IS NOT NULL AND ph.starter_last_5_rate < 0.4 THEN 1 ELSE 0 END AS bench_risk_flag,
      CASE WHEN ph.history_games_10 IS NULL OR ph.history_games_10 < 5 THEN 1 ELSE 0 END AS low_recent_sample_flag,

      gc."homeRankOff" AS "homeRankOff",
      gc."awayRankOff" AS "awayRankOff",
      gc."homeRankDef" AS "homeRankDef",
      gc."awayRankDef" AS "awayRankDef",
      CASE
        WHEN gc."homePace" IS NOT NULL AND gc."awayPace" IS NOT NULL THEN (gc."homePace" + gc."awayPace") / 2.0
        WHEN gc."homePace" IS NOT NULL THEN gc."homePace"
        WHEN gc."awayPace" IS NOT NULL THEN gc."awayPace"
        ELSE NULL
      END AS pace,
      gc."homeRestDays" AS "homeRestDays",
      gc."awayRestDays" AS "awayRestDays",
      gc."homeIsB2b" AS "homeIsB2b",
      gc."awayIsB2b" AS "awayIsB2b",
      gc."homeLineupCertainty" AS "homeLineupCertainty",
      gc."awayLineupCertainty" AS "awayLineupCertainty",
      gc."homeInjuryOutCount" AS "homeInjuryOutCount",
      gc."awayInjuryOutCount" AS "awayInjuryOutCount",
      CASE
        WHEN s."teamId" = g."homeTeamId" THEN gc."homeLineupCertainty"
        WHEN s."teamId" = g."awayTeamId" THEN gc."awayLineupCertainty"
        ELSE NULL
      END AS player_lineup_certainty,
      CASE
        WHEN s."teamId" = g."homeTeamId" THEN gc."homeLateScratchRisk"
        WHEN s."teamId" = g."awayTeamId" THEN gc."awayLateScratchRisk"
        ELSE NULL
      END AS player_late_scratch_risk,

      go_best."spreadHome" AS spread_home,
      go_best."totalOver" AS total_over
    FROM prop_best p
    JOIN "PlayerGameStat" s
      ON s."gameId" = p."gameId"
     AND s."playerId" = p."playerId"
    JOIN "Game" g
      ON g.id = p."gameId"
    LEFT JOIN "PlayerGameContext" pgc
      ON pgc."gameId" = p."gameId"
     AND pgc."playerId" = p."playerId"
    LEFT JOIN player_hist ph
      ON ph."gameId" = p."gameId"
     AND ph."playerId" = p."playerId"
    LEFT JOIN "GameContext" gc
      ON gc."gameId" = p."gameId"
    LEFT JOIN LATERAL (
      SELECT go.*
      FROM "GameOdds" go
      WHERE go."gameId" = p."gameId"
      ORDER BY
        CASE WHEN go.source = 'consensus' THEN 0 ELSE 1 END,
        go."fetchedAt" DESC
      LIMIT 1
    ) go_best ON TRUE
    WHERE g.season IN (2023, 2024, 2025)
    ORDER BY g.date ASC, p."gameId", p."playerId";
  `);

  return rows.map((row) => ({
    ...row,
    date: row.date instanceof Date ? row.date.toISOString().slice(0, 10) : String(row.date).slice(0, 10),
    homeIsB2b: row.homeIsB2b == null ? null : row.homeIsB2b ? 1 : 0,
    awayIsB2b: row.awayIsB2b == null ? null : row.awayIsB2b ? 1 : 0,
  }));
}

function runPythonLightGbm(rows: ModelInputRow[], learningRate: number, numEstimators: number): Promise<PythonResult> {
  const pythonScript = `
import json
import sys

try:
    import lightgbm as lgb
except Exception as exc:
    print(json.dumps({
        "error": "python_lightgbm_import_failed",
        "detail": str(exc),
        "hint": "Install Python package: pip install lightgbm"
    }))
    sys.exit(2)

try:
    import numpy as np
except Exception as exc:
    print(json.dumps({
        "error": "python_numpy_import_failed",
        "detail": str(exc),
        "hint": "Install Python package: pip install numpy"
    }))
    sys.exit(2)

payload = json.loads(sys.stdin.read())
rows = payload["rows"]
learning_rate = payload["learning_rate"]
n_estimators = payload["n_estimators"]

train_rows = [r for r in rows if r["season"] in (2023, 2024)]
test_rows = [r for r in rows if r["season"] == 2025]

if len(train_rows) == 0:
    print(json.dumps({
        "error": "no_train_rows",
        "detail": "No rows found for train seasons 2023-2024."
    }))
    sys.exit(3)

if len(test_rows) == 0:
    print(json.dumps({
        "error": "no_test_rows",
        "detail": "No rows found for test season 2025."
    }))
    sys.exit(4)

baseline_features = [
    "ppg", "mpg", "last5Ppg", "rankPpg", "oppRankDef",
    "homeRankOff", "awayRankOff", "homeRankDef", "awayRankDef",
    "pace", "homeRestDays", "awayRestDays", "homeIsB2b", "awayIsB2b",
    "homeLineupCertainty", "awayLineupCertainty", "homeInjuryOutCount", "awayInjuryOutCount",
    "line", "spread_home", "total_over"
]

rolling_features = [
    "points_last_3_avg", "points_last_5_avg", "points_last_10_avg",
    "points_std_5", "points_std_10",
    "minutes_last_3_avg", "minutes_last_5_avg", "minutes_last_10_avg",
    "minutes_std_5", "minutes_std_10",
    "points_per_minute_last_5",
    "fga_last_3_avg", "fga_last_5_avg", "fga_last_10_avg",
    "fga_per_minute_last_5",
    "starter_last_5_rate"
]

line_relative_features = [
    "line_minus_points_last_3_avg",
    "line_minus_points_last_5_avg",
    "line_minus_points_last_10_avg",
    "line_minus_expected_points_per_minute",
    "line_points_z_10"
]

flag_features = [
    "low_minutes_stability_flag",
    "low_points_stability_flag",
    "bench_risk_flag",
    "low_recent_sample_flag"
]

variants = [
    ("baseline_original_only", baseline_features),
    ("baseline_plus_rolling", baseline_features + rolling_features),
    ("baseline_plus_line_relative", baseline_features + line_relative_features),
    ("baseline_plus_stability_role_flags", baseline_features + flag_features),
    ("baseline_plus_all_new", baseline_features + rolling_features + line_relative_features + flag_features)
]

def vec(row, feature_keys):
    vals = []
    for key in feature_keys:
        val = row.get(key)
        if val is None:
            vals.append(float("nan"))
        else:
            vals.append(float(val))
    return vals

def metric_accuracy(p_over, y_true, over_threshold=0.65, under_threshold=0.35):
    preds = [1 if p >= 0.5 else 0 for p in p_over]
    n = len(y_true)
    overall_acc = None
    if n > 0:
        overall_acc = sum(1 for i in range(n) if preds[i] == y_true[i]) / n

    conf = [abs(p - 0.5) for p in p_over]
    idxs_sorted_conf = sorted(range(n), key=lambda i: conf[i], reverse=True)
    top20_n = max(1, int(n * 0.20))
    top10_n = max(1, int(n * 0.10))
    top20 = idxs_sorted_conf[:top20_n]
    top10 = idxs_sorted_conf[:top10_n]

    def acc_for(idxs):
        if len(idxs) == 0:
            return {"accuracy": None, "count": 0}
        good = sum(1 for i in idxs if preds[i] == y_true[i])
        return {"accuracy": good / len(idxs), "count": len(idxs)}

    over_idxs = [i for i, p in enumerate(p_over) if p > over_threshold]
    under_idxs = [i for i, p in enumerate(p_over) if p < under_threshold]
    over_good = sum(1 for i in over_idxs if y_true[i] == 1)
    under_good = sum(1 for i in under_idxs if y_true[i] == 0)
    combined_count = len(over_idxs) + len(under_idxs)
    combined_good = over_good + under_good

    return {
        "overall_accuracy": overall_acc,
        "top20_confidence_accuracy": acc_for(top20),
        "top10_confidence_accuracy": acc_for(top10),
        "over_p_gt_0_65": {
            "accuracy": (over_good / len(over_idxs)) if len(over_idxs) > 0 else None,
            "count": len(over_idxs)
        },
        "under_p_lt_0_35": {
            "accuracy": (under_good / len(under_idxs)) if len(under_idxs) > 0 else None,
            "count": len(under_idxs)
        },
        "combined_strategy": {
            "accuracy": (combined_good / combined_count) if combined_count > 0 else None,
            "count": combined_count
        }
    }

def threshold_search(val_probs, val_y, holdout_probs, holdout_y):
    best = {
        "over": 0.65,
        "under": 0.35,
        "validation_accuracy": None,
        "validation_count": 0
    }

    over_grid = [round(x, 2) for x in np.arange(0.55, 0.86, 0.02)]
    under_grid = [round(x, 2) for x in np.arange(0.15, 0.46, 0.02)]

    for o in over_grid:
        for u in under_grid:
            if u >= o:
                continue
            over_idxs = [i for i, p in enumerate(val_probs) if p > o]
            under_idxs = [i for i, p in enumerate(val_probs) if p < u]
            count = len(over_idxs) + len(under_idxs)
            if count == 0:
                continue
            good = sum(1 for i in over_idxs if val_y[i] == 1) + sum(1 for i in under_idxs if val_y[i] == 0)
            acc = good / count
            current_best_acc = best["validation_accuracy"] if best["validation_accuracy"] is not None else -1.0
            if (acc > current_best_acc) or (abs(acc - current_best_acc) < 1e-12 and count > best["validation_count"]):
                best = {
                    "over": o,
                    "under": u,
                    "validation_accuracy": acc,
                    "validation_count": count
                }

    over_idxs_h = [i for i, p in enumerate(holdout_probs) if p > best["over"]]
    under_idxs_h = [i for i, p in enumerate(holdout_probs) if p < best["under"]]
    over_good_h = sum(1 for i in over_idxs_h if holdout_y[i] == 1)
    under_good_h = sum(1 for i in under_idxs_h if holdout_y[i] == 0)
    combined_count_h = len(over_idxs_h) + len(under_idxs_h)
    combined_good_h = over_good_h + under_good_h

    return {
        "best_thresholds": best,
        "holdout_metrics": {
            "over_picks": {
                "accuracy": (over_good_h / len(over_idxs_h)) if len(over_idxs_h) > 0 else None,
                "count": len(over_idxs_h)
            },
            "under_picks": {
                "accuracy": (under_good_h / len(under_idxs_h)) if len(under_idxs_h) > 0 else None,
                "count": len(under_idxs_h)
            },
            "combined": {
                "accuracy": (combined_good_h / combined_count_h) if combined_count_h > 0 else None,
                "count": combined_count_h
            }
        }
    }

test_rows_sorted = sorted(test_rows, key=lambda r: r.get("date") or "")
n_test = len(test_rows_sorted)
split_idx = int(n_test * 0.40)
if split_idx < 1:
    split_idx = 1
if split_idx >= n_test:
    split_idx = n_test - 1
val_rows = test_rows_sorted[:split_idx]
holdout_rows = test_rows_sorted[split_idx:]

variant_results = []
baseline_post_payload = None
for variant_name, feature_keys in variants:
    X_train = np.array([vec(r, feature_keys) for r in train_rows], dtype=np.float32)
    y_train = np.array([int(r["target_over_hit"]) for r in train_rows], dtype=np.int32)
    X_test = np.array([vec(r, feature_keys) for r in test_rows], dtype=np.float32)
    y_test = np.array([int(r["target_over_hit"]) for r in test_rows], dtype=np.int32)
    X_val = np.array([vec(r, feature_keys) for r in val_rows], dtype=np.float32)
    y_val = np.array([int(r["target_over_hit"]) for r in val_rows], dtype=np.int32)
    X_holdout = np.array([vec(r, feature_keys) for r in holdout_rows], dtype=np.float32)
    y_holdout = np.array([int(r["target_over_hit"]) for r in holdout_rows], dtype=np.int32)

    train_dataset = lgb.Dataset(X_train, label=y_train, free_raw_data=True)
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": float(learning_rate),
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "seed": 42,
        "verbosity": -1
    }
    model = lgb.train(
        params=params,
        train_set=train_dataset,
        num_boost_round=int(n_estimators)
    )

    p_test = [float(v) for v in model.predict(X_test)]
    p_val = [float(v) for v in model.predict(X_val)]
    p_holdout = [float(v) for v in model.predict(X_holdout)]

    metrics = metric_accuracy(p_test, y_test, 0.65, 0.35)
    th = threshold_search(p_val, list(y_val), p_holdout, list(y_holdout))
    variant_results.append({
        "variant": variant_name,
        "feature_count": len(feature_keys),
        "metrics": metrics,
        "threshold_search": {
            "validation_window": {
                "from_date": val_rows[0]["date"] if len(val_rows) > 0 else None,
                "to_date": val_rows[-1]["date"] if len(val_rows) > 0 else None,
                "rows": len(val_rows)
            },
            "holdout_window": {
                "from_date": holdout_rows[0]["date"] if len(holdout_rows) > 0 else None,
                "to_date": holdout_rows[-1]["date"] if len(holdout_rows) > 0 else None,
                "rows": len(holdout_rows)
            },
            "best_thresholds": th["best_thresholds"],
            "holdout_metrics": th["holdout_metrics"]
        }
    })

    if variant_name == "baseline_original_only":
        baseline_post_payload = {
            "probs": p_test,
            "rows": test_rows,
            "y_test": list(y_test)
        }

def get_variant(name):
    for v in variant_results:
        if v["variant"] == name:
            return v
    return None

baseline = get_variant("baseline_original_only")
rolling = get_variant("baseline_plus_rolling")
line_rel = get_variant("baseline_plus_line_relative")
flags = get_variant("baseline_plus_stability_role_flags")
all_new = get_variant("baseline_plus_all_new")

def delta_metric(v, base, path):
    if v is None or base is None:
        return None
    if path == "under":
        a = v["metrics"]["under_p_lt_0_35"]["accuracy"]
        b = base["metrics"]["under_p_lt_0_35"]["accuracy"]
    elif path == "over":
        a = v["metrics"]["over_p_gt_0_65"]["accuracy"]
        b = base["metrics"]["over_p_gt_0_65"]["accuracy"]
    elif path == "combined":
        a = v["metrics"]["combined_strategy"]["accuracy"]
        b = base["metrics"]["combined_strategy"]["accuracy"]
    else:
        return None
    if a is None or b is None:
        return None
    return a - b

groups = [
    ("rolling scoring/minutes/FGA", rolling),
    ("line-relative", line_rel),
    ("stability/role flags", flags),
    ("all new features", all_new)
]

under_best = "unknown"
over_best = "unknown"
hurt_group = "none"

under_deltas = [(name, delta_metric(v, baseline, "under")) for name, v in groups]
over_deltas = [(name, delta_metric(v, baseline, "over")) for name, v in groups]
combined_deltas = [(name, delta_metric(v, baseline, "combined")) for name, v in groups]

under_valid = [(n, d) for n, d in under_deltas if d is not None]
over_valid = [(n, d) for n, d in over_deltas if d is not None]
combined_valid = [(n, d) for n, d in combined_deltas if d is not None]

if len(under_valid) > 0:
    under_best = max(under_valid, key=lambda x: x[1])[0]
if len(over_valid) > 0:
    over_best = max(over_valid, key=lambda x: x[1])[0]
if len(combined_valid) > 0:
    worst = min(combined_valid, key=lambda x: x[1])
    if worst[1] < 0:
        hurt_group = worst[0]

best_variant = "unknown"
best_score = -1.0
for v in variant_results:
    score = v["metrics"]["combined_strategy"]["accuracy"]
    count = v["metrics"]["combined_strategy"]["count"]
    if score is None:
        continue
    adjusted = score + (count / max(1, len(test_rows))) * 1e-6
    if adjusted > best_score:
        best_score = adjusted
        best_variant = v["variant"]

def percentile_or_none(values, pct):
    vals = sorted([float(v) for v in values if v is not None and not (isinstance(v, float) and np.isnan(v))])
    if len(vals) == 0:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * pct
    lo = int(np.floor(pos))
    hi = int(np.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac

def evaluate_post_filters(payload):
    if payload is None:
        return {
            "baseline_variant": "baseline_original_only",
            "filter_thresholds": {
                "minutes_std_p75": None,
                "points_std_p75": None,
                "line_p10": None,
                "line_p90": None
            },
            "strategies": [],
            "single_filter_impact": {
                "top10_conf": [],
                "top20_conf": []
            },
            "leave_one_out": {
                "top10_conf": [],
                "top20_conf": []
            },
            "filter_importance": {
                "largest_hit_rate_gain_alone": "unknown",
                "largest_under_gain_alone": "unknown",
                "biggest_drop_when_removed_from_combined": "unknown",
                "over_pruning_candidate": "unknown"
            },
            "best_strategy": None,
            "under_best_filter": None,
            "best_reduced_strategy": None,
            "confidence_tiers": None,
            "rolling_window_stability": None,
            "rolling_strategy_comparison": None,
            "final_selection_strategy": None,
            "controlled_under_addon_experiment": None,
            "final_strategy_comparison": None
        }

    probs = payload["probs"]
    rows_local = payload["rows"]
    y_true = payload["y_test"]
    confidence = [abs(float(p) - 0.5) for p in probs]
    order = sorted(range(len(probs)), key=lambda i: confidence[i], reverse=True)

    minutes_std_p75 = percentile_or_none([r.get("minutes_std_10") for r in rows_local], 0.75)
    points_std_p75 = percentile_or_none([r.get("points_std_10") for r in rows_local], 0.75)
    line_p10 = percentile_or_none([r.get("line") for r in rows_local], 0.10)
    line_p90 = percentile_or_none([r.get("line") for r in rows_local], 0.90)

    def make_filter(name):
        if name == "none":
            return lambda i: True
        if name == "remove_high_minutes_std":
            return lambda i: (minutes_std_p75 is None) or (rows_local[i].get("minutes_std_10") is None) or (float(rows_local[i].get("minutes_std_10")) <= minutes_std_p75)
        if name == "remove_high_points_std":
            return lambda i: (points_std_p75 is None) or (rows_local[i].get("points_std_10") is None) or (float(rows_local[i].get("points_std_10")) <= points_std_p75)
        if name == "remove_low_recent_sample":
            return lambda i: (rows_local[i].get("history_games_10") is not None) and (float(rows_local[i].get("history_games_10")) >= 5)
        if name == "remove_low_lineup_or_high_scratch":
            return lambda i: ((rows_local[i].get("player_lineup_certainty") is None or float(rows_local[i].get("player_lineup_certainty")) >= 0.60) and
                              (rows_local[i].get("player_late_scratch_risk") is None or float(rows_local[i].get("player_late_scratch_risk")) <= 0.35))
        if name == "remove_extreme_lines":
            return lambda i: (line_p10 is None or line_p90 is None or rows_local[i].get("line") is None or
                              (float(rows_local[i].get("line")) >= line_p10 and float(rows_local[i].get("line")) <= line_p90))
        if name == "combined_all_filters":
            f_minutes = make_filter("remove_high_minutes_std")
            f_points = make_filter("remove_high_points_std")
            f_sample = make_filter("remove_low_recent_sample")
            f_lineup = make_filter("remove_low_lineup_or_high_scratch")
            f_lines = make_filter("remove_extreme_lines")
            return lambda i: f_minutes(i) and f_points(i) and f_sample(i) and f_lineup(i) and f_lines(i)
        return lambda i: True

    core_filter_names = [
        "remove_high_minutes_std",
        "remove_high_points_std",
        "remove_low_recent_sample",
        "remove_low_lineup_or_high_scratch",
        "remove_extreme_lines"
    ]
    filter_names = [
        "none",
        *core_filter_names,
        "combined_all_filters"
    ]

    strategies = []
    single_filter_impact = {"top10_conf": [], "top20_conf": []}
    leave_one_out = {"top10_conf": [], "top20_conf": []}

    def eval_selected_indices(name, selection, filter_mode, filters_applied, selected):
        if len(selected) == 0:
            return {
                "strategy": name,
                "selection": selection,
                "filter_mode": filter_mode,
                "filters_applied": filters_applied,
                "filter_count": len(filters_applied),
                "picks": 0,
                "hit_rate": None,
                "under_picks": 0,
                "under_hit_rate": None
            }
        preds = [1 if probs[i] >= 0.5 else 0 for i in selected]
        hit = sum(1 for idx, i in enumerate(selected) if preds[idx] == y_true[i])
        under_selected = [i for i in selected if probs[i] < 0.5]
        under_good = sum(1 for i in under_selected if y_true[i] == 0)
        return {
            "strategy": name,
            "selection": selection,
            "filter_mode": filter_mode,
            "filters_applied": filters_applied,
            "filter_count": len(filters_applied),
            "picks": len(selected),
            "hit_rate": hit / len(selected),
            "under_picks": len(under_selected),
            "under_hit_rate": (under_good / len(under_selected)) if len(under_selected) > 0 else None
        }

    def eval_strategy(name, top_ratio, filter_name):
        top_n = max(1, int(len(order) * top_ratio))
        picked_by_conf = order[:top_n]
        filt = make_filter(filter_name)
        selected = [i for i in picked_by_conf if filt(i)]
        if filter_name == "none":
            mode = "none"
            applied = []
        elif filter_name == "combined_all_filters":
            mode = "combined_all_filters"
            applied = list(core_filter_names)
        else:
            mode = "single_filter"
            applied = [filter_name]
        return eval_selected_indices(name, "top10_conf" if top_ratio == 0.10 else "top20_conf", mode, applied, selected)

    for top_ratio, label in [(0.10, "top10_conf"), (0.20, "top20_conf")]:
        row_records = []
        for fname in filter_names:
            rec = eval_strategy(f"{label}+{fname}", top_ratio, fname)
            strategies.append(rec)
            row_records.append(rec)
            if fname == "none" or fname in core_filter_names:
                single_filter_impact[label].append({
                    "strategy": rec["strategy"],
                    "picks": rec["picks"],
                    "hit_rate": rec["hit_rate"],
                    "under_picks": rec["under_picks"],
                    "under_hit_rate": rec["under_hit_rate"]
                })

        combined_ref = next((r for r in row_records if r["filter_mode"] == "combined_all_filters"), None)
        top_n = max(1, int(len(order) * top_ratio))
        picked_by_conf = order[:top_n]
        for removed in core_filter_names:
            keep_filters = [f for f in core_filter_names if f != removed]
            predicates = [make_filter(f) for f in keep_filters]
            selected = [i for i in picked_by_conf if all(pred(i) for pred in predicates)]
            rec = eval_selected_indices(
                f"{label}+combined_minus_{removed}",
                label,
                "leave_one_out",
                keep_filters,
                selected,
            )
            delta = None
            if combined_ref is not None and rec["hit_rate"] is not None and combined_ref["hit_rate"] is not None:
                delta = rec["hit_rate"] - combined_ref["hit_rate"]
            leave_one_out[label].append({
                "strategy": rec["strategy"],
                "picks": rec["picks"],
                "hit_rate": rec["hit_rate"],
                "delta_vs_combined": delta
            })
            strategies.append(rec)

    valid_by_hit = [s for s in strategies if s["hit_rate"] is not None]
    best_strategy = None
    if len(valid_by_hit) > 0:
        best_strategy = max(valid_by_hit, key=lambda s: (s["hit_rate"], s["picks"]))

    valid_under = [s for s in strategies if s["under_hit_rate"] is not None]
    under_best = None
    if len(valid_under) > 0:
        under_best = max(valid_under, key=lambda s: (s["under_hit_rate"], s["under_picks"]))

    # Filter importance ranking
    largest_hit_rate_gain_alone = "unknown"
    largest_under_gain_alone = "unknown"
    biggest_drop_when_removed_from_combined = "unknown"
    over_pruning_candidate = "unknown"

    alone_gain_rows = []
    alone_under_rows = []
    overprune_rows = []
    for selection in ["top10_conf", "top20_conf"]:
        baseline_row = next((r for r in single_filter_impact[selection] if r["strategy"].endswith("+none")), None)
        if baseline_row is None:
            continue
        for fname in core_filter_names:
            frow = next((r for r in single_filter_impact[selection] if r["strategy"].endswith("+" + fname)), None)
            if frow is None:
                continue
            if frow["hit_rate"] is not None and baseline_row["hit_rate"] is not None:
                alone_gain_rows.append((fname, selection, frow["hit_rate"] - baseline_row["hit_rate"]))
            if frow["under_hit_rate"] is not None and baseline_row["under_hit_rate"] is not None:
                alone_under_rows.append((fname, selection, frow["under_hit_rate"] - baseline_row["under_hit_rate"]))
            removed = baseline_row["picks"] - frow["picks"]
            gain = None
            if frow["hit_rate"] is not None and baseline_row["hit_rate"] is not None:
                gain = frow["hit_rate"] - baseline_row["hit_rate"]
            overprune_rows.append((fname, selection, removed, gain))

    if len(alone_gain_rows) > 0:
        best_gain = max(alone_gain_rows, key=lambda x: x[2])
        largest_hit_rate_gain_alone = f"{best_gain[0]} ({best_gain[1]})"
    if len(alone_under_rows) > 0:
        best_ugain = max(alone_under_rows, key=lambda x: x[2])
        largest_under_gain_alone = f"{best_ugain[0]} ({best_ugain[1]})"

    loo_rows = []
    for selection in ["top10_conf", "top20_conf"]:
        for row in leave_one_out[selection]:
            if row["delta_vs_combined"] is not None:
                loo_rows.append((row["strategy"], row["delta_vs_combined"]))
    if len(loo_rows) > 0:
        worst = min(loo_rows, key=lambda x: x[1])
        biggest_drop_when_removed_from_combined = worst[0]

    # Over-pruning: biggest pick reduction with <=0.5pp gain
    prunable = [r for r in overprune_rows if r[2] > 0 and (r[3] is None or r[3] <= 0.005)]
    if len(prunable) > 0:
        worst_prune = max(prunable, key=lambda x: x[2])
        over_pruning_candidate = f"{worst_prune[0]} ({worst_prune[1]})"
    elif len(overprune_rows) > 0:
        worst_prune = max(overprune_rows, key=lambda x: x[2])
        over_pruning_candidate = f"{worst_prune[0]} ({worst_prune[1]})"

    # Best reduced filter set: <=1% accuracy drop from best, more picks, fewer filters
    best_reduced = None
    if best_strategy is not None and best_strategy["hit_rate"] is not None:
        target_floor = best_strategy["hit_rate"] - 0.01
        candidates = [
            s for s in strategies
            if s["hit_rate"] is not None
            and s["filter_count"] < best_strategy["filter_count"]
            and s["picks"] > best_strategy["picks"]
            and s["hit_rate"] >= target_floor
        ]
        if len(candidates) > 0:
            best_reduced = max(candidates, key=lambda s: (s["picks"], s["hit_rate"]))

    # Confidence tiers for fixed strategy: topX_conf + combined_minus_remove_high_points_std
    confidence_tiers = None
    fixed_keep_filters = [f for f in core_filter_names if f != "remove_high_points_std"]
    fixed_preds = [make_filter(f) for f in fixed_keep_filters]

    def build_tier_metrics(indices, name):
        rec = eval_selected_indices(
            name,
            "confidence_tier",
            "fixed_reduced_strategy",
            fixed_keep_filters,
            indices,
        )
        return {
            "tier": name,
            "picks": rec["picks"],
            "hit_rate": rec["hit_rate"],
            "under_picks": rec["under_picks"],
            "under_hit_rate": rec["under_hit_rate"]
        }

    tier_defs = [
        ("top5", 0.00, 0.05),
        ("top10", 0.00, 0.10),
        ("top20", 0.00, 0.20),
        ("top30", 0.00, 0.30),
    ]
    band_defs = [
        ("top5", 0.00, 0.05),
        ("top5-10", 0.05, 0.10),
        ("top10-20", 0.10, 0.20),
        ("top20-30", 0.20, 0.30),
    ]

    cumulative_rows = []
    band_rows = []
    for tier_name, start_pct, end_pct in tier_defs:
        start_idx = int(len(order) * start_pct)
        end_idx = max(start_idx + 1, int(len(order) * end_pct))
        selected_base = order[start_idx:end_idx]
        selected_filtered = [i for i in selected_base if all(pred(i) for pred in fixed_preds)]
        cumulative_rows.append(build_tier_metrics(selected_filtered, tier_name))

    for tier_name, start_pct, end_pct in band_defs:
        start_idx = int(len(order) * start_pct)
        end_idx = max(start_idx + 1, int(len(order) * end_pct))
        selected_base = order[start_idx:end_idx]
        selected_filtered = [i for i in selected_base if all(pred(i) for pred in fixed_preds)]
        band_rows.append(build_tier_metrics(selected_filtered, tier_name))

    def first_below(rows, threshold):
        for row in rows:
            hit = row["hit_rate"]
            if hit is not None and hit < threshold:
                return row["tier"]
        return None

    confidence_tiers = {
        "strategy": "topX_conf+combined_minus_remove_high_points_std",
        "cumulative": cumulative_rows,
        "bands": band_rows,
        "thresholds_crossings": {
            "below_65_pct_at_tier": first_below(cumulative_rows, 0.65),
            "below_63_pct_at_tier": first_below(cumulative_rows, 0.63),
            "below_60_pct_at_tier": first_below(cumulative_rows, 0.60)
        }
    }

    top10_n_fixed = max(1, int(len(order) * 0.10))
    top10_selected = [i for i in order[:top10_n_fixed] if all(pred(i) for pred in fixed_preds)]

    def chrono_sort_indices(indices):
        return sorted(
            indices,
            key=lambda i: (
                str(rows_local[i].get("date") or ""),
                str(rows_local[i].get("game_id") or ""),
                int(rows_local[i].get("player_id") or 0),
            ),
        )

    def run_rolling_windows(indices, window_size, stride):
        selected_chrono = chrono_sort_indices(indices)
        windows = []
        start = 0
        while start < len(selected_chrono):
            end = min(start + window_size, len(selected_chrono))
            idxs = selected_chrono[start:end]
            picks = len(idxs)
            if picks == 0:
                break
            hit = 0
            under_picks = 0
            under_hit = 0
            for i in idxs:
                pred = 1 if probs[i] >= 0.5 else 0
                if pred == y_true[i]:
                    hit += 1
                if probs[i] < 0.5:
                    under_picks += 1
                    if y_true[i] == 0:
                        under_hit += 1
            windows.append({
                "start_index": start,
                "end_index": end - 1,
                "picks": picks,
                "hit_rate": (hit / picks) if picks > 0 else None,
                "under_picks": under_picks,
                "under_hit_rate": (under_hit / under_picks) if under_picks > 0 else None
            })
            if end == len(selected_chrono):
                break
            start += stride

        window_hit_rates = [w["hit_rate"] for w in windows if w["hit_rate"] is not None]
        if len(window_hit_rates) > 0:
            avg_hit = sum(window_hit_rates) / len(window_hit_rates)
            min_hit = min(window_hit_rates)
            max_hit = max(window_hit_rates)
            var_hit = sum((h - avg_hit) * (h - avg_hit) for h in window_hit_rates) / len(window_hit_rates)
            std_hit = float(np.sqrt(var_hit))
        else:
            avg_hit = None
            min_hit = None
            max_hit = None
            std_hit = None

        return {
            "windows": windows,
            "summary": {
                "average_hit_rate": avg_hit,
                "min_hit_rate": min_hit,
                "max_hit_rate": max_hit,
                "hit_rate_stddev": std_hit
            },
            "below_threshold_counts": {
                "below_65_pct": sum(1 for h in window_hit_rates if h < 0.65),
                "below_63_pct": sum(1 for h in window_hit_rates if h < 0.63),
                "below_60_pct": sum(1 for h in window_hit_rates if h < 0.60)
            },
            "total_picks": len(selected_chrono)
        }

    window_size = 100
    stride = 50
    rolling_primary = run_rolling_windows(top10_selected, window_size, stride)
    rolling_window_stability = {
        "strategy": "baseline_original_only + top10_conf + combined_minus_remove_high_points_std",
        "window_size": window_size,
        "stride": stride,
        "windows": rolling_primary["windows"],
        "summary": rolling_primary["summary"],
        "below_threshold_counts": rolling_primary["below_threshold_counts"]
    }

    # Multi-strategy rolling comparison
    def selected_indices_for_strategy(name, under_only_threshold):
        if name == "top10_conf+combined_minus_remove_high_points_std":
            n = max(1, int(len(order) * 0.10))
            base = order[:n]
            return [i for i in base if all(pred(i) for pred in fixed_preds)]
        if name == "top15_conf+combined_minus_remove_high_points_std":
            n = max(1, int(len(order) * 0.15))
            base = order[:n]
            return [i for i in base if all(pred(i) for pred in fixed_preds)]
        if name == "top20_conf+combined_minus_remove_high_points_std":
            n = max(1, int(len(order) * 0.20))
            base = order[:n]
            return [i for i in base if all(pred(i) for pred in fixed_preds)]
        if name == "top10_conf+combined_all_filters":
            n = max(1, int(len(order) * 0.10))
            base = order[:n]
            all_filters_pred = make_filter("combined_all_filters")
            return [i for i in base if all_filters_pred(i)]
        if name == "under_only_p_lt_threshold":
            return [i for i in range(len(probs)) if probs[i] < under_only_threshold]
        return []

    under_only_threshold = 0.35
    strategy_names = [
        "top10_conf+combined_minus_remove_high_points_std",
        "top15_conf+combined_minus_remove_high_points_std",
        "top20_conf+combined_minus_remove_high_points_std",
        "top10_conf+combined_all_filters",
        "under_only_p_lt_threshold"
    ]
    strategy_rows = []
    for sname in strategy_names:
        idxs = selected_indices_for_strategy(sname, under_only_threshold)
        roll = run_rolling_windows(idxs, window_size, stride)
        strategy_rows.append({
            "strategy": sname if sname != "under_only_p_lt_threshold" else f"under_only_p_lt_{under_only_threshold:.2f}",
            "average_hit_rate": roll["summary"]["average_hit_rate"],
            "min_hit_rate": roll["summary"]["min_hit_rate"],
            "max_hit_rate": roll["summary"]["max_hit_rate"],
            "hit_rate_stddev": roll["summary"]["hit_rate_stddev"],
            "total_picks": roll["total_picks"]
        })

    def pick_most_stable(rows):
        valid = [r for r in rows if r["hit_rate_stddev"] is not None and r["min_hit_rate"] is not None]
        if len(valid) == 0:
            return "unknown"
        best = min(valid, key=lambda r: (r["hit_rate_stddev"], -r["min_hit_rate"], -(r["average_hit_rate"] or -1)))
        return best["strategy"]

    def pick_best_accuracy(rows):
        valid = [r for r in rows if r["average_hit_rate"] is not None]
        if len(valid) == 0:
            return "unknown"
        best = max(valid, key=lambda r: (r["average_hit_rate"], r["min_hit_rate"] if r["min_hit_rate"] is not None else -1))
        return best["strategy"]

    def pick_best_balance(rows):
        valid = [r for r in rows if r["average_hit_rate"] is not None and r["total_picks"] > 0]
        if len(valid) == 0:
            return "unknown"
        max_picks = max(r["total_picks"] for r in valid)
        best = max(valid, key=lambda r: ((r["average_hit_rate"] * 0.7) + ((r["total_picks"] / max_picks) * 0.3)))
        return best["strategy"]

    rolling_strategy_comparison = {
        "window_size": window_size,
        "stride": stride,
        "under_only_threshold": under_only_threshold,
        "strategies": strategy_rows,
        "conclusions": {
            "most_stable_strategy": pick_most_stable(strategy_rows),
            "best_accuracy_strategy": pick_best_accuracy(strategy_rows),
            "best_balance_strategy": pick_best_balance(strategy_rows)
        }
    }

    # Final combined strategy: UNDER-first, then fill with core top15 strategy.
    def summarize_strategy(indices):
        picks = len(indices)
        if picks == 0:
            return {
                "total_picks": 0,
                "overall_hit_rate": None,
                "under_picks": 0,
                "under_hit_rate": None
            }
        hit = 0
        under_picks = 0
        under_hit = 0
        for i in indices:
            pred = 1 if probs[i] >= 0.5 else 0
            if pred == y_true[i]:
                hit += 1
            if probs[i] < 0.5:
                under_picks += 1
                if y_true[i] == 0:
                    under_hit += 1
        return {
            "total_picks": picks,
            "overall_hit_rate": hit / picks,
            "under_picks": under_picks,
            "under_hit_rate": (under_hit / under_picks) if under_picks > 0 else None
        }

    idx_under_only = selected_indices_for_strategy("under_only_p_lt_threshold", under_only_threshold)
    idx_top15_core = selected_indices_for_strategy("top15_conf+combined_minus_remove_high_points_std", under_only_threshold)
    under_set = set(idx_under_only)
    combined_indices = list(idx_under_only)
    for i in idx_top15_core:
        if i not in under_set:
            combined_indices.append(i)

    roll_under = run_rolling_windows(idx_under_only, window_size, stride)
    roll_top15 = run_rolling_windows(idx_top15_core, window_size, stride)
    roll_combined = run_rolling_windows(combined_indices, window_size, stride)
    sum_under = summarize_strategy(idx_under_only)
    sum_top15 = summarize_strategy(idx_top15_core)
    sum_combined = summarize_strategy(combined_indices)

    final_selection_strategy = {
        "under_only_threshold": under_only_threshold,
        "core_strategy": "top15_conf+combined_minus_remove_high_points_std",
        "combined_strategy": "under_first_then_top15_core_fill",
        "strategies": [
            {
                "strategy": f"under_only_p_lt_{under_only_threshold:.2f}",
                "total_picks": sum_under["total_picks"],
                "overall_hit_rate": sum_under["overall_hit_rate"],
                "under_picks": sum_under["under_picks"],
                "under_hit_rate": sum_under["under_hit_rate"],
                "rolling_avg_hit_rate": roll_under["summary"]["average_hit_rate"],
                "rolling_min_hit_rate": roll_under["summary"]["min_hit_rate"],
                "rolling_max_hit_rate": roll_under["summary"]["max_hit_rate"],
                "rolling_hit_rate_stddev": roll_under["summary"]["hit_rate_stddev"]
            },
            {
                "strategy": "top15_conf+combined_minus_remove_high_points_std",
                "total_picks": sum_top15["total_picks"],
                "overall_hit_rate": sum_top15["overall_hit_rate"],
                "under_picks": sum_top15["under_picks"],
                "under_hit_rate": sum_top15["under_hit_rate"],
                "rolling_avg_hit_rate": roll_top15["summary"]["average_hit_rate"],
                "rolling_min_hit_rate": roll_top15["summary"]["min_hit_rate"],
                "rolling_max_hit_rate": roll_top15["summary"]["max_hit_rate"],
                "rolling_hit_rate_stddev": roll_top15["summary"]["hit_rate_stddev"]
            },
            {
                "strategy": "combined_under_first_plus_top15_fill",
                "total_picks": sum_combined["total_picks"],
                "overall_hit_rate": sum_combined["overall_hit_rate"],
                "under_picks": sum_combined["under_picks"],
                "under_hit_rate": sum_combined["under_hit_rate"],
                "rolling_avg_hit_rate": roll_combined["summary"]["average_hit_rate"],
                "rolling_min_hit_rate": roll_combined["summary"]["min_hit_rate"],
                "rolling_max_hit_rate": roll_combined["summary"]["max_hit_rate"],
                "rolling_hit_rate_stddev": roll_combined["summary"]["hit_rate_stddev"]
            }
        ]
    }

    # Controlled UNDER add-on experiment on benchmark strategy.
    benchmark_indices = list(idx_top15_core)
    benchmark_set = set(benchmark_indices)
    benchmark_roll = run_rolling_windows(benchmark_indices, window_size, stride)
    benchmark_sum = summarize_strategy(benchmark_indices)

    add_on_thresholds = [0.35, 0.32, 0.30, 0.28, 0.25]
    confidence_gates = ["none", "top40_conf", "top25_conf"]
    daily_caps = [None, 1, 2, 3]

    all_variants = []
    variant_indices_map = {}

    def apply_conf_gate(indices, gate_name):
        if gate_name == "none":
            return list(indices)
        if len(indices) == 0:
            return []
        ratio = 0.40 if gate_name == "top40_conf" else 0.25
        n_keep = max(1, int(len(indices) * ratio))
        ranked = sorted(indices, key=lambda i: abs(float(probs[i]) - 0.5), reverse=True)
        return ranked[:n_keep]

    def apply_daily_cap(indices, cap):
        if cap is None:
            return list(indices)
        ranked = sorted(indices, key=lambda i: abs(float(probs[i]) - 0.5), reverse=True)
        by_day = {}
        picked = []
        for i in ranked:
            day = str(rows_local[i].get("date") or "")[:10]
            count = by_day.get(day, 0)
            if count >= cap:
                continue
            by_day[day] = count + 1
            picked.append(i)
        return picked

    for th in add_on_thresholds:
        remaining_under = [i for i in range(len(probs)) if (i not in benchmark_set) and (probs[i] < th)]
        for gate in confidence_gates:
            gated = apply_conf_gate(remaining_under, gate)
            for cap in daily_caps:
                add_on = apply_daily_cap(gated, cap)
                merged = list(benchmark_indices) + [i for i in add_on if i not in benchmark_set]
                merged_sum = summarize_strategy(merged)
                merged_roll = run_rolling_windows(merged, window_size, stride)
                cap_label = "none" if cap is None else str(cap)
                variant_name = f"addon_p_lt_{th:.2f}_gate_{gate}_cap_{cap_label}"
                all_variants.append({
                    "variant": variant_name,
                    "under_threshold": th,
                    "confidence_gate": gate,
                    "daily_cap": cap_label,
                    "total_picks": merged_sum["total_picks"],
                    "added_picks": len(add_on),
                    "overall_hit_rate": merged_sum["overall_hit_rate"],
                    "under_hit_rate": merged_sum["under_hit_rate"],
                    "rolling_avg_hit_rate": merged_roll["summary"]["average_hit_rate"],
                    "rolling_min_hit_rate": merged_roll["summary"]["min_hit_rate"],
                    "rolling_max_hit_rate": merged_roll["summary"]["max_hit_rate"],
                    "rolling_hit_rate_stddev": merged_roll["summary"]["hit_rate_stddev"]
                })
                variant_indices_map[variant_name] = merged

    frontier_sorted = sorted(
        all_variants,
        key=lambda r: (
            r["overall_hit_rate"] if r["overall_hit_rate"] is not None else -1,
            r["total_picks"],
        ),
        reverse=True,
    )

    constraints_passed = [
        r for r in frontier_sorted
        if r["overall_hit_rate"] is not None and r["overall_hit_rate"] >= 0.68
        and r["rolling_hit_rate_stddev"] is not None and r["rolling_hit_rate_stddev"] <= 0.03
        and r["rolling_min_hit_rate"] is not None and r["rolling_min_hit_rate"] >= 0.65
    ]

    # Conservative = smallest positive add-on that meets strict constraints; else "none".
    conservative_candidates = [r for r in constraints_passed if r["added_picks"] > 0]
    if len(conservative_candidates) > 0:
        best_conservative = min(conservative_candidates, key=lambda r: (r["added_picks"], -(r["overall_hit_rate"] or 0)))
        best_conservative_name = best_conservative["variant"]
    else:
        best_conservative_name = "none"

    # Balanced = maximize blended score of hit rate and added volume.
    if len(all_variants) > 0:
        max_added = max(r["added_picks"] for r in all_variants) or 1
        best_balanced = max(
            all_variants,
            key=lambda r: ((r["overall_hit_rate"] or 0) * 0.75) + ((r["added_picks"] / max_added) * 0.25),
        )
        best_balanced_name = best_balanced["variant"]
    else:
        best_balanced_name = "none"

    benchmark_beats_all = True
    b_hit = benchmark_sum["overall_hit_rate"]
    if b_hit is None:
        benchmark_beats_all = False
    else:
        for r in all_variants:
            rh = r["overall_hit_rate"]
            if rh is not None and rh > b_hit:
                benchmark_beats_all = False
                break

    controlled_under_addon_experiment = {
        "benchmark_strategy": "top15_conf+combined_minus_remove_high_points_std",
        "benchmark_metrics": {
            "total_picks": benchmark_sum["total_picks"],
            "overall_hit_rate": benchmark_sum["overall_hit_rate"],
            "under_picks": benchmark_sum["under_picks"],
            "under_hit_rate": benchmark_sum["under_hit_rate"],
            "rolling_avg_hit_rate": benchmark_roll["summary"]["average_hit_rate"],
            "rolling_min_hit_rate": benchmark_roll["summary"]["min_hit_rate"],
            "rolling_max_hit_rate": benchmark_roll["summary"]["max_hit_rate"],
            "rolling_hit_rate_stddev": benchmark_roll["summary"]["hit_rate_stddev"]
        },
        "variants": all_variants,
        "frontier_sorted": frontier_sorted,
        "constraints_passed": constraints_passed,
        "recommendation": {
            "best_conservative_addon": best_conservative_name,
            "best_balanced_addon": best_balanced_name,
            "benchmark_beats_all_addons": benchmark_beats_all
        }
    }

    # Final strategy comparison report from current controlled add-on outputs.
    ranking_priority = "rolling_min desc, overall_hit_rate desc, rolling_stddev asc, total_picks desc"
    ranked_constraints = sorted(
        constraints_passed,
        key=lambda r: (
            r["rolling_min_hit_rate"] if r["rolling_min_hit_rate"] is not None else -1,
            r["overall_hit_rate"] if r["overall_hit_rate"] is not None else -1,
            -(r["rolling_hit_rate_stddev"] if r["rolling_hit_rate_stddev"] is not None else 10**9),
            r["total_picks"],
        ),
        reverse=True,
    )
    top3_addons = ranked_constraints[:3]

    benchmark_row = {
        "label": "benchmark",
        "variant": "top15_conf+combined_minus_remove_high_points_std",
        "total_picks": benchmark_sum["total_picks"],
        "added_picks": 0,
        "overall_hit_rate": benchmark_sum["overall_hit_rate"],
        "under_hit_rate": benchmark_sum["under_hit_rate"],
        "rolling_avg_hit_rate": benchmark_roll["summary"]["average_hit_rate"],
        "rolling_min_hit_rate": benchmark_roll["summary"]["min_hit_rate"],
        "rolling_max_hit_rate": benchmark_roll["summary"]["max_hit_rate"],
        "rolling_hit_rate_stddev": benchmark_roll["summary"]["hit_rate_stddev"],
    }
    comparison_rows = [benchmark_row]
    for idx, row in enumerate(top3_addons, start=1):
        comparison_rows.append({
            "label": f"addon_rank_{idx}",
            "variant": row["variant"],
            "total_picks": row["total_picks"],
            "added_picks": row["added_picks"],
            "overall_hit_rate": row["overall_hit_rate"],
            "under_hit_rate": row["under_hit_rate"],
            "rolling_avg_hit_rate": row["rolling_avg_hit_rate"],
            "rolling_min_hit_rate": row["rolling_min_hit_rate"],
            "rolling_max_hit_rate": row["rolling_max_hit_rate"],
            "rolling_hit_rate_stddev": row["rolling_hit_rate_stddev"],
        })

    # 3 chronological blocks over full 2025 test-set ordering.
    chron_all = sorted(
        list(range(len(test_rows))),
        key=lambda i: (
            str(test_rows[i].get("date") or ""),
            str(test_rows[i].get("game_id") or ""),
            int(test_rows[i].get("player_id") or 0),
        ),
    )
    n_all = len(chron_all)
    cut1 = n_all // 3
    cut2 = (2 * n_all) // 3
    block_lookup = {}
    for pos, i in enumerate(chron_all):
        if pos < cut1:
            block_lookup[i] = "early"
        elif pos < cut2:
            block_lookup[i] = "middle"
        else:
            block_lookup[i] = "late"

    def block_stats(indices):
        out = {"early": {"picks": 0, "hit": 0}, "middle": {"picks": 0, "hit": 0}, "late": {"picks": 0, "hit": 0}}
        for i in indices:
            b = block_lookup.get(i)
            if b is None:
                continue
            out[b]["picks"] += 1
            pred = 1 if probs[i] >= 0.5 else 0
            if pred == y_true[i]:
                out[b]["hit"] += 1
        return {
            "early": {"picks": out["early"]["picks"], "hit_rate": (out["early"]["hit"] / out["early"]["picks"]) if out["early"]["picks"] > 0 else None},
            "middle": {"picks": out["middle"]["picks"], "hit_rate": (out["middle"]["hit"] / out["middle"]["picks"]) if out["middle"]["picks"] > 0 else None},
            "late": {"picks": out["late"]["picks"], "hit_rate": (out["late"]["hit"] / out["late"]["picks"]) if out["late"]["picks"] > 0 else None},
        }

    strategy_indices_for_blocks = {"benchmark": idx_top15_core}
    for idx, row in enumerate(top3_addons, start=1):
        strategy_indices_for_blocks[f"addon_rank_{idx}"] = variant_indices_map.get(row["variant"], [])

    block_performance = {"early": [], "middle": [], "late": []}
    for label, idxs in strategy_indices_for_blocks.items():
        s = block_stats(idxs)
        block_performance["early"].append({"label": label, "picks": s["early"]["picks"], "hit_rate": s["early"]["hit_rate"]})
        block_performance["middle"].append({"label": label, "picks": s["middle"]["picks"], "hit_rate": s["middle"]["hit_rate"]})
        block_performance["late"].append({"label": label, "picks": s["late"]["picks"], "hit_rate": s["late"]["hit_rate"]})

    # Recommendations
    default_mode = benchmark_row["variant"]
    if len(top3_addons) > 0:
        default_mode = top3_addons[0]["variant"]
    conservative_mode = benchmark_row["variant"]
    if len(top3_addons) > 0:
        conservative_mode = min(
            top3_addons,
            key=lambda r: (
                r["rolling_hit_rate_stddev"] if r["rolling_hit_rate_stddev"] is not None else 10**9,
                -(r["rolling_min_hit_rate"] if r["rolling_min_hit_rate"] is not None else -1),
            ),
        )["variant"]
    keep_benchmark_fallback = True

    final_strategy_comparison = {
        "ranking_priority": ranking_priority,
        "top3_addons": top3_addons,
        "comparison_rows": comparison_rows,
        "block_performance": block_performance,
        "recommendation": {
            "default_mode": default_mode,
            "conservative_mode": conservative_mode,
            "keep_benchmark_as_fallback": keep_benchmark_fallback
        }
    }

    return {
        "baseline_variant": "baseline_original_only",
        "filter_thresholds": {
            "minutes_std_p75": minutes_std_p75,
            "points_std_p75": points_std_p75,
            "line_p10": line_p10,
            "line_p90": line_p90
        },
        "strategies": strategies,
        "single_filter_impact": single_filter_impact,
        "leave_one_out": leave_one_out,
        "filter_importance": {
            "largest_hit_rate_gain_alone": largest_hit_rate_gain_alone,
            "largest_under_gain_alone": largest_under_gain_alone,
            "biggest_drop_when_removed_from_combined": biggest_drop_when_removed_from_combined,
            "over_pruning_candidate": over_pruning_candidate
        },
        "best_strategy": best_strategy,
        "under_best_filter": {
            "strategy": under_best["strategy"],
            "under_hit_rate": under_best["under_hit_rate"],
            "under_picks": under_best["under_picks"]
        } if under_best is not None else None,
        "best_reduced_strategy": {
            "strategy": best_reduced["strategy"],
            "selection": best_reduced["selection"],
            "filter_mode": best_reduced["filter_mode"],
            "filters_applied": best_reduced["filters_applied"],
            "filter_count": best_reduced["filter_count"],
            "picks": best_reduced["picks"],
            "hit_rate": best_reduced["hit_rate"],
            "under_picks": best_reduced["under_picks"],
            "under_hit_rate": best_reduced["under_hit_rate"],
            "reference_best_strategy": best_strategy["strategy"] if best_strategy is not None else "n/a",
            "reference_best_hit_rate": best_strategy["hit_rate"] if best_strategy is not None else None,
            "accuracy_delta_vs_best": (best_reduced["hit_rate"] - best_strategy["hit_rate"]) if best_reduced is not None and best_strategy is not None and best_reduced["hit_rate"] is not None and best_strategy["hit_rate"] is not None else None
        } if best_reduced is not None else None,
        "confidence_tiers": confidence_tiers,
        "rolling_window_stability": rolling_window_stability,
        "rolling_strategy_comparison": rolling_strategy_comparison,
        "final_selection_strategy": final_selection_strategy,
        "controlled_under_addon_experiment": controlled_under_addon_experiment,
        "final_strategy_comparison": final_strategy_comparison
    }

post_filtering = evaluate_post_filters(baseline_post_payload)

y_test_full = np.array([int(r["target_over_hit"]) for r in test_rows], dtype=np.int32)
over_count = int(np.sum(y_test_full == 1))
under_count = int(np.sum(y_test_full == 0))

result = {
    "dataset_size": len(rows),
    "train_size": len(train_rows),
    "test_size": len(test_rows),
    "class_balance": {
        "over_count": over_count,
        "under_count": under_count,
        "over_rate": (over_count / len(y_test_full)) if len(y_test_full) > 0 else None
    },
    "variants": variant_results,
    "conclusions": {
        "under_improved_group": under_best,
        "over_improved_group": over_best,
        "selective_hurt_group": hurt_group,
        "best_variant_for_hit_rate": best_variant
    },
    "post_prediction_filtering": post_filtering
}

print(json.dumps(result))
`;

  const tempScriptPath = join(
    tmpdir(),
    `bluey_player_points_${Date.now()}_${Math.random().toString(36).slice(2)}.py`,
  );
  writeFileSync(tempScriptPath, pythonScript, "utf8");

  return new Promise((resolve, reject) => {
    let cleaned = false;
    const cleanupTempScript = () => {
      if (cleaned) return;
      cleaned = true;
      try {
        unlinkSync(tempScriptPath);
      } catch {
        // Best-effort cleanup only.
      }
    };

    const proc = spawn("python", [tempScriptPath], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const payload = JSON.stringify({
      rows,
      learning_rate: learningRate,
      n_estimators: numEstimators,
    });
    proc.stdin.write(payload);
    proc.stdin.end();

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (chunk: unknown) => {
      stdout += String(chunk);
    });
    proc.stderr.on("data", (chunk: unknown) => {
      stderr += String(chunk);
    });

    proc.on("close", (code: number | null) => {
      cleanupTempScript();
      if (code !== 0) {
        reject(new Error(`Python LightGBM failed with code ${String(code)}: ${stderr || stdout}`));
        return;
      }

      try {
        const parsed = JSON.parse(stdout) as PythonResult & { error?: string; detail?: string; hint?: string };
        if (parsed.error) {
          reject(new Error(`${parsed.error}: ${parsed.detail ?? ""} ${parsed.hint ?? ""}`.trim()));
          return;
        }
        resolve(parsed as PythonResult);
      } catch {
        reject(new Error(`Failed to parse Python output: ${stdout || stderr}`));
      }
    });

    proc.on("error", (err: unknown) => {
      cleanupTempScript();
      reject(err);
    });
  });
}

function fmtPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) return "n/a";
  return `${(value * 100).toFixed(2)}%`;
}

function fmtDeltaPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) return "n/a";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(2)}%`;
}

function printResults(result: PythonResult): void {
  console.log("\n=== Player Points OVER/UNDER Ablation Study ===\n");
  console.log(`Dataset size: ${result.dataset_size}`);
  console.log(`Train size (2023-2024): ${result.train_size}`);
  console.log(`Test size (2025): ${result.test_size}`);
  console.log("\nClass balance (2025 full):");
  console.log(`  Over:  ${result.class_balance.over_count}`);
  console.log(`  Under: ${result.class_balance.under_count}`);
  console.log(`  Over rate: ${fmtPct(result.class_balance.over_rate)}`);

  console.log("\nVariant comparison (fixed thresholds OVER>0.65, UNDER<0.35):");
  console.log(
    "  variant | overall | top20 conf | top10 conf | over>0.65 | under<0.35 | combined",
  );
  for (const variant of result.variants) {
    const m = variant.metrics;
    console.log(
      `  ${variant.variant} | ${fmtPct(m.overall_accuracy)} | ${fmtPct(m.top20_confidence_accuracy.accuracy)} (n=${m.top20_confidence_accuracy.count})` +
        ` | ${fmtPct(m.top10_confidence_accuracy.accuracy)} (n=${m.top10_confidence_accuracy.count})` +
        ` | ${fmtPct(m.over_p_gt_0_65.accuracy)} (n=${m.over_p_gt_0_65.count})` +
        ` | ${fmtPct(m.under_p_lt_0_35.accuracy)} (n=${m.under_p_lt_0_35.count})` +
        ` | ${fmtPct(m.combined_strategy.accuracy)} (n=${m.combined_strategy.count})`,
    );
  }

  console.log("\nValidation-based threshold search (2025 early -> remaining holdout):");
  for (const variant of result.variants) {
    const t = variant.threshold_search;
    console.log(`  ${variant.variant}`);
    console.log(
      `    Validation window: ${t.validation_window.from_date ?? "n/a"} to ${t.validation_window.to_date ?? "n/a"} (n=${t.validation_window.rows})`,
    );
    console.log(
      `    Holdout window: ${t.holdout_window.from_date ?? "n/a"} to ${t.holdout_window.to_date ?? "n/a"} (n=${t.holdout_window.rows})`,
    );
    console.log(
      `    Best thresholds on validation: OVER>${t.best_thresholds.over.toFixed(2)}, UNDER<${t.best_thresholds.under.toFixed(2)} | acc=${fmtPct(t.best_thresholds.validation_accuracy)} (n=${t.best_thresholds.validation_count})`,
    );
    console.log(
      `    Holdout combined: ${fmtPct(t.holdout_metrics.combined.accuracy)} (n=${t.holdout_metrics.combined.count}), ` +
        `OVER=${fmtPct(t.holdout_metrics.over_picks.accuracy)} (n=${t.holdout_metrics.over_picks.count}), ` +
        `UNDER=${fmtPct(t.holdout_metrics.under_picks.accuracy)} (n=${t.holdout_metrics.under_picks.count})`,
    );
  }

  console.log("\nAblation conclusions:");
  console.log(`  UNDER improved most with: ${result.conclusions.under_improved_group}`);
  console.log(`  OVER improved most with: ${result.conclusions.over_improved_group}`);
  console.log(`  Group that hurt selective accuracy: ${result.conclusions.selective_hurt_group}`);
  console.log(`  Best variant for hit-rate picks: ${result.conclusions.best_variant_for_hit_rate}`);

  console.log("\nPost-prediction filtering (baseline_original_only):");
  const pf = result.post_prediction_filtering;
  console.log(
    `  Thresholds: minutes_std_p75=${pf.filter_thresholds.minutes_std_p75?.toFixed?.(3) ?? "n/a"}, ` +
      `points_std_p75=${pf.filter_thresholds.points_std_p75?.toFixed?.(3) ?? "n/a"}, ` +
      `line_p10=${pf.filter_thresholds.line_p10?.toFixed?.(3) ?? "n/a"}, ` +
      `line_p90=${pf.filter_thresholds.line_p90?.toFixed?.(3) ?? "n/a"}`,
  );

  console.log("\n  1) Single Filter Impact - top10_conf");
  console.log("  strategy | picks | hit rate | under picks | under hit rate");
  for (const s of pf.single_filter_impact.top10_conf) {
    console.log(`  ${s.strategy} | ${s.picks} | ${fmtPct(s.hit_rate)} | ${s.under_picks} | ${fmtPct(s.under_hit_rate)}`);
  }

  console.log("\n  1) Single Filter Impact - top20_conf");
  console.log("  strategy | picks | hit rate | under picks | under hit rate");
  for (const s of pf.single_filter_impact.top20_conf) {
    console.log(`  ${s.strategy} | ${s.picks} | ${fmtPct(s.hit_rate)} | ${s.under_picks} | ${fmtPct(s.under_hit_rate)}`);
  }

  console.log("\n  2) Leave-One-Out vs combined_all_filters - top10_conf");
  console.log("  strategy | picks | hit rate | delta vs combined");
  for (const s of pf.leave_one_out.top10_conf) {
    console.log(`  ${s.strategy} | ${s.picks} | ${fmtPct(s.hit_rate)} | ${fmtDeltaPct(s.delta_vs_combined)}`);
  }

  console.log("\n  2) Leave-One-Out vs combined_all_filters - top20_conf");
  console.log("  strategy | picks | hit rate | delta vs combined");
  for (const s of pf.leave_one_out.top20_conf) {
    console.log(`  ${s.strategy} | ${s.picks} | ${fmtPct(s.hit_rate)} | ${fmtDeltaPct(s.delta_vs_combined)}`);
  }

  console.log("\n  3) Filter Contribution Ranking");
  console.log(`  Largest hit-rate gain alone: ${pf.filter_importance.largest_hit_rate_gain_alone}`);
  console.log(`  Largest UNDER gain alone: ${pf.filter_importance.largest_under_gain_alone}`);
  console.log(`  Biggest drop when removed from combined: ${pf.filter_importance.biggest_drop_when_removed_from_combined}`);
  console.log(`  Over-pruning candidate: ${pf.filter_importance.over_pruning_candidate}`);

  if (pf.under_best_filter) {
    console.log(
      `  UNDER-best filter strategy: ${pf.under_best_filter.strategy} ` +
        `(${fmtPct(pf.under_best_filter.under_hit_rate)}, n=${pf.under_best_filter.under_picks})`,
    );
  }
  if (pf.best_strategy) {
    console.log(
      `  Best combined filter+selection: ${pf.best_strategy.strategy} ` +
        `(${fmtPct(pf.best_strategy.hit_rate)}, n=${pf.best_strategy.picks})`,
    );
  }

  console.log("\n  4) Output Summary");
  console.log(`  Most important filter for accuracy: ${pf.filter_importance.biggest_drop_when_removed_from_combined}`);
  console.log(`  Most important filter for UNDER accuracy: ${pf.filter_importance.largest_under_gain_alone}`);
  console.log(`  Least useful filter (candidate for removal): ${pf.filter_importance.over_pruning_candidate}`);
  if (pf.best_reduced_strategy) {
    console.log(
      `  Best reduced filter set (~within 1% with more picks): ${pf.best_reduced_strategy.strategy} ` +
        `(${fmtPct(pf.best_reduced_strategy.hit_rate)}, n=${pf.best_reduced_strategy.picks}, ` +
        `delta_vs_best=${fmtDeltaPct(pf.best_reduced_strategy.accuracy_delta_vs_best)})`,
    );
  } else {
    console.log("  Best reduced filter set (~within 1% with more picks): none found");
  }

  if (pf.confidence_tiers) {
    const ct = pf.confidence_tiers;
    console.log("\n  5) Confidence Tiers (fixed reduced strategy)");
    console.log(`  Strategy: ${ct.strategy}`);
    console.log("  Cumulative tiers: tier | picks | hit rate | under picks | under hit rate");
    for (const row of ct.cumulative) {
      console.log(
        `  ${row.tier} | ${row.picks} | ${fmtPct(row.hit_rate)} | ${row.under_picks} | ${fmtPct(row.under_hit_rate)}`,
      );
    }
    console.log("  Band tiers: tier | picks | hit rate | under picks | under hit rate");
    for (const row of ct.bands) {
      console.log(
        `  ${row.tier} | ${row.picks} | ${fmtPct(row.hit_rate)} | ${row.under_picks} | ${fmtPct(row.under_hit_rate)}`,
      );
    }
    console.log("  Hit-rate decay thresholds:");
    console.log(`  below 65% at: ${ct.thresholds_crossings.below_65_pct_at_tier ?? "not reached"}`);
    console.log(`  below 63% at: ${ct.thresholds_crossings.below_63_pct_at_tier ?? "not reached"}`);
    console.log(`  below 60% at: ${ct.thresholds_crossings.below_60_pct_at_tier ?? "not reached"}`);
  }

  if (pf.rolling_window_stability) {
    const rw = pf.rolling_window_stability;
    console.log("\n  6) Rolling Window Stability Analysis");
    console.log(`  Strategy: ${rw.strategy}`);
    console.log(`  Window size: ${rw.window_size}, stride: ${rw.stride}`);
    console.log("  windows: start_index | end_index | picks | hit rate | under picks | under hit rate");
    for (const w of rw.windows) {
      console.log(
        `  ${w.start_index} | ${w.end_index} | ${w.picks} | ${fmtPct(w.hit_rate)} | ${w.under_picks} | ${fmtPct(w.under_hit_rate)}`,
      );
    }
    console.log(`  avg window hit rate: ${fmtPct(rw.summary.average_hit_rate)}`);
    console.log(`  min window hit rate: ${fmtPct(rw.summary.min_hit_rate)}`);
    console.log(`  max window hit rate: ${fmtPct(rw.summary.max_hit_rate)}`);
    console.log(`  window hit rate stddev: ${fmtPct(rw.summary.hit_rate_stddev)}`);
    console.log(`  windows below 65%: ${rw.below_threshold_counts.below_65_pct}`);
    console.log(`  windows below 63%: ${rw.below_threshold_counts.below_63_pct}`);
    console.log(`  windows below 60%: ${rw.below_threshold_counts.below_60_pct}`);
  }

  if (pf.rolling_strategy_comparison) {
    const rc = pf.rolling_strategy_comparison;
    console.log("\n  7) Multi-Strategy Rolling Comparison");
    console.log(`  Window size: ${rc.window_size}, stride: ${rc.stride}, under-only threshold: ${rc.under_only_threshold.toFixed(2)}`);
    console.log("  strategy | avg hit | min hit | max hit | stddev | total picks");
    for (const row of rc.strategies) {
      console.log(
        `  ${row.strategy} | ${fmtPct(row.average_hit_rate)} | ${fmtPct(row.min_hit_rate)} | ${fmtPct(row.max_hit_rate)} | ${fmtPct(row.hit_rate_stddev)} | ${row.total_picks}`,
      );
    }
    console.log(`  Most stable strategy: ${rc.conclusions.most_stable_strategy}`);
    console.log(`  Best accuracy strategy: ${rc.conclusions.best_accuracy_strategy}`);
    console.log(`  Best balance strategy: ${rc.conclusions.best_balance_strategy}`);
  }

  if (pf.final_selection_strategy) {
    const fs = pf.final_selection_strategy;
    console.log("\n  8) Final Combined Selection Strategy");
    console.log(`  Under-only signal: p < ${fs.under_only_threshold.toFixed(2)}`);
    console.log(`  Core signal: ${fs.core_strategy}`);
    console.log(`  Combination rule: ${fs.combined_strategy}`);
    console.log(
      "  strategy | total picks | overall hit | UNDER picks | UNDER hit | roll avg | roll min | roll max | roll stddev",
    );
    for (const row of fs.strategies) {
      console.log(
        `  ${row.strategy} | ${row.total_picks} | ${fmtPct(row.overall_hit_rate)} | ${row.under_picks} | ${fmtPct(row.under_hit_rate)} | ` +
          `${fmtPct(row.rolling_avg_hit_rate)} | ${fmtPct(row.rolling_min_hit_rate)} | ${fmtPct(row.rolling_max_hit_rate)} | ${fmtPct(row.rolling_hit_rate_stddev)}`,
      );
    }
  }

  if (pf.controlled_under_addon_experiment) {
    const ex = pf.controlled_under_addon_experiment;
    console.log("\n  9) Controlled UNDER Add-On Experiment");
    console.log(`  Benchmark strategy: ${ex.benchmark_strategy}`);
    console.log(
      `  Benchmark -> picks=${ex.benchmark_metrics.total_picks}, hit=${fmtPct(ex.benchmark_metrics.overall_hit_rate)}, ` +
        `UNDER hit=${fmtPct(ex.benchmark_metrics.under_hit_rate)}, roll avg/min/max/std=` +
        `${fmtPct(ex.benchmark_metrics.rolling_avg_hit_rate)}/${fmtPct(ex.benchmark_metrics.rolling_min_hit_rate)}/` +
        `${fmtPct(ex.benchmark_metrics.rolling_max_hit_rate)}/${fmtPct(ex.benchmark_metrics.rolling_hit_rate_stddev)}`,
    );

    console.log("  Frontier (sorted by hit rate then picks):");
    console.log(
      "  variant | total picks | added picks | overall hit | UNDER hit | roll avg | roll min | roll max | roll stddev",
    );
    for (const r of ex.frontier_sorted) {
      console.log(
        `  ${r.variant} | ${r.total_picks} | ${r.added_picks} | ${fmtPct(r.overall_hit_rate)} | ${fmtPct(r.under_hit_rate)} | ` +
          `${fmtPct(r.rolling_avg_hit_rate)} | ${fmtPct(r.rolling_min_hit_rate)} | ${fmtPct(r.rolling_max_hit_rate)} | ${fmtPct(r.rolling_hit_rate_stddev)}`,
      );
    }

    console.log("  Variants passing constraints (hit>=68%, std<=3%, min>=65%):");
    if (ex.constraints_passed.length === 0) {
      console.log("  none");
    } else {
      for (const r of ex.constraints_passed) {
        console.log(
          `  ${r.variant} | picks=${r.total_picks} | added=${r.added_picks} | hit=${fmtPct(r.overall_hit_rate)} | ` +
            `min=${fmtPct(r.rolling_min_hit_rate)} | std=${fmtPct(r.rolling_hit_rate_stddev)}`,
        );
      }
    }

    console.log("  Recommendation:");
    console.log(`  best conservative add-on: ${ex.recommendation.best_conservative_addon}`);
    console.log(`  best balanced add-on: ${ex.recommendation.best_balanced_addon}`);
    console.log(
      `  benchmark alone better than every add-on variant: ${ex.recommendation.benchmark_beats_all_addons ? "yes" : "no"}`,
    );
  }

  if (pf.final_strategy_comparison) {
    const fs = pf.final_strategy_comparison;
    console.log("\n  10) Final Strategy Comparison");
    console.log(`  Ranking priority: ${fs.ranking_priority}`);
    console.log(
      "  label | variant | total picks | added picks | overall hit | UNDER hit | roll avg | roll min | roll max | roll stddev",
    );
    for (const row of fs.comparison_rows) {
      console.log(
        `  ${row.label} | ${row.variant} | ${row.total_picks} | ${row.added_picks} | ${fmtPct(row.overall_hit_rate)} | ${fmtPct(row.under_hit_rate)} | ` +
          `${fmtPct(row.rolling_avg_hit_rate)} | ${fmtPct(row.rolling_min_hit_rate)} | ${fmtPct(row.rolling_max_hit_rate)} | ${fmtPct(row.rolling_hit_rate_stddev)}`,
      );
    }

    console.log("  Block performance (2025 holdout thirds):");
    console.log("  early: label | picks | hit rate");
    for (const row of fs.block_performance.early) {
      console.log(`  ${row.label} | ${row.picks} | ${fmtPct(row.hit_rate)}`);
    }
    console.log("  middle: label | picks | hit rate");
    for (const row of fs.block_performance.middle) {
      console.log(`  ${row.label} | ${row.picks} | ${fmtPct(row.hit_rate)}`);
    }
    console.log("  late: label | picks | hit rate");
    for (const row of fs.block_performance.late) {
      console.log(`  ${row.label} | ${row.picks} | ${fmtPct(row.hit_rate)}`);
    }

    console.log("  Recommendation:");
    console.log(`  recommended default mode: ${fs.recommendation.default_mode}`);
    console.log(`  recommended conservative mode: ${fs.recommendation.conservative_mode}`);
    console.log(
      `  keep benchmark as fallback: ${fs.recommendation.keep_benchmark_as_fallback ? "yes" : "no"}`,
    );
  }
}

export async function runPlayerPointsModel(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const learningRate = flags.learningRate ? Number(flags.learningRate) : 0.05;
  const numEstimators = flags.nEstimators ? Number(flags.nEstimators) : 300;

  if (!Number.isFinite(learningRate) || learningRate <= 0) {
    throw new Error("Invalid --learningRate (must be > 0)");
  }
  if (!Number.isFinite(numEstimators) || numEstimators <= 0) {
    throw new Error("Invalid --nEstimators (must be > 0)");
  }

  console.log("Building player-points training dataset from existing tables...");
  const rows = await buildDatasetRows();
  if (rows.length === 0) {
    throw new Error("No eligible rows found. Ensure player_points props and completed player stats exist.");
  }

  console.log(`Fetched ${rows.length} rows. Training LightGBM baseline...`);
  const result = await runPythonLightGbm(rows, learningRate, numEstimators);
  printResults(result);
}

