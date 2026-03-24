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
  date: Date | string;
  season: number;
  home_score: number;
  away_score: number;
  actual_total: number;
  total_line: number;
  over_hit: number;
  home_rank_off: number | null;
  away_rank_off: number | null;
  home_rank_def: number | null;
  away_rank_def: number | null;
  home_ppg: number | null;
  away_ppg: number | null;
  home_oppg: number | null;
  away_oppg: number | null;
  home_rest_days: number | null;
  away_rest_days: number | null;
  home_is_b2b: boolean | null;
  away_is_b2b: boolean | null;
  home_lineup_certainty: number | null;
  away_lineup_certainty: number | null;
  home_late_scratch_risk: number | null;
  away_late_scratch_risk: number | null;
  home_injury_out_count: number | null;
  away_injury_out_count: number | null;
  spread_home: number | null;
  ml_home: number | null;
  ml_away: number | null;
  home_team_total_env_last_5_avg: number | null;
  away_team_total_env_last_5_avg: number | null;
  home_team_total_env_std_10: number | null;
  away_team_total_env_std_10: number | null;
  home_team_hist_games_10: number | null;
  away_team_hist_games_10: number | null;
  home_points_for_last_5_avg: number | null;
  away_points_for_last_5_avg: number | null;
  home_points_against_last_5_avg: number | null;
  away_points_against_last_5_avg: number | null;
}

interface ModelInputRow {
  game_id: string;
  date: string;
  season: number;
  over_hit: number;
  actual_total: number;
  total_line: number;
  home_rank_off: number | null;
  away_rank_off: number | null;
  home_rank_def: number | null;
  away_rank_def: number | null;
  home_ppg: number | null;
  away_ppg: number | null;
  home_oppg: number | null;
  away_oppg: number | null;
  home_rest_days: number | null;
  away_rest_days: number | null;
  home_is_b2b: number | null;
  away_is_b2b: number | null;
  home_lineup_certainty: number | null;
  away_lineup_certainty: number | null;
  home_late_scratch_risk: number | null;
  away_late_scratch_risk: number | null;
  home_injury_out_count: number | null;
  away_injury_out_count: number | null;
  spread_home: number | null;
  ml_home: number | null;
  ml_away: number | null;
  // engineered
  expected_total_simple: number | null;
  total_minus_expected_simple: number | null;
  rank_off_sum: number | null;
  rank_def_sum: number | null;
  rest_sum: number | null;
  b2b_count: number | null;
  injury_out_sum: number | null;
  lineup_certainty_min: number | null;
  late_scratch_risk_max: number | null;
  spread_abs: number | null;
  home_team_total_env_last_5_avg: number | null;
  away_team_total_env_last_5_avg: number | null;
  home_team_total_env_std_10: number | null;
  away_team_total_env_std_10: number | null;
  home_team_hist_games_10: number | null;
  away_team_hist_games_10: number | null;
  home_points_for_last_5_avg: number | null;
  away_points_for_last_5_avg: number | null;
  home_points_against_last_5_avg: number | null;
  away_points_against_last_5_avg: number | null;
  combined_team_total_env_last_5: number | null;
  combined_team_total_env_std_10: number | null;
  line_minus_combined_team_env: number | null;
  low_team_env_sample_flag: number | null;
  high_team_env_volatility_flag: number | null;
}

type PythonMetric = { accuracy: number | null; count: number };

type PythonVariant = {
  variant: string;
  feature_count: number;
  metrics: {
    overall_accuracy: number | null;
    top20_confidence_accuracy: PythonMetric;
    top15_confidence_accuracy: PythonMetric;
    top10_confidence_accuracy: PythonMetric;
    over_p_gt_0_60: PythonMetric;
    under_p_lt_0_40: PythonMetric;
    combined_strategy: PythonMetric;
  };
  threshold_search: {
    validation_window: { from_date: string | null; to_date: string | null; rows: number };
    holdout_window: { from_date: string | null; to_date: string | null; rows: number };
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
};

type PythonResult = {
  dataset_size: number;
  train_size: number;
  test_size: number;
  class_balance: {
    over_count: number;
    under_count: number;
    over_rate: number | null;
  };
  variants: PythonVariant[];
  conclusions: {
    strongest_variant: string;
    best_balanced_variant: string;
  };
  post_filtering: {
    benchmark_variant: string;
    benchmark: {
      picks: number;
      hit_rate: number | null;
      over_picks: number;
      over_hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
    };
    filters: Array<{
      strategy: string;
      picks: number;
      hit_rate: number | null;
      over_picks: number;
      over_hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
      delta_vs_benchmark: number | null;
    }>;
    best_strategy: {
      strategy: string;
      picks: number;
      hit_rate: number | null;
      over_picks: number;
      over_hit_rate: number | null;
      under_picks: number;
      under_hit_rate: number | null;
      delta_vs_benchmark: number | null;
    } | null;
    rolling_window: {
      strategy: string;
      window_size: number;
      stride: number;
      summary: {
        average_hit_rate: number | null;
        min_hit_rate: number | null;
        max_hit_rate: number | null;
        hit_rate_stddev: number | null;
      };
      windows_below_60_pct: number;
      windows_below_55_pct: number;
    } | null;
  };
};

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
    WITH game_odds_best AS (
      SELECT DISTINCT ON (go."gameId")
        go."gameId",
        go."totalOver" as total_line,
        go."spreadHome" as spread_home,
        go."mlHome" as ml_home,
        go."mlAway" as ml_away
      FROM "GameOdds" go
      WHERE go."totalOver" IS NOT NULL
      ORDER BY
        go."gameId",
        CASE WHEN go.source = 'consensus' THEN 0 ELSE 1 END,
        go."fetchedAt" DESC
    ),
    team_game_totals AS (
      SELECT
        g.id AS game_id,
        g.date,
        g.season,
        g."homeTeamId" AS team_id,
        g."homeScore"::float AS points_for,
        g."awayScore"::float AS points_against,
        (g."homeScore" + g."awayScore")::float AS total_points
      FROM "Game" g
      WHERE g.season IN (2023, 2024, 2025)
        AND g."homeScore" > 0
        AND g."awayScore" > 0
        AND g.status ILIKE '%Final%'
      UNION ALL
      SELECT
        g.id AS game_id,
        g.date,
        g.season,
        g."awayTeamId" AS team_id,
        g."awayScore"::float AS points_for,
        g."homeScore"::float AS points_against,
        (g."homeScore" + g."awayScore")::float AS total_points
      FROM "Game" g
      WHERE g.season IN (2023, 2024, 2025)
        AND g."homeScore" > 0
        AND g."awayScore" > 0
        AND g.status ILIKE '%Final%'
    ),
    team_hist AS (
      SELECT
        tgs.*,
        AVG(tgs.total_points) OVER (
          PARTITION BY tgs.season, tgs.team_id
          ORDER BY tgs.date, tgs.game_id
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_total_env_last_5_avg,
        STDDEV_SAMP(tgs.total_points) OVER (
          PARTITION BY tgs.season, tgs.team_id
          ORDER BY tgs.date, tgs.game_id
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS team_total_env_std_10,
        COUNT(*) OVER (
          PARTITION BY tgs.season, tgs.team_id
          ORDER BY tgs.date, tgs.game_id
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        )::int AS team_hist_games_10,
        AVG(tgs.points_for) OVER (
          PARTITION BY tgs.season, tgs.team_id
          ORDER BY tgs.date, tgs.game_id
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS points_for_last_5_avg,
        AVG(tgs.points_against) OVER (
          PARTITION BY tgs.season, tgs.team_id
          ORDER BY tgs.date, tgs.game_id
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS points_against_last_5_avg
      FROM team_game_totals tgs
    )
    SELECT
      g.id AS game_id,
      g.date AS date,
      g.season AS season,
      g."homeScore" AS home_score,
      g."awayScore" AS away_score,
      (g."homeScore" + g."awayScore") AS actual_total,
      go.total_line,
      CASE WHEN (g."homeScore" + g."awayScore") > go.total_line THEN 1 ELSE 0 END AS over_hit,
      gc."homeRankOff" AS home_rank_off,
      gc."awayRankOff" AS away_rank_off,
      gc."homeRankDef" AS home_rank_def,
      gc."awayRankDef" AS away_rank_def,
      gc."homePpg" AS home_ppg,
      gc."awayPpg" AS away_ppg,
      gc."homeOppg" AS home_oppg,
      gc."awayOppg" AS away_oppg,
      gc."homeRestDays" AS home_rest_days,
      gc."awayRestDays" AS away_rest_days,
      gc."homeIsB2b" AS home_is_b2b,
      gc."awayIsB2b" AS away_is_b2b,
      gc."homeLineupCertainty" AS home_lineup_certainty,
      gc."awayLineupCertainty" AS away_lineup_certainty,
      gc."homeLateScratchRisk" AS home_late_scratch_risk,
      gc."awayLateScratchRisk" AS away_late_scratch_risk,
      gc."homeInjuryOutCount" AS home_injury_out_count,
      gc."awayInjuryOutCount" AS away_injury_out_count,
      go.spread_home,
      go.ml_home,
      go.ml_away,
      home_hist.team_total_env_last_5_avg AS home_team_total_env_last_5_avg,
      away_hist.team_total_env_last_5_avg AS away_team_total_env_last_5_avg,
      home_hist.team_total_env_std_10 AS home_team_total_env_std_10,
      away_hist.team_total_env_std_10 AS away_team_total_env_std_10,
      home_hist.team_hist_games_10 AS home_team_hist_games_10,
      away_hist.team_hist_games_10 AS away_team_hist_games_10,
      home_hist.points_for_last_5_avg AS home_points_for_last_5_avg,
      away_hist.points_for_last_5_avg AS away_points_for_last_5_avg,
      home_hist.points_against_last_5_avg AS home_points_against_last_5_avg,
      away_hist.points_against_last_5_avg AS away_points_against_last_5_avg
    FROM "Game" g
    JOIN game_odds_best go
      ON go."gameId" = g.id
    LEFT JOIN "GameContext" gc
      ON gc."gameId" = g.id
    LEFT JOIN team_hist home_hist
      ON home_hist.game_id = g.id
      AND home_hist.team_id = g."homeTeamId"
    LEFT JOIN team_hist away_hist
      ON away_hist.game_id = g.id
      AND away_hist.team_id = g."awayTeamId"
    WHERE g.season IN (2023, 2024, 2025)
      AND g."homeScore" > 0
      AND g."awayScore" > 0
      AND g.status ILIKE '%Final%'
    ORDER BY g.date ASC, g.id ASC;
  `);

  return rows.map((row) => {
    const expectedSimple =
      row.home_ppg != null && row.away_ppg != null && row.home_oppg != null && row.away_oppg != null
        ? ((row.home_ppg + row.away_ppg + row.home_oppg + row.away_oppg) / 2)
        : null;
    return {
      game_id: row.game_id,
      date: row.date instanceof Date ? row.date.toISOString().slice(0, 10) : String(row.date).slice(0, 10),
      season: row.season,
      over_hit: row.over_hit,
      actual_total: row.actual_total,
      total_line: row.total_line,
      home_rank_off: row.home_rank_off,
      away_rank_off: row.away_rank_off,
      home_rank_def: row.home_rank_def,
      away_rank_def: row.away_rank_def,
      home_ppg: row.home_ppg,
      away_ppg: row.away_ppg,
      home_oppg: row.home_oppg,
      away_oppg: row.away_oppg,
      home_rest_days: row.home_rest_days,
      away_rest_days: row.away_rest_days,
      home_is_b2b: row.home_is_b2b == null ? null : row.home_is_b2b ? 1 : 0,
      away_is_b2b: row.away_is_b2b == null ? null : row.away_is_b2b ? 1 : 0,
      home_lineup_certainty: row.home_lineup_certainty,
      away_lineup_certainty: row.away_lineup_certainty,
      home_late_scratch_risk: row.home_late_scratch_risk,
      away_late_scratch_risk: row.away_late_scratch_risk,
      home_injury_out_count: row.home_injury_out_count,
      away_injury_out_count: row.away_injury_out_count,
      spread_home: row.spread_home,
      ml_home: row.ml_home,
      ml_away: row.ml_away,
      expected_total_simple: expectedSimple,
      total_minus_expected_simple:
        expectedSimple != null ? row.total_line - expectedSimple : null,
      rank_off_sum:
        row.home_rank_off != null && row.away_rank_off != null
          ? row.home_rank_off + row.away_rank_off
          : null,
      rank_def_sum:
        row.home_rank_def != null && row.away_rank_def != null
          ? row.home_rank_def + row.away_rank_def
          : null,
      rest_sum:
        row.home_rest_days != null && row.away_rest_days != null
          ? row.home_rest_days + row.away_rest_days
          : null,
      b2b_count:
        (row.home_is_b2b ? 1 : 0) + (row.away_is_b2b ? 1 : 0),
      injury_out_sum:
        row.home_injury_out_count != null && row.away_injury_out_count != null
          ? row.home_injury_out_count + row.away_injury_out_count
          : null,
      lineup_certainty_min:
        row.home_lineup_certainty != null && row.away_lineup_certainty != null
          ? Math.min(row.home_lineup_certainty, row.away_lineup_certainty)
          : null,
      late_scratch_risk_max:
        row.home_late_scratch_risk != null && row.away_late_scratch_risk != null
          ? Math.max(row.home_late_scratch_risk, row.away_late_scratch_risk)
          : null,
      spread_abs: row.spread_home == null ? null : Math.abs(row.spread_home),
      home_team_total_env_last_5_avg: row.home_team_total_env_last_5_avg,
      away_team_total_env_last_5_avg: row.away_team_total_env_last_5_avg,
      home_team_total_env_std_10: row.home_team_total_env_std_10,
      away_team_total_env_std_10: row.away_team_total_env_std_10,
      home_team_hist_games_10: row.home_team_hist_games_10,
      away_team_hist_games_10: row.away_team_hist_games_10,
      home_points_for_last_5_avg: row.home_points_for_last_5_avg,
      away_points_for_last_5_avg: row.away_points_for_last_5_avg,
      home_points_against_last_5_avg: row.home_points_against_last_5_avg,
      away_points_against_last_5_avg: row.away_points_against_last_5_avg,
      combined_team_total_env_last_5:
        row.home_team_total_env_last_5_avg != null && row.away_team_total_env_last_5_avg != null
          ? (row.home_team_total_env_last_5_avg + row.away_team_total_env_last_5_avg) / 2
          : null,
      combined_team_total_env_std_10:
        row.home_team_total_env_std_10 != null && row.away_team_total_env_std_10 != null
          ? (row.home_team_total_env_std_10 + row.away_team_total_env_std_10) / 2
          : null,
      line_minus_combined_team_env:
        row.total_line != null &&
        row.home_team_total_env_last_5_avg != null &&
        row.away_team_total_env_last_5_avg != null
          ? row.total_line - ((row.home_team_total_env_last_5_avg + row.away_team_total_env_last_5_avg) / 2)
          : null,
      low_team_env_sample_flag:
        row.home_team_hist_games_10 != null &&
        row.away_team_hist_games_10 != null &&
        row.home_team_hist_games_10 >= 5 &&
        row.away_team_hist_games_10 >= 5
          ? 0
          : 1,
      high_team_env_volatility_flag:
        row.home_team_total_env_std_10 != null &&
        row.away_team_total_env_std_10 != null &&
        ((row.home_team_total_env_std_10 + row.away_team_total_env_std_10) / 2) >= 18
          ? 1
          : 0,
    };
  });
}

function runPythonLightGbm(
  rows: ModelInputRow[],
  learningRate: number,
  numEstimators: number,
): Promise<PythonResult> {
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
    print(json.dumps({"error": "no_train_rows", "detail": "No train rows found for 2023-2024"}))
    sys.exit(3)
if len(test_rows) == 0:
    print(json.dumps({"error": "no_test_rows", "detail": "No test rows found for 2025"}))
    sys.exit(4)

baseline_features = [
    "total_line",
    "combined_team_total_env_last_5",
    "line_minus_combined_team_env",
    "combined_team_total_env_std_10",
    "rank_off_sum",
    "rank_def_sum",
    "rest_sum",
    "b2b_count",
    "injury_out_sum",
    "lineup_certainty_min",
    "late_scratch_risk_max",
    "low_team_env_sample_flag",
    "high_team_env_volatility_flag",
]
market_features = [
    "spread_home", "spread_abs", "ml_home", "ml_away"
]
line_relative_features = [
    "expected_total_simple",
    "total_minus_expected_simple",
    "home_team_total_env_last_5_avg",
    "away_team_total_env_last_5_avg",
]
rolling_team_form_features = [
    "home_points_for_last_5_avg",
    "away_points_for_last_5_avg",
    "home_points_against_last_5_avg",
    "away_points_against_last_5_avg",
]
raw_context_features = [
    "home_rank_off", "away_rank_off", "home_rank_def", "away_rank_def",
    "home_ppg", "away_ppg", "home_oppg", "away_oppg",
    "home_rest_days", "away_rest_days",
    "home_is_b2b", "away_is_b2b",
    "home_lineup_certainty", "away_lineup_certainty",
    "home_late_scratch_risk", "away_late_scratch_risk",
    "home_injury_out_count", "away_injury_out_count"
]

variants = [
    ("total_baseline_environment", baseline_features),
    ("total_plus_market_context", baseline_features + market_features + line_relative_features),
    ("total_plus_team_form", baseline_features + rolling_team_form_features + line_relative_features),
    ("total_plus_raw_context", baseline_features + raw_context_features + line_relative_features),
    ("total_all_features", baseline_features + market_features + raw_context_features + line_relative_features + rolling_team_form_features),
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

def metric_accuracy(p_over, y_true, over_threshold=0.60, under_threshold=0.40):
    preds = [1 if p >= 0.5 else 0 for p in p_over]
    n = len(y_true)
    overall_acc = None
    if n > 0:
        overall_acc = sum(1 for i in range(n) if preds[i] == y_true[i]) / n

    conf = [abs(p - 0.5) for p in p_over]
    idxs_sorted_conf = sorted(range(n), key=lambda i: conf[i], reverse=True)
    top20_n = max(1, int(n * 0.20))
    top15_n = max(1, int(n * 0.15))
    top10_n = max(1, int(n * 0.10))
    top20 = idxs_sorted_conf[:top20_n]
    top15 = idxs_sorted_conf[:top15_n]
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
        "top15_confidence_accuracy": acc_for(top15),
        "top10_confidence_accuracy": acc_for(top10),
        "over_p_gt_0_60": {
            "accuracy": (over_good / len(over_idxs)) if len(over_idxs) > 0 else None,
            "count": len(over_idxs)
        },
        "under_p_lt_0_40": {
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
        "over": 0.60,
        "under": 0.40,
        "validation_accuracy": None,
        "validation_count": 0
    }
    over_grid = [round(x, 2) for x in np.arange(0.55, 0.81, 0.02)]
    under_grid = [round(x, 2) for x in np.arange(0.20, 0.46, 0.02)]
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
baseline_payload = None
for variant_name, feature_keys in variants:
    X_train = np.array([vec(r, feature_keys) for r in train_rows], dtype=np.float32)
    y_train = np.array([int(r["over_hit"]) for r in train_rows], dtype=np.int32)
    X_test = np.array([vec(r, feature_keys) for r in test_rows], dtype=np.float32)
    y_test = np.array([int(r["over_hit"]) for r in test_rows], dtype=np.int32)
    X_val = np.array([vec(r, feature_keys) for r in val_rows], dtype=np.float32)
    y_val = np.array([int(r["over_hit"]) for r in val_rows], dtype=np.int32)
    X_holdout = np.array([vec(r, feature_keys) for r in holdout_rows], dtype=np.float32)
    y_holdout = np.array([int(r["over_hit"]) for r in holdout_rows], dtype=np.int32)

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
    metrics = metric_accuracy(p_test, list(y_test), 0.60, 0.40)
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
    if variant_name == "total_plus_team_form":
        baseline_payload = {
            "rows": test_rows,
            "probs": p_test,
            "y_test": list(y_test)
        }

def best_by_combined(variants_list):
    valid = [v for v in variants_list if v["metrics"]["combined_strategy"]["accuracy"] is not None]
    if len(valid) == 0:
        return "unknown"
    return max(valid, key=lambda v: (v["metrics"]["combined_strategy"]["accuracy"], v["metrics"]["combined_strategy"]["count"]))["variant"]

def best_by_balance(variants_list):
    valid = [v for v in variants_list if v["metrics"]["combined_strategy"]["accuracy"] is not None]
    if len(valid) == 0:
        return "unknown"
    max_count = max(v["metrics"]["combined_strategy"]["count"] for v in valid) or 1
    return max(valid, key=lambda v: ((v["metrics"]["combined_strategy"]["accuracy"] * 0.8) + ((v["metrics"]["combined_strategy"]["count"] / max_count) * 0.2)))["variant"]

def eval_post_filters(payload):
    BENCHMARK_CONFIG = {
        "id": "totals_benchmark_v1",
        "model_variant": "total_plus_team_form",
        "top_conf_pct": 15,
        "require_low_volatility": True,
        "max_volatility_std10": 18.0
    }
    if payload is None:
        return {
            "benchmark_variant": BENCHMARK_CONFIG["model_variant"],
            "benchmark": {
                "picks": 0, "hit_rate": None, "over_picks": 0, "over_hit_rate": None, "under_picks": 0, "under_hit_rate": None
            },
            "filters": [],
            "best_strategy": None,
            "rolling_window": None,
            "comparison_harness": {
                "benchmark_id": BENCHMARK_CONFIG["id"],
                "benchmark_config": BENCHMARK_CONFIG,
                "variants": [],
                "top3": [],
                "recommendation": {
                    "default_mode": "totals_benchmark_v1",
                    "conservative_mode": "totals_benchmark_v1",
                    "replace_benchmark": False
                }
            }
        }

    rows_local = payload["rows"]
    probs = payload["probs"]
    y_true = payload["y_test"]
    conf = [abs(float(p) - 0.5) for p in probs]
    order = sorted(range(len(probs)), key=lambda i: conf[i], reverse=True)

    test_rows_sorted = sorted(
        list(range(len(rows_local))),
        key=lambda i: (
            str(rows_local[i].get("date") or ""),
            str(rows_local[i].get("game_id") or ""),
        ),
    )
    n_all = len(test_rows_sorted)
    cut1 = n_all // 3
    cut2 = (2 * n_all) // 3
    segment_by_idx = {}
    for pos, idx in enumerate(test_rows_sorted):
      if pos < cut1:
        segment_by_idx[idx] = "early"
      elif pos < cut2:
        segment_by_idx[idx] = "mid"
      else:
        segment_by_idx[idx] = "late"

    def eval_indices(id_label, idxs):
        idxs_sorted = sorted(
            idxs,
            key=lambda i: (
                str(rows_local[i].get("date") or ""),
                str(rows_local[i].get("game_id") or ""),
            ),
        )
        picks = len(idxs_sorted)
        if picks == 0:
            return {
                "id": id_label,
                "picks": 0,
                "hit_rate": None,
                "over_picks": 0,
                "over_hit_rate": None,
                "under_picks": 0,
                "under_hit_rate": None,
                "rolling": {
                    "average_hit_rate": None,
                    "min_hit_rate": None,
                    "max_hit_rate": None,
                    "hit_rate_stddev": None,
                    "pct_windows_below_60": None,
                    "pct_windows_below_55": None
                },
                "longest_cold_streak": 0,
                "segments": {
                    "early": {"picks": 0, "hit_rate": None},
                    "mid": {"picks": 0, "hit_rate": None},
                    "late": {"picks": 0, "hit_rate": None}
                },
                "distribution": {
                    "avg_total_line": None,
                    "line_buckets": {"low_le_220": 0, "mid_220_235": 0, "high_gt_235": 0},
                    "spread_buckets": {"close_le_3": 0, "mid_3_9": 0, "blowout_ge_9": 0}
                }
            }

        preds = [1 if probs[i] >= 0.5 else 0 for i in idxs_sorted]
        good = sum(1 for j, i in enumerate(idxs_sorted) if preds[j] == y_true[i])
        hit_rate = good / picks
        over_idxs = [i for i in idxs_sorted if probs[i] >= 0.5]
        under_idxs = [i for i in idxs_sorted if probs[i] < 0.5]
        over_good = sum(1 for i in over_idxs if y_true[i] == 1)
        under_good = sum(1 for i in under_idxs if y_true[i] == 0)

        # Rolling windows over picked sequence
        window_size = max(5, min(20, picks // 3 if picks >= 9 else picks))
        stride = max(3, window_size // 2)
        rates = []
        start = 0
        while start < picks:
            end = min(start + window_size, picks)
            bucket = idxs_sorted[start:end]
            if len(bucket) == 0:
                break
            bpred = [1 if probs[i] >= 0.5 else 0 for i in bucket]
            bhit = sum(1 for j, i in enumerate(bucket) if bpred[j] == y_true[i])
            rates.append(bhit / len(bucket))
            if end == picks:
                break
            start += stride
        if len(rates) > 0:
            avg_hit = sum(rates) / len(rates)
            min_hit = min(rates)
            max_hit = max(rates)
            var_hit = sum((h - avg_hit) * (h - avg_hit) for h in rates) / len(rates)
            std_hit = float(np.sqrt(var_hit))
            pct_below_60 = (sum(1 for r in rates if r < 0.60) / len(rates)) * 100.0
            pct_below_55 = (sum(1 for r in rates if r < 0.55) / len(rates)) * 100.0
        else:
            avg_hit = None
            min_hit = None
            max_hit = None
            std_hit = None
            pct_below_60 = None
            pct_below_55 = None

        # Longest cold streak
        longest_cold = 0
        current_cold = 0
        for j, idx in enumerate(idxs_sorted):
            pred = preds[j]
            is_hit = (pred == y_true[idx])
            if is_hit:
                current_cold = 0
            else:
                current_cold += 1
                if current_cold > longest_cold:
                    longest_cold = current_cold

        # Time segments
        segment_data = {"early": {"picks": 0, "good": 0}, "mid": {"picks": 0, "good": 0}, "late": {"picks": 0, "good": 0}}
        for j, idx in enumerate(idxs_sorted):
            seg = segment_by_idx.get(idx, "late")
            segment_data[seg]["picks"] += 1
            if preds[j] == y_true[idx]:
                segment_data[seg]["good"] += 1

        # Distribution diagnostics
        lines = [rows_local[i].get("total_line") for i in idxs_sorted if rows_local[i].get("total_line") is not None]
        avg_line = float(sum(float(v) for v in lines) / len(lines)) if len(lines) > 0 else None
        line_buckets = {"low_le_220": 0, "mid_220_235": 0, "high_gt_235": 0}
        spread_buckets = {"close_le_3": 0, "mid_3_9": 0, "blowout_ge_9": 0}
        for i in idxs_sorted:
            line = rows_local[i].get("total_line")
            if line is not None:
                lv = float(line)
                if lv <= 220:
                    line_buckets["low_le_220"] += 1
                elif lv > 235:
                    line_buckets["high_gt_235"] += 1
                else:
                    line_buckets["mid_220_235"] += 1
            spread_abs = rows_local[i].get("spread_abs")
            if spread_abs is not None:
                sv = float(spread_abs)
                if sv <= 3:
                    spread_buckets["close_le_3"] += 1
                elif sv >= 9:
                    spread_buckets["blowout_ge_9"] += 1
                else:
                    spread_buckets["mid_3_9"] += 1

        return {
            "id": id_label,
            "picks": picks,
            "hit_rate": hit_rate,
            "over_picks": len(over_idxs),
            "over_hit_rate": (over_good / len(over_idxs)) if len(over_idxs) > 0 else None,
            "under_picks": len(under_idxs),
            "under_hit_rate": (under_good / len(under_idxs)) if len(under_idxs) > 0 else None,
            "rolling": {
                "average_hit_rate": avg_hit,
                "min_hit_rate": min_hit,
                "max_hit_rate": max_hit,
                "hit_rate_stddev": std_hit,
                "pct_windows_below_60": pct_below_60,
                "pct_windows_below_55": pct_below_55
            },
            "longest_cold_streak": longest_cold,
            "segments": {
                "early": {
                    "picks": segment_data["early"]["picks"],
                    "hit_rate": (segment_data["early"]["good"] / segment_data["early"]["picks"]) if segment_data["early"]["picks"] > 0 else None
                },
                "mid": {
                    "picks": segment_data["mid"]["picks"],
                    "hit_rate": (segment_data["mid"]["good"] / segment_data["mid"]["picks"]) if segment_data["mid"]["picks"] > 0 else None
                },
                "late": {
                    "picks": segment_data["late"]["picks"],
                    "hit_rate": (segment_data["late"]["good"] / segment_data["late"]["picks"]) if segment_data["late"]["picks"] > 0 else None
                }
            },
            "distribution": {
                "avg_total_line": avg_line,
                "line_buckets": line_buckets,
                "spread_buckets": spread_buckets
            }
        }

    def select_indices(config):
        top_pct = float(config.get("top_conf_pct", 15))
        top_count = max(1, int(len(order) * (top_pct / 100.0)))
        idxs = order[:min(len(order), top_count)]
        out = []
        for i in idxs:
            if config.get("require_low_volatility", False):
                vol = rows_local[i].get("combined_team_total_env_std_10")
                if vol is not None and float(vol) > float(config.get("max_volatility_std10", 18.0)):
                    continue
            if "max_abs_line_minus_env" in config:
                lm = rows_local[i].get("line_minus_combined_team_env")
                if lm is not None and abs(float(lm)) >= float(config["max_abs_line_minus_env"]):
                    continue
            if config.get("under_only", False):
                if probs[i] >= 0.5:
                    continue
            if "max_total_line" in config:
                line = rows_local[i].get("total_line")
                if line is not None and float(line) > float(config["max_total_line"]):
                    continue
            if config.get("stricter_volatility", False):
                vol = rows_local[i].get("combined_team_total_env_std_10")
                if vol is not None and float(vol) > float(config.get("strict_volatility_std10", 14.0)):
                    continue
            out.append(i)
        return out

    # Locked benchmark + controlled expansions
    compare_configs = [
        {"id": "totals_benchmark_v1", "top_conf_pct": 15, "require_low_volatility": True, "max_volatility_std10": 18.0},
        {"id": "top20_low_volatility", "top_conf_pct": 20, "require_low_volatility": True, "max_volatility_std10": 18.0},
        {"id": "top15_low_volatility_lineenv_lt8", "top_conf_pct": 15, "require_low_volatility": True, "max_volatility_std10": 18.0, "max_abs_line_minus_env": 8.0},
        {"id": "top15_low_volatility_under_only", "top_conf_pct": 15, "require_low_volatility": True, "max_volatility_std10": 18.0, "under_only": True},
        {"id": "top15_low_volatility_exclude_high_totals", "top_conf_pct": 15, "require_low_volatility": True, "max_volatility_std10": 18.0, "max_total_line": 235.0},
        {"id": "top15_stricter_volatility", "top_conf_pct": 15, "require_low_volatility": True, "max_volatility_std10": 18.0, "stricter_volatility": True, "strict_volatility_std10": 14.0},
    ]

    variant_rows = []
    for cfg in compare_configs:
        idxs = select_indices(cfg)
        metrics = eval_indices(cfg["id"], idxs)
        variant_rows.append({
            "id": cfg["id"],
            "config": cfg,
            **metrics
        })

    bench_row = next((r for r in variant_rows if r["id"] == "totals_benchmark_v1"), None)
    bench_hit = bench_row["hit_rate"] if bench_row is not None else None
    simple_filter_rows = []
    for r in variant_rows:
        if bench_hit is not None and r["hit_rate"] is not None:
            delta = r["hit_rate"] - bench_hit
        else:
            delta = None
        simple_filter_rows.append({
            "strategy": r["id"],
            "picks": r["picks"],
            "hit_rate": r["hit_rate"],
            "over_picks": r["over_picks"],
            "over_hit_rate": r["over_hit_rate"],
            "under_picks": r["under_picks"],
            "under_hit_rate": r["under_hit_rate"],
            "delta_vs_benchmark": delta,
        })

    def rank_key(r):
        min_hit = r["rolling"]["min_hit_rate"] if r["rolling"]["min_hit_rate"] is not None else -1.0
        hit = r["hit_rate"] if r["hit_rate"] is not None else -1.0
        std = r["rolling"]["hit_rate_stddev"] if r["rolling"]["hit_rate_stddev"] is not None else 1e9
        return (min_hit, hit, -std, r["picks"])

    ranked = sorted(variant_rows, key=rank_key, reverse=True)
    top3 = ranked[:3]
    bench_picks = bench_row["picks"] if bench_row is not None else 0
    robust_candidates = [
        r for r in ranked
        if r["picks"] >= max(10, int(bench_picks * 0.6))
    ]
    default_pool = robust_candidates if len(robust_candidates) > 0 else ranked
    default_mode = default_pool[0]["id"] if len(default_pool) > 0 else "totals_benchmark_v1"

    conservative_ranked = sorted(
        (robust_candidates if len(robust_candidates) > 0 else variant_rows),
        key=lambda r: (
            r["rolling"]["min_hit_rate"] if r["rolling"]["min_hit_rate"] is not None else -1.0,
            -(r["rolling"]["hit_rate_stddev"] if r["rolling"]["hit_rate_stddev"] is not None else 1e9),
            r["hit_rate"] if r["hit_rate"] is not None else -1.0,
            r["picks"]
        ),
        reverse=True
    )
    conservative_mode = conservative_ranked[0]["id"] if len(conservative_ranked) > 0 else "totals_benchmark_v1"

    replace_benchmark = False
    if bench_row is not None and len(top3) > 0:
        top = top3[0]
        enough_sample = top["picks"] >= 10 and top["picks"] >= max(5, int(bench_row["picks"] * 0.8))
        replace_benchmark = (
            enough_sample and
            top["id"] != "totals_benchmark_v1" and
            top["hit_rate"] is not None and
            bench_row["hit_rate"] is not None and
            top["hit_rate"] >= bench_row["hit_rate"] and
            top["rolling"]["min_hit_rate"] is not None and
            bench_row["rolling"]["min_hit_rate"] is not None and
            top["rolling"]["min_hit_rate"] >= bench_row["rolling"]["min_hit_rate"]
        )

    benchmark_rolling = bench_row["rolling"] if bench_row is not None else None
    return {
        "benchmark_variant": BENCHMARK_CONFIG["model_variant"],
        "benchmark": {
            "picks": bench_row["picks"] if bench_row is not None else 0,
            "hit_rate": bench_row["hit_rate"] if bench_row is not None else None,
            "over_picks": bench_row["over_picks"] if bench_row is not None else 0,
            "over_hit_rate": bench_row["over_hit_rate"] if bench_row is not None else None,
            "under_picks": bench_row["under_picks"] if bench_row is not None else 0,
            "under_hit_rate": bench_row["under_hit_rate"] if bench_row is not None else None
        },
        "filters": simple_filter_rows,
        "best_strategy": (dict(top3[0], strategy=top3[0]["id"]) if len(top3) > 0 else None),
        "rolling_window": {
            "strategy": "totals_benchmark_v1",
            "window_size": max(5, min(20, (bench_row["picks"] // 3) if bench_row is not None and bench_row["picks"] >= 9 else (bench_row["picks"] if bench_row is not None else 5))),
            "stride": 0,
            "summary": benchmark_rolling if benchmark_rolling is not None else {
                "average_hit_rate": None, "min_hit_rate": None, "max_hit_rate": None, "hit_rate_stddev": None, "pct_windows_below_60": None, "pct_windows_below_55": None
            },
            "windows_below_60_pct": benchmark_rolling["pct_windows_below_60"] if benchmark_rolling is not None else None,
            "windows_below_55_pct": benchmark_rolling["pct_windows_below_55"] if benchmark_rolling is not None else None,
        },
        "comparison_harness": {
            "benchmark_id": BENCHMARK_CONFIG["id"],
            "benchmark_config": BENCHMARK_CONFIG,
            "variants": ranked,
            "top3": top3,
            "recommendation": {
                "default_mode": default_mode,
                "conservative_mode": conservative_mode,
                "replace_benchmark": replace_benchmark
            }
        }
    }

y_test = np.array([int(r["over_hit"]) for r in test_rows], dtype=np.int32)
over_count = int(np.sum(y_test == 1))
under_count = int(np.sum(y_test == 0))

result = {
    "dataset_size": len(rows),
    "train_size": len(train_rows),
    "test_size": len(test_rows),
    "class_balance": {
        "over_count": over_count,
        "under_count": under_count,
        "over_rate": (over_count / len(y_test)) if len(y_test) > 0 else None
    },
    "variants": variant_results,
    "conclusions": {
        "strongest_variant": best_by_combined(variant_results),
        "best_balanced_variant": best_by_balance(variant_results)
    },
    "post_filtering": eval_post_filters(baseline_payload)
}
print(json.dumps(result))
`;

  const tempScriptPath = join(
    tmpdir(),
    `bluey_game_total_${Date.now()}_${Math.random().toString(36).slice(2)}.py`,
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

    const proc = spawn("python", [tempScriptPath], { stdio: ["pipe", "pipe", "pipe"] });
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
  console.log("\\n=== Game Total O/U Model Study ===\\n");
  console.log(`Dataset size: ${result.dataset_size}`);
  console.log(`Train size (2023-2024): ${result.train_size}`);
  console.log(`Test size (2025): ${result.test_size}`);
  console.log("\\nClass balance (2025 full):");
  console.log(`  Over:  ${result.class_balance.over_count}`);
  console.log(`  Under: ${result.class_balance.under_count}`);
  console.log(`  Over rate: ${fmtPct(result.class_balance.over_rate)}`);

  console.log("\\nVariant comparison (fixed thresholds OVER>0.60, UNDER<0.40):");
  console.log(
    "  variant | overall | top20 conf | top15 conf | top10 conf | over>0.60 | under<0.40 | combined",
  );
  for (const variant of result.variants) {
    const m = variant.metrics;
    console.log(
      `  ${variant.variant} | ${fmtPct(m.overall_accuracy)} | ${fmtPct(m.top20_confidence_accuracy.accuracy)} (n=${m.top20_confidence_accuracy.count})` +
        ` | ${fmtPct(m.top15_confidence_accuracy.accuracy)} (n=${m.top15_confidence_accuracy.count})` +
        ` | ${fmtPct(m.top10_confidence_accuracy.accuracy)} (n=${m.top10_confidence_accuracy.count})` +
        ` | ${fmtPct(m.over_p_gt_0_60.accuracy)} (n=${m.over_p_gt_0_60.count})` +
        ` | ${fmtPct(m.under_p_lt_0_40.accuracy)} (n=${m.under_p_lt_0_40.count})` +
        ` | ${fmtPct(m.combined_strategy.accuracy)} (n=${m.combined_strategy.count})`,
    );
  }

  console.log("\\nValidation-based threshold search (2025 early -> remaining holdout):");
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

  console.log("\\nConclusions:");
  console.log(`  Strongest variant: ${result.conclusions.strongest_variant}`);
  console.log(`  Best balanced variant: ${result.conclusions.best_balanced_variant}`);

  const pf = result.post_filtering as any;
  console.log("\\nPost-filtering (benchmark = totals_benchmark_v1 on total_plus_team_form):");
  console.log(
    `  Benchmark: picks=${pf.benchmark.picks}, hit=${fmtPct(pf.benchmark.hit_rate)}, ` +
      `over=${fmtPct(pf.benchmark.over_hit_rate)} (n=${pf.benchmark.over_picks}), ` +
      `under=${fmtPct(pf.benchmark.under_hit_rate)} (n=${pf.benchmark.under_picks})`,
  );
  console.log("  Filters:");
  for (const row of pf.filters) {
    console.log(
      `  ${row.strategy} | picks=${row.picks} | hit=${fmtPct(row.hit_rate)} | delta=${fmtDeltaPct(row.delta_vs_benchmark)} | ` +
        `over=${fmtPct(row.over_hit_rate)} (n=${row.over_picks}) | under=${fmtPct(row.under_hit_rate)} (n=${row.under_picks})`,
    );
  }
  if (pf.best_strategy) {
    console.log(
      `  Best strategy: ${pf.best_strategy.strategy} | hit=${fmtPct(pf.best_strategy.hit_rate)} | picks=${pf.best_strategy.picks} | delta=${fmtDeltaPct(pf.best_strategy.delta_vs_benchmark)}`,
    );
  } else {
    console.log("  Best strategy: n/a");
  }
  if (pf.rolling_window) {
    console.log(
      `  Rolling (${pf.rolling_window.strategy}): avg=${fmtPct(pf.rolling_window.summary.average_hit_rate)}, ` +
        `min=${fmtPct(pf.rolling_window.summary.min_hit_rate)}, max=${fmtPct(pf.rolling_window.summary.max_hit_rate)}, ` +
        `stddev=${fmtPct(pf.rolling_window.summary.hit_rate_stddev)}, ` +
        `windows<60%=${pf.rolling_window.windows_below_60_pct}, windows<55%=${pf.rolling_window.windows_below_55_pct}`,
    );
  }

  const harness = pf.comparison_harness;
  if (harness) {
    console.log("\\nBenchmark lock:");
    console.log(`  id: ${harness.benchmark_id}`);
    console.log(`  config: ${JSON.stringify(harness.benchmark_config)}`);

    console.log("\\nRanked variants (priority: rolling min, hit rate, lower stddev, picks):");
    console.log("  id | picks | hit | roll min | roll std | cold streak | <60% windows");
    for (const row of harness.variants as any[]) {
      console.log(
        `  ${row.id} | ${row.picks} | ${fmtPct(row.hit_rate)} | ${fmtPct(row.rolling?.min_hit_rate ?? null)} | ` +
          `${fmtPct(row.rolling?.hit_rate_stddev ?? null)} | ${row.longest_cold_streak ?? 0} | ` +
          `${row.rolling?.pct_windows_below_60 == null ? "n/a" : `${row.rolling.pct_windows_below_60.toFixed(1)}%`}`,
      );
    }

    console.log("\\nTop 3 variants:");
    for (const row of harness.top3 as any[]) {
      console.log(
        `  ${row.id} | picks=${row.picks} | hit=${fmtPct(row.hit_rate)} | ` +
          `roll_min=${fmtPct(row.rolling?.min_hit_rate ?? null)} | roll_std=${fmtPct(row.rolling?.hit_rate_stddev ?? null)}`,
      );
    }

    console.log("\\nRecommendation:");
    console.log(`  default mode (balanced): ${harness.recommendation.default_mode}`);
    console.log(`  conservative mode (max stability): ${harness.recommendation.conservative_mode}`);
    console.log(`  replace benchmark: ${harness.recommendation.replace_benchmark ? "yes" : "no"}`);
  }
}

export async function runGameTotalModel(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const learningRate = flags.learningRate ? Number(flags.learningRate) : 0.05;
  const numEstimators = flags.nEstimators ? Number(flags.nEstimators) : 300;

  if (!Number.isFinite(learningRate) || learningRate <= 0) {
    throw new Error("Invalid --learningRate (must be > 0)");
  }
  if (!Number.isFinite(numEstimators) || numEstimators <= 0) {
    throw new Error("Invalid --nEstimators (must be > 0)");
  }

  console.log("Building game-total O/U training dataset from existing tables...");
  const rows = await buildDatasetRows();
  if (rows.length === 0) {
    throw new Error("No eligible rows found. Ensure completed games with totals lines exist.");
  }

  console.log(`Fetched ${rows.length} rows. Training LightGBM total baseline...`);
  const result = await runPythonLightGbm(rows, learningRate, numEstimators);
  printResults(result);
}

export async function runGameTotalCompare(args: string[]): Promise<void> {
  await runGameTotalModel(args);
}

