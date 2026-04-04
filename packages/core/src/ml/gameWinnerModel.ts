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
  home_win: number;
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
  total_over: number | null;
  ml_home: number | null;
  ml_away: number | null;
}

interface ModelInputRow {
  game_id: string;
  date: string;
  season: number;
  home_win: number;
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
  total_over: number | null;
  ml_home: number | null;
  ml_away: number | null;
  // engineered deltas
  rank_off_delta: number | null;
  rank_def_delta: number | null;
  ppg_delta: number | null;
  oppg_delta: number | null;
  rest_delta: number | null;
  injury_out_delta: number | null;
  lineup_certainty_delta: number | null;
  late_scratch_risk_delta: number | null;
  spread_abs: number | null;
  market_implied_home: number | null;
  market_implied_away: number | null;
}

type PythonMetric = {
  accuracy: number | null;
  count: number;
};

type PythonVariant = {
  variant: string;
  feature_count: number;
  metrics: {
    overall_accuracy: number | null;
    top20_confidence_accuracy: PythonMetric;
    top15_confidence_accuracy: PythonMetric;
    top10_confidence_accuracy: PythonMetric;
    home_p_gt_0_60: PythonMetric;
    away_p_lt_0_40: PythonMetric;
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
      home: number;
      away: number;
      validation_accuracy: number | null;
      validation_count: number;
    };
    holdout_metrics: {
      home_picks: PythonMetric;
      away_picks: PythonMetric;
      combined: PythonMetric;
    };
  };
};

type PythonResult = {
  dataset_size: number;
  train_size: number;
  test_size: number;
  class_balance: {
    home_win_count: number;
    away_win_count: number;
    home_win_rate: number | null;
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
      home_picks: number;
      home_hit_rate: number | null;
      away_picks: number;
      away_hit_rate: number | null;
    };
    filters: Array<{
      strategy: string;
      picks: number;
      hit_rate: number | null;
      home_picks: number;
      home_hit_rate: number | null;
      away_picks: number;
      away_hit_rate: number | null;
      delta_vs_benchmark: number | null;
    }>;
    best_strategy: {
      strategy: string;
      picks: number;
      hit_rate: number | null;
      home_picks: number;
      home_hit_rate: number | null;
      away_picks: number;
      away_hit_rate: number | null;
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

function impliedProbFromAmerican(american: number | null): number | null {
  if (american == null || !Number.isFinite(american) || american === 0) return null;
  if (american > 0) return 100 / (american + 100);
  return (-american) / ((-american) + 100);
}

async function buildDatasetRows(): Promise<ModelInputRow[]> {
  const rows = await prisma.$queryRawUnsafe<RawDatasetRow[]>(`
    SELECT
      g.id AS game_id,
      g.date AS date,
      g.season AS season,
      g."homeScore" AS home_score,
      g."awayScore" AS away_score,
      CASE WHEN g."homeScore" > g."awayScore" THEN 1 ELSE 0 END AS home_win,
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
      go_best."spreadHome" AS spread_home,
      go_best."totalOver" AS total_over,
      go_best."mlHome" AS ml_home,
      go_best."mlAway" AS ml_away
    FROM "Game" g
    LEFT JOIN "GameContext" gc
      ON gc."gameId" = g.id
    LEFT JOIN LATERAL (
      SELECT go.*
      FROM "GameOdds" go
      WHERE go."gameId" = g.id
      ORDER BY
        CASE WHEN go.source = 'consensus' THEN 0 ELSE 1 END,
        go."fetchedAt" DESC
      LIMIT 1
    ) go_best ON TRUE
    WHERE g.season IN (2023, 2024, 2025)
      AND g."homeScore" > 0
      AND g."awayScore" > 0
      AND g.status ILIKE '%Final%'
    ORDER BY g.date ASC, g.id ASC;
  `);

  return rows.map((row) => {
    const homeImplied = impliedProbFromAmerican(row.ml_home);
    const awayImplied = impliedProbFromAmerican(row.ml_away);
    return {
      game_id: row.game_id,
      date: row.date instanceof Date ? row.date.toISOString().slice(0, 10) : String(row.date).slice(0, 10),
      season: row.season,
      home_win: row.home_win,
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
      total_over: row.total_over,
      ml_home: row.ml_home,
      ml_away: row.ml_away,
      rank_off_delta:
        row.home_rank_off != null && row.away_rank_off != null
          ? row.away_rank_off - row.home_rank_off
          : null,
      rank_def_delta:
        row.home_rank_def != null && row.away_rank_def != null
          ? row.away_rank_def - row.home_rank_def
          : null,
      ppg_delta:
        row.home_ppg != null && row.away_ppg != null
          ? row.home_ppg - row.away_ppg
          : null,
      oppg_delta:
        row.home_oppg != null && row.away_oppg != null
          ? row.away_oppg - row.home_oppg
          : null,
      rest_delta:
        row.home_rest_days != null && row.away_rest_days != null
          ? row.home_rest_days - row.away_rest_days
          : null,
      injury_out_delta:
        row.home_injury_out_count != null && row.away_injury_out_count != null
          ? row.away_injury_out_count - row.home_injury_out_count
          : null,
      lineup_certainty_delta:
        row.home_lineup_certainty != null && row.away_lineup_certainty != null
          ? row.home_lineup_certainty - row.away_lineup_certainty
          : null,
      late_scratch_risk_delta:
        row.home_late_scratch_risk != null && row.away_late_scratch_risk != null
          ? row.away_late_scratch_risk - row.home_late_scratch_risk
          : null,
      spread_abs: row.spread_home == null ? null : Math.abs(row.spread_home),
      market_implied_home: homeImplied,
      market_implied_away: awayImplied,
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
    "rank_off_delta", "rank_def_delta",
    "ppg_delta", "oppg_delta",
    "rest_delta",
    "home_is_b2b", "away_is_b2b",
    "injury_out_delta",
    "lineup_certainty_delta",
    "late_scratch_risk_delta",
]
market_features = [
    "spread_home", "spread_abs", "total_over",
    "ml_home", "ml_away",
    "market_implied_home", "market_implied_away",
]
raw_context_features = [
    "home_rank_off", "away_rank_off", "home_rank_def", "away_rank_def",
    "home_ppg", "away_ppg", "home_oppg", "away_oppg",
    "home_rest_days", "away_rest_days",
    "home_lineup_certainty", "away_lineup_certainty",
    "home_late_scratch_risk", "away_late_scratch_risk",
    "home_injury_out_count", "away_injury_out_count",
]

variants = [
    ("winner_baseline_deltas", baseline_features),
    ("winner_plus_market_context", baseline_features + market_features),
    ("winner_plus_raw_context", baseline_features + raw_context_features),
    ("winner_all_features", baseline_features + market_features + raw_context_features),
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

def metric_accuracy(p_home, y_true, home_threshold=0.60, away_threshold=0.40):
    preds = [1 if p >= 0.5 else 0 for p in p_home]
    n = len(y_true)
    overall_acc = None
    if n > 0:
        overall_acc = sum(1 for i in range(n) if preds[i] == y_true[i]) / n

    conf = [abs(p - 0.5) for p in p_home]
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

    home_idxs = [i for i, p in enumerate(p_home) if p > home_threshold]
    away_idxs = [i for i, p in enumerate(p_home) if p < away_threshold]
    home_good = sum(1 for i in home_idxs if y_true[i] == 1)
    away_good = sum(1 for i in away_idxs if y_true[i] == 0)
    combined_count = len(home_idxs) + len(away_idxs)
    combined_good = home_good + away_good

    return {
        "overall_accuracy": overall_acc,
        "top20_confidence_accuracy": acc_for(top20),
        "top15_confidence_accuracy": acc_for(top15),
        "top10_confidence_accuracy": acc_for(top10),
        "home_p_gt_0_60": {
            "accuracy": (home_good / len(home_idxs)) if len(home_idxs) > 0 else None,
            "count": len(home_idxs)
        },
        "away_p_lt_0_40": {
            "accuracy": (away_good / len(away_idxs)) if len(away_idxs) > 0 else None,
            "count": len(away_idxs)
        },
        "combined_strategy": {
            "accuracy": (combined_good / combined_count) if combined_count > 0 else None,
            "count": combined_count
        }
    }

def threshold_search(val_probs, val_y, holdout_probs, holdout_y):
    best = {
        "home": 0.60,
        "away": 0.40,
        "validation_accuracy": None,
        "validation_count": 0
    }
    home_grid = [round(x, 2) for x in np.arange(0.55, 0.81, 0.02)]
    away_grid = [round(x, 2) for x in np.arange(0.20, 0.46, 0.02)]
    for h in home_grid:
        for a in away_grid:
            if a >= h:
                continue
            home_idxs = [i for i, p in enumerate(val_probs) if p > h]
            away_idxs = [i for i, p in enumerate(val_probs) if p < a]
            count = len(home_idxs) + len(away_idxs)
            if count == 0:
                continue
            good = sum(1 for i in home_idxs if val_y[i] == 1) + sum(1 for i in away_idxs if val_y[i] == 0)
            acc = good / count
            current_best_acc = best["validation_accuracy"] if best["validation_accuracy"] is not None else -1.0
            if (acc > current_best_acc) or (abs(acc - current_best_acc) < 1e-12 and count > best["validation_count"]):
                best = {
                    "home": h,
                    "away": a,
                    "validation_accuracy": acc,
                    "validation_count": count
                }

    home_idxs_h = [i for i, p in enumerate(holdout_probs) if p > best["home"]]
    away_idxs_h = [i for i, p in enumerate(holdout_probs) if p < best["away"]]
    home_good_h = sum(1 for i in home_idxs_h if holdout_y[i] == 1)
    away_good_h = sum(1 for i in away_idxs_h if holdout_y[i] == 0)
    combined_count_h = len(home_idxs_h) + len(away_idxs_h)
    combined_good_h = home_good_h + away_good_h
    return {
        "best_thresholds": best,
        "holdout_metrics": {
            "home_picks": {
                "accuracy": (home_good_h / len(home_idxs_h)) if len(home_idxs_h) > 0 else None,
                "count": len(home_idxs_h)
            },
            "away_picks": {
                "accuracy": (away_good_h / len(away_idxs_h)) if len(away_idxs_h) > 0 else None,
                "count": len(away_idxs_h)
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
    y_train = np.array([int(r["home_win"]) for r in train_rows], dtype=np.int32)
    X_test = np.array([vec(r, feature_keys) for r in test_rows], dtype=np.float32)
    y_test = np.array([int(r["home_win"]) for r in test_rows], dtype=np.int32)
    X_val = np.array([vec(r, feature_keys) for r in val_rows], dtype=np.float32)
    y_val = np.array([int(r["home_win"]) for r in val_rows], dtype=np.int32)
    X_holdout = np.array([vec(r, feature_keys) for r in holdout_rows], dtype=np.float32)
    y_holdout = np.array([int(r["home_win"]) for r in holdout_rows], dtype=np.int32)

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
    if variant_name == "winner_plus_market_context":
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
    if payload is None:
        return {
            "benchmark_variant": "winner_plus_market_context",
            "benchmark": {
                "picks": 0, "hit_rate": None, "home_picks": 0, "home_hit_rate": None, "away_picks": 0, "away_hit_rate": None
            },
            "filters": [],
            "best_strategy": None,
            "rolling_window": None
        }
    rows_local = payload["rows"]
    probs = payload["probs"]
    y_true = payload["y_test"]
    conf = [abs(float(p) - 0.5) for p in probs]
    order = sorted(range(len(probs)), key=lambda i: conf[i], reverse=True)

    def eval_indices(name, idxs):
        if len(idxs) == 0:
            return {
                "strategy": name, "picks": 0, "hit_rate": None,
                "home_picks": 0, "home_hit_rate": None,
                "away_picks": 0, "away_hit_rate": None,
                "delta_vs_benchmark": None
            }
        preds = [1 if probs[i] >= 0.5 else 0 for i in idxs]
        good = sum(1 for j, i in enumerate(idxs) if preds[j] == y_true[i])
        home_idxs = [i for i in idxs if probs[i] >= 0.5]
        away_idxs = [i for i in idxs if probs[i] < 0.5]
        home_good = sum(1 for i in home_idxs if y_true[i] == 1)
        away_good = sum(1 for i in away_idxs if y_true[i] == 0)
        return {
            "strategy": name,
            "picks": len(idxs),
            "hit_rate": good / len(idxs),
            "home_picks": len(home_idxs),
            "home_hit_rate": (home_good / len(home_idxs)) if len(home_idxs) > 0 else None,
            "away_picks": len(away_idxs),
            "away_hit_rate": (away_good / len(away_idxs)) if len(away_idxs) > 0 else None,
            "delta_vs_benchmark": None
        }

    top15_n = max(1, int(len(order) * 0.15))
    benchmark_idxs = order[:top15_n]
    benchmark = eval_indices("top15_conf_no_filter", benchmark_idxs)
    benchmark_hit = benchmark["hit_rate"]

    def pass_lineup(i):
        h = rows_local[i].get("home_lineup_certainty")
        a = rows_local[i].get("away_lineup_certainty")
        h_ok = (h is None) or float(h) >= 0.60
        a_ok = (a is None) or float(a) >= 0.60
        return h_ok and a_ok

    def pass_scratch(i):
        h = rows_local[i].get("home_late_scratch_risk")
        a = rows_local[i].get("away_late_scratch_risk")
        h_ok = (h is None) or float(h) <= 0.35
        a_ok = (a is None) or float(a) <= 0.35
        return h_ok and a_ok

    def pass_market_extreme(i):
        s = rows_local[i].get("spread_abs")
        return (s is None) or (float(s) <= 12.5)

    strategy_rows = []
    for name, predicate in [
        ("top15_conf_lineup_filter", pass_lineup),
        ("top15_conf_scratch_filter", pass_scratch),
        ("top15_conf_market_extreme_filter", pass_market_extreme),
        ("top15_conf_combined_filter", lambda i: pass_lineup(i) and pass_scratch(i) and pass_market_extreme(i)),
        ("top20_conf_combined_filter", lambda i: (i in set(order[:max(1, int(len(order) * 0.20))])) and pass_lineup(i) and pass_scratch(i) and pass_market_extreme(i)),
    ]:
        if name.startswith("top20"):
            base = order[:max(1, int(len(order) * 0.20))]
            idxs = [i for i in base if pass_lineup(i) and pass_scratch(i) and pass_market_extreme(i)]
        else:
            idxs = [i for i in benchmark_idxs if predicate(i)]
        rec = eval_indices(name, idxs)
        if rec["hit_rate"] is not None and benchmark_hit is not None:
            rec["delta_vs_benchmark"] = rec["hit_rate"] - benchmark_hit
        strategy_rows.append(rec)

    valid = [r for r in strategy_rows if r["hit_rate"] is not None]
    best_strategy = None
    if len(valid) > 0:
        best_strategy = max(valid, key=lambda r: (r["hit_rate"], r["picks"]))

    rolling = None
    if best_strategy is not None:
        if best_strategy["strategy"] == "top20_conf_combined_filter":
            idxs = [i for i in order[:max(1, int(len(order) * 0.20))] if pass_lineup(i) and pass_scratch(i) and pass_market_extreme(i)]
        elif best_strategy["strategy"] == "top15_conf_combined_filter":
            idxs = [i for i in benchmark_idxs if pass_lineup(i) and pass_scratch(i) and pass_market_extreme(i)]
        elif best_strategy["strategy"] == "top15_conf_lineup_filter":
            idxs = [i for i in benchmark_idxs if pass_lineup(i)]
        elif best_strategy["strategy"] == "top15_conf_scratch_filter":
            idxs = [i for i in benchmark_idxs if pass_scratch(i)]
        elif best_strategy["strategy"] == "top15_conf_market_extreme_filter":
            idxs = [i for i in benchmark_idxs if pass_market_extreme(i)]
        else:
            idxs = benchmark_idxs

        chrono = sorted(
            idxs,
            key=lambda i: (
                str(rows_local[i].get("date") or ""),
                str(rows_local[i].get("game_id") or ""),
            ),
        )
        window_size = 60
        stride = 30
        rates = []
        start = 0
        while start < len(chrono):
            end = min(start + window_size, len(chrono))
            bucket = chrono[start:end]
            if len(bucket) == 0:
                break
            preds = [1 if probs[i] >= 0.5 else 0 for i in bucket]
            good = sum(1 for j, i in enumerate(bucket) if preds[j] == y_true[i])
            rates.append(good / len(bucket))
            if end == len(chrono):
                break
            start += stride
        if len(rates) > 0:
            avg_hit = sum(rates) / len(rates)
            min_hit = min(rates)
            max_hit = max(rates)
            var_hit = sum((h - avg_hit) * (h - avg_hit) for h in rates) / len(rates)
            std_hit = float(np.sqrt(var_hit))
        else:
            avg_hit = None
            min_hit = None
            max_hit = None
            std_hit = None
        rolling = {
            "strategy": best_strategy["strategy"],
            "window_size": window_size,
            "stride": stride,
            "summary": {
                "average_hit_rate": avg_hit,
                "min_hit_rate": min_hit,
                "max_hit_rate": max_hit,
                "hit_rate_stddev": std_hit
            },
            "windows_below_60_pct": sum(1 for r in rates if r < 0.60),
            "windows_below_55_pct": sum(1 for r in rates if r < 0.55),
        }

    return {
        "benchmark_variant": "winner_plus_market_context",
        "benchmark": benchmark,
        "filters": strategy_rows,
        "best_strategy": best_strategy,
        "rolling_window": rolling,
    }

y_test = np.array([int(r["home_win"]) for r in test_rows], dtype=np.int32)
home_wins = int(np.sum(y_test == 1))
away_wins = int(np.sum(y_test == 0))

result = {
    "dataset_size": len(rows),
    "train_size": len(train_rows),
    "test_size": len(test_rows),
    "class_balance": {
        "home_win_count": home_wins,
        "away_win_count": away_wins,
        "home_win_rate": (home_wins / len(y_test)) if len(y_test) > 0 else None
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
    `bluey_game_winner_${Date.now()}_${Math.random().toString(36).slice(2)}.py`,
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
  console.log("\\n=== Game Winner Model Study ===\\n");
  console.log(`Dataset size: ${result.dataset_size}`);
  console.log(`Train size (2023-2024): ${result.train_size}`);
  console.log(`Test size (2025): ${result.test_size}`);
  console.log("\\nClass balance (2025 full):");
  console.log(`  Home wins: ${result.class_balance.home_win_count}`);
  console.log(`  Away wins: ${result.class_balance.away_win_count}`);
  console.log(`  Home win rate: ${fmtPct(result.class_balance.home_win_rate)}`);

  console.log("\\nVariant comparison (fixed thresholds HOME>0.60, AWAY<0.40):");
  console.log(
    "  variant | overall | top20 conf | top15 conf | top10 conf | home>0.60 | away<0.40 | combined",
  );
  for (const variant of result.variants) {
    const m = variant.metrics;
    console.log(
      `  ${variant.variant} | ${fmtPct(m.overall_accuracy)} | ${fmtPct(m.top20_confidence_accuracy.accuracy)} (n=${m.top20_confidence_accuracy.count})` +
        ` | ${fmtPct(m.top15_confidence_accuracy.accuracy)} (n=${m.top15_confidence_accuracy.count})` +
        ` | ${fmtPct(m.top10_confidence_accuracy.accuracy)} (n=${m.top10_confidence_accuracy.count})` +
        ` | ${fmtPct(m.home_p_gt_0_60.accuracy)} (n=${m.home_p_gt_0_60.count})` +
        ` | ${fmtPct(m.away_p_lt_0_40.accuracy)} (n=${m.away_p_lt_0_40.count})` +
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
      `    Best thresholds on validation: HOME>${t.best_thresholds.home.toFixed(2)}, AWAY<${t.best_thresholds.away.toFixed(2)} | acc=${fmtPct(t.best_thresholds.validation_accuracy)} (n=${t.best_thresholds.validation_count})`,
    );
    console.log(
      `    Holdout combined: ${fmtPct(t.holdout_metrics.combined.accuracy)} (n=${t.holdout_metrics.combined.count}), ` +
        `HOME=${fmtPct(t.holdout_metrics.home_picks.accuracy)} (n=${t.holdout_metrics.home_picks.count}), ` +
        `AWAY=${fmtPct(t.holdout_metrics.away_picks.accuracy)} (n=${t.holdout_metrics.away_picks.count})`,
    );
  }

  console.log("\\nConclusions:");
  console.log(`  Strongest variant: ${result.conclusions.strongest_variant}`);
  console.log(`  Best balanced variant: ${result.conclusions.best_balanced_variant}`);

  const pf = result.post_filtering;
  console.log("\\nPost-filtering (benchmark = top15_conf on winner_plus_market_context):");
  console.log(
    `  Benchmark: picks=${pf.benchmark.picks}, hit=${fmtPct(pf.benchmark.hit_rate)}, ` +
      `home=${fmtPct(pf.benchmark.home_hit_rate)} (n=${pf.benchmark.home_picks}), ` +
      `away=${fmtPct(pf.benchmark.away_hit_rate)} (n=${pf.benchmark.away_picks})`,
  );
  console.log("  Filters:");
  for (const row of pf.filters) {
    console.log(
      `  ${row.strategy} | picks=${row.picks} | hit=${fmtPct(row.hit_rate)} | delta=${fmtDeltaPct(row.delta_vs_benchmark)} | ` +
        `home=${fmtPct(row.home_hit_rate)} (n=${row.home_picks}) | away=${fmtPct(row.away_hit_rate)} (n=${row.away_picks})`,
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
}

export async function runGameWinnerModel(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const learningRate = flags.learningRate ? Number(flags.learningRate) : 0.05;
  const numEstimators = flags.nEstimators ? Number(flags.nEstimators) : 300;

  if (!Number.isFinite(learningRate) || learningRate <= 0) {
    throw new Error("Invalid --learningRate (must be > 0)");
  }
  if (!Number.isFinite(numEstimators) || numEstimators <= 0) {
    throw new Error("Invalid --nEstimators (must be > 0)");
  }

  console.log("Building game-winner training dataset from existing tables...");
  const rows = await buildDatasetRows();
  if (rows.length === 0) {
    throw new Error("No eligible rows found. Ensure completed games with pregame context exist.");
  }

  console.log(`Fetched ${rows.length} rows. Training LightGBM winner baseline...`);
  const result = await runPythonLightGbm(rows, learningRate, numEstimators);
  printResults(result);
}

