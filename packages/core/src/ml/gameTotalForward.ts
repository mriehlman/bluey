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

type StrategyDefinition = {
  strategyName: string;
  modelVariant: "total_plus_team_form";
  topConfPct: number;
  requireLowVolatility: boolean;
  maxVolatilityStd10: number;
  maxAbsLineMinusCombinedEnv?: number;
  underOnly?: boolean;
  maxTotalLine?: number;
  filterTags: string[];
};

type ForwardRow = {
  game_id: string;
  game_date: string;
  season: number;
  home_team: string;
  away_team: string;
  total_line: number;
  spread_home: number | null;
  expected_total_simple: number | null;
  total_minus_expected_simple: number | null;
  rank_off_sum: number | null;
  rank_def_sum: number | null;
  rest_sum: number | null;
  b2b_count: number | null;
  injury_out_sum: number | null;
  lineup_certainty_min: number | null;
  late_scratch_risk_max: number | null;
  low_team_env_sample_flag: number | null;
  high_team_env_volatility_flag: number | null;
  home_points_for_last_5_avg: number | null;
  away_points_for_last_5_avg: number | null;
  home_points_against_last_5_avg: number | null;
  away_points_against_last_5_avg: number | null;
  combined_team_total_env_last_5: number | null;
  combined_team_total_env_std_10: number | null;
  line_minus_combined_team_env: number | null;
};

type ScoredForwardRow = ForwardRow & {
  p_over: number;
  confidence: number;
};

type ForwardPickRecord = {
  id: string;
  strategyName: string;
  modelVariant: string;
  gameId: string;
  gameDate: Date | string;
  season: number;
  homeTeam: string;
  awayTeam: string;
  pickSide: "OVER" | "UNDER";
  marketLine: number;
  modelScore: number;
  confidence: number;
  rankWithinDay: number;
  selectionReason: string[];
  strategySnapshot: unknown;
  contextDiagnostics: unknown;
  actualTotalPoints: number | null;
  result: "WIN" | "LOSS" | "PUSH" | "PENDING";
  resolvedAt: Date | string | null;
  createdAt: Date | string;
};

export const TOTALS_FORWARD_STRATEGIES: readonly StrategyDefinition[] = [
  {
    strategyName: "totals_benchmark_v1",
    modelVariant: "total_plus_team_form",
    topConfPct: 15,
    requireLowVolatility: true,
    maxVolatilityStd10: 18,
    filterTags: ["top15_conf", "low_volatility"],
  },
  {
    strategyName: "totals_candidate_balanced_v1",
    modelVariant: "total_plus_team_form",
    topConfPct: 15,
    requireLowVolatility: true,
    maxVolatilityStd10: 18,
    maxAbsLineMinusCombinedEnv: 8,
    filterTags: ["top15_conf", "low_volatility", "lineenv_lt_8"],
  },
  {
    strategyName: "totals_candidate_conservative_v1",
    modelVariant: "total_plus_team_form",
    topConfPct: 20,
    requireLowVolatility: true,
    maxVolatilityStd10: 18,
    filterTags: ["top20_conf", "low_volatility"],
  },
] as const;

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

function sqlEsc(v: string): string {
  return v.replaceAll("'", "''");
}

function parseDateFlag(dateFlag: string | undefined): string {
  if (dateFlag && /^\d{4}-\d{2}-\d{2}$/.test(dateFlag)) return dateFlag;
  return new Date().toISOString().slice(0, 10);
}

function totalLineBucket(line: number): "low_le_220" | "mid_220_235" | "high_gt_235" {
  if (line <= 220) return "low_le_220";
  if (line > 235) return "high_gt_235";
  return "mid_220_235";
}

function spreadBucket(spreadAbs: number | null): "close_le_3" | "mid_3_9" | "blowout_ge_9" | "unknown" {
  if (spreadAbs == null || !Number.isFinite(spreadAbs)) return "unknown";
  if (spreadAbs <= 3) return "close_le_3";
  if (spreadAbs >= 9) return "blowout_ge_9";
  return "mid_3_9";
}

async function ensureForwardTable(): Promise<void> {
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "TotalForwardPick" (
      "id" text PRIMARY KEY,
      "strategyName" text NOT NULL,
      "modelVariant" text NOT NULL,
      "gameId" text NOT NULL,
      "gameDate" date NOT NULL,
      "season" int NOT NULL,
      "homeTeam" text NOT NULL,
      "awayTeam" text NOT NULL,
      "pickSide" text NOT NULL,
      "marketLine" double precision NOT NULL,
      "modelScore" double precision NOT NULL,
      "confidence" double precision NOT NULL,
      "rankWithinDay" int NOT NULL,
      "selectionReason" text[] NOT NULL,
      "strategySnapshot" jsonb NOT NULL,
      "contextDiagnostics" jsonb NOT NULL,
      "actualTotalPoints" double precision,
      "result" text NOT NULL DEFAULT 'PENDING',
      "resolvedAt" timestamptz,
      "createdAt" timestamptz NOT NULL DEFAULT NOW(),
      "updatedAt" timestamptz NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(
    `CREATE UNIQUE INDEX IF NOT EXISTS "TotalForwardPick_strategy_game_idx"
     ON "TotalForwardPick" ("strategyName","gameId")`,
  );
  await prisma.$executeRawUnsafe(
    `CREATE INDEX IF NOT EXISTS "TotalForwardPick_strategy_date_idx"
     ON "TotalForwardPick" ("strategyName","gameDate","createdAt")`,
  );
}

async function loadTrainRowsForTotalsModel(beforeDateInclusive: string): Promise<ForwardRow[]> {
  const rows = await prisma.$queryRawUnsafe<ForwardRow[]>(`
    WITH game_odds_best AS (
      SELECT DISTINCT ON (go."gameId")
        go."gameId",
        go."totalOver" as total_line,
        go."spreadHome" as spread_home
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
      WHERE g."homeScore" > 0 AND g."awayScore" > 0 AND g.status ILIKE '%Final%'
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
      WHERE g."homeScore" > 0 AND g."awayScore" > 0 AND g.status ILIKE '%Final%'
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
      to_char(g.date, 'YYYY-MM-DD') AS game_date,
      g.season AS season,
      COALESCE(ht.code, ht.name, CONCAT('T', g."homeTeamId"::text)) AS home_team,
      COALESCE(at.code, at.name, CONCAT('T', g."awayTeamId"::text)) AS away_team,
      go.total_line AS total_line,
      go.spread_home AS spread_home,
      CASE
        WHEN gc."homePpg" IS NOT NULL AND gc."awayPpg" IS NOT NULL AND gc."homeOppg" IS NOT NULL AND gc."awayOppg" IS NOT NULL
        THEN (gc."homePpg" + gc."awayPpg" + gc."homeOppg" + gc."awayOppg") / 2.0
        ELSE NULL
      END AS expected_total_simple,
      CASE
        WHEN gc."homePpg" IS NOT NULL AND gc."awayPpg" IS NOT NULL AND gc."homeOppg" IS NOT NULL AND gc."awayOppg" IS NOT NULL
        THEN go.total_line - ((gc."homePpg" + gc."awayPpg" + gc."homeOppg" + gc."awayOppg") / 2.0)
        ELSE NULL
      END AS total_minus_expected_simple,
      CASE WHEN gc."homeRankOff" IS NOT NULL AND gc."awayRankOff" IS NOT NULL THEN gc."homeRankOff" + gc."awayRankOff" ELSE NULL END AS rank_off_sum,
      CASE WHEN gc."homeRankDef" IS NOT NULL AND gc."awayRankDef" IS NOT NULL THEN gc."homeRankDef" + gc."awayRankDef" ELSE NULL END AS rank_def_sum,
      CASE WHEN gc."homeRestDays" IS NOT NULL AND gc."awayRestDays" IS NOT NULL THEN gc."homeRestDays" + gc."awayRestDays" ELSE NULL END AS rest_sum,
      (CASE WHEN gc."homeIsB2b" THEN 1 ELSE 0 END + CASE WHEN gc."awayIsB2b" THEN 1 ELSE 0 END) AS b2b_count,
      CASE WHEN gc."homeInjuryOutCount" IS NOT NULL AND gc."awayInjuryOutCount" IS NOT NULL THEN gc."homeInjuryOutCount" + gc."awayInjuryOutCount" ELSE NULL END AS injury_out_sum,
      CASE WHEN gc."homeLineupCertainty" IS NOT NULL AND gc."awayLineupCertainty" IS NOT NULL THEN LEAST(gc."homeLineupCertainty", gc."awayLineupCertainty") ELSE NULL END AS lineup_certainty_min,
      CASE WHEN gc."homeLateScratchRisk" IS NOT NULL AND gc."awayLateScratchRisk" IS NOT NULL THEN GREATEST(gc."homeLateScratchRisk", gc."awayLateScratchRisk") ELSE NULL END AS late_scratch_risk_max,
      CASE WHEN home_hist.team_hist_games_10 IS NOT NULL AND away_hist.team_hist_games_10 IS NOT NULL AND home_hist.team_hist_games_10 >= 5 AND away_hist.team_hist_games_10 >= 5 THEN 0 ELSE 1 END AS low_team_env_sample_flag,
      CASE WHEN home_hist.team_total_env_std_10 IS NOT NULL AND away_hist.team_total_env_std_10 IS NOT NULL AND ((home_hist.team_total_env_std_10 + away_hist.team_total_env_std_10)/2.0) >= 18 THEN 1 ELSE 0 END AS high_team_env_volatility_flag,
      home_hist.points_for_last_5_avg AS home_points_for_last_5_avg,
      away_hist.points_for_last_5_avg AS away_points_for_last_5_avg,
      home_hist.points_against_last_5_avg AS home_points_against_last_5_avg,
      away_hist.points_against_last_5_avg AS away_points_against_last_5_avg,
      CASE WHEN home_hist.team_total_env_last_5_avg IS NOT NULL AND away_hist.team_total_env_last_5_avg IS NOT NULL THEN (home_hist.team_total_env_last_5_avg + away_hist.team_total_env_last_5_avg)/2.0 ELSE NULL END AS combined_team_total_env_last_5,
      CASE WHEN home_hist.team_total_env_std_10 IS NOT NULL AND away_hist.team_total_env_std_10 IS NOT NULL THEN (home_hist.team_total_env_std_10 + away_hist.team_total_env_std_10)/2.0 ELSE NULL END AS combined_team_total_env_std_10,
      CASE WHEN home_hist.team_total_env_last_5_avg IS NOT NULL AND away_hist.team_total_env_last_5_avg IS NOT NULL THEN go.total_line - ((home_hist.team_total_env_last_5_avg + away_hist.team_total_env_last_5_avg)/2.0) ELSE NULL END AS line_minus_combined_team_env
    FROM "Game" g
    JOIN game_odds_best go ON go."gameId" = g.id
    LEFT JOIN "GameContext" gc ON gc."gameId" = g.id
    LEFT JOIN "Team" ht ON ht.id = g."homeTeamId"
    LEFT JOIN "Team" at ON at.id = g."awayTeamId"
    LEFT JOIN team_hist home_hist ON home_hist.game_id = g.id AND home_hist.team_id = g."homeTeamId"
    LEFT JOIN team_hist away_hist ON away_hist.game_id = g.id AND away_hist.team_id = g."awayTeamId"
    WHERE g.date <= '${sqlEsc(beforeDateInclusive)}'::date
      AND g."homeScore" > 0
      AND g."awayScore" > 0
      AND g.status ILIKE '%Final%'
      AND go.total_line IS NOT NULL
    ORDER BY g.date ASC, g.id ASC;
  `);
  return rows;
}

async function loadForwardRowsForDate(targetDate: string): Promise<ForwardRow[]> {
  const rows = await prisma.$queryRawUnsafe<ForwardRow[]>(`
    WITH game_odds_best AS (
      SELECT DISTINCT ON (go."gameId")
        go."gameId",
        go."totalOver" as total_line,
        go."spreadHome" as spread_home
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
      WHERE g."homeScore" > 0 AND g."awayScore" > 0 AND g.status ILIKE '%Final%'
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
      WHERE g."homeScore" > 0 AND g."awayScore" > 0 AND g.status ILIKE '%Final%'
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
      to_char(g.date, 'YYYY-MM-DD') AS game_date,
      g.season AS season,
      COALESCE(ht.code, ht.name, CONCAT('T', g."homeTeamId"::text)) AS home_team,
      COALESCE(at.code, at.name, CONCAT('T', g."awayTeamId"::text)) AS away_team,
      go.total_line AS total_line,
      go.spread_home AS spread_home,
      CASE
        WHEN gc."homePpg" IS NOT NULL AND gc."awayPpg" IS NOT NULL AND gc."homeOppg" IS NOT NULL AND gc."awayOppg" IS NOT NULL
        THEN (gc."homePpg" + gc."awayPpg" + gc."homeOppg" + gc."awayOppg") / 2.0
        ELSE NULL
      END AS expected_total_simple,
      CASE
        WHEN gc."homePpg" IS NOT NULL AND gc."awayPpg" IS NOT NULL AND gc."homeOppg" IS NOT NULL AND gc."awayOppg" IS NOT NULL
        THEN go.total_line - ((gc."homePpg" + gc."awayPpg" + gc."homeOppg" + gc."awayOppg") / 2.0)
        ELSE NULL
      END AS total_minus_expected_simple,
      CASE WHEN gc."homeRankOff" IS NOT NULL AND gc."awayRankOff" IS NOT NULL THEN gc."homeRankOff" + gc."awayRankOff" ELSE NULL END AS rank_off_sum,
      CASE WHEN gc."homeRankDef" IS NOT NULL AND gc."awayRankDef" IS NOT NULL THEN gc."homeRankDef" + gc."awayRankDef" ELSE NULL END AS rank_def_sum,
      CASE WHEN gc."homeRestDays" IS NOT NULL AND gc."awayRestDays" IS NOT NULL THEN gc."homeRestDays" + gc."awayRestDays" ELSE NULL END AS rest_sum,
      (CASE WHEN gc."homeIsB2b" THEN 1 ELSE 0 END + CASE WHEN gc."awayIsB2b" THEN 1 ELSE 0 END) AS b2b_count,
      CASE WHEN gc."homeInjuryOutCount" IS NOT NULL AND gc."awayInjuryOutCount" IS NOT NULL THEN gc."homeInjuryOutCount" + gc."awayInjuryOutCount" ELSE NULL END AS injury_out_sum,
      CASE WHEN gc."homeLineupCertainty" IS NOT NULL AND gc."awayLineupCertainty" IS NOT NULL THEN LEAST(gc."homeLineupCertainty", gc."awayLineupCertainty") ELSE NULL END AS lineup_certainty_min,
      CASE WHEN gc."homeLateScratchRisk" IS NOT NULL AND gc."awayLateScratchRisk" IS NOT NULL THEN GREATEST(gc."homeLateScratchRisk", gc."awayLateScratchRisk") ELSE NULL END AS late_scratch_risk_max,
      CASE WHEN home_hist.team_hist_games_10 IS NOT NULL AND away_hist.team_hist_games_10 IS NOT NULL AND home_hist.team_hist_games_10 >= 5 AND away_hist.team_hist_games_10 >= 5 THEN 0 ELSE 1 END AS low_team_env_sample_flag,
      CASE WHEN home_hist.team_total_env_std_10 IS NOT NULL AND away_hist.team_total_env_std_10 IS NOT NULL AND ((home_hist.team_total_env_std_10 + away_hist.team_total_env_std_10)/2.0) >= 18 THEN 1 ELSE 0 END AS high_team_env_volatility_flag,
      home_hist.points_for_last_5_avg AS home_points_for_last_5_avg,
      away_hist.points_for_last_5_avg AS away_points_for_last_5_avg,
      home_hist.points_against_last_5_avg AS home_points_against_last_5_avg,
      away_hist.points_against_last_5_avg AS away_points_against_last_5_avg,
      CASE WHEN home_hist.team_total_env_last_5_avg IS NOT NULL AND away_hist.team_total_env_last_5_avg IS NOT NULL THEN (home_hist.team_total_env_last_5_avg + away_hist.team_total_env_last_5_avg)/2.0 ELSE NULL END AS combined_team_total_env_last_5,
      CASE WHEN home_hist.team_total_env_std_10 IS NOT NULL AND away_hist.team_total_env_std_10 IS NOT NULL THEN (home_hist.team_total_env_std_10 + away_hist.team_total_env_std_10)/2.0 ELSE NULL END AS combined_team_total_env_std_10,
      CASE WHEN home_hist.team_total_env_last_5_avg IS NOT NULL AND away_hist.team_total_env_last_5_avg IS NOT NULL THEN go.total_line - ((home_hist.team_total_env_last_5_avg + away_hist.team_total_env_last_5_avg)/2.0) ELSE NULL END AS line_minus_combined_team_env
    FROM "Game" g
    JOIN game_odds_best go ON go."gameId" = g.id
    LEFT JOIN "GameContext" gc ON gc."gameId" = g.id
    LEFT JOIN "Team" ht ON ht.id = g."homeTeamId"
    LEFT JOIN "Team" at ON at.id = g."awayTeamId"
    LEFT JOIN team_hist home_hist ON home_hist.game_id = g.id AND home_hist.team_id = g."homeTeamId"
    LEFT JOIN team_hist away_hist ON away_hist.game_id = g.id AND away_hist.team_id = g."awayTeamId"
    WHERE g.date = '${sqlEsc(targetDate)}'::date
      AND go.total_line IS NOT NULL
    ORDER BY g.id ASC;
  `);
  return rows;
}

async function scoreForwardRows(args: {
  trainRows: ForwardRow[];
  scoreRows: ForwardRow[];
}): Promise<ScoredForwardRow[]> {
  const pythonScript = `
import json
import sys

try:
    import lightgbm as lgb
except Exception as exc:
    print(json.dumps({"error": "python_lightgbm_import_failed", "detail": str(exc), "hint": "Install Python package: pip install lightgbm"}))
    sys.exit(2)

try:
    import numpy as np
except Exception as exc:
    print(json.dumps({"error": "python_numpy_import_failed", "detail": str(exc), "hint": "Install Python package: pip install numpy"}))
    sys.exit(2)

payload = json.loads(sys.stdin.read())
train_rows = payload["train_rows"]
score_rows = payload["score_rows"]

if len(train_rows) == 0:
    print(json.dumps({"error": "no_train_rows", "detail": "No historical rows available for totals model training"}))
    sys.exit(3)

feature_keys = [
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
    "home_points_for_last_5_avg",
    "away_points_for_last_5_avg",
    "home_points_against_last_5_avg",
    "away_points_against_last_5_avg",
    "expected_total_simple",
    "total_minus_expected_simple",
    "home_team_total_env_last_5_avg",
    "away_team_total_env_last_5_avg"
]

def vec(row):
    vals = []
    for k in feature_keys:
        v = row.get(k)
        vals.append(float("nan") if v is None else float(v))
    return vals

y_train = np.array([int(r.get("over_hit", 0)) for r in train_rows], dtype=np.int32)
X_train = np.array([vec(r) for r in train_rows], dtype=np.float32)

train_dataset = lgb.Dataset(X_train, label=y_train, free_raw_data=True)
params = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 1,
    "seed": 42,
    "verbosity": -1
}
model = lgb.train(params=params, train_set=train_dataset, num_boost_round=300)

if len(score_rows) == 0:
    print(json.dumps({"rows": []}))
    sys.exit(0)

X_score = np.array([vec(r) for r in score_rows], dtype=np.float32)
p_over = [float(v) for v in model.predict(X_score)]

result_rows = []
for i, row in enumerate(score_rows):
    p = float(p_over[i])
    conf = abs(p - 0.5)
    merged = dict(row)
    merged["p_over"] = p
    merged["confidence"] = conf
    result_rows.append(merged)

print(json.dumps({"rows": result_rows}))
`;
  const tempScriptPath = join(
    tmpdir(),
    `bluey_total_forward_${Date.now()}_${Math.random().toString(36).slice(2)}.py`,
  );
  writeFileSync(tempScriptPath, pythonScript, "utf8");

  return new Promise((resolve, reject) => {
    let cleaned = false;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      try {
        unlinkSync(tempScriptPath);
      } catch {
        // best effort
      }
    };
    const proc = spawn("python", [tempScriptPath], { stdio: ["pipe", "pipe", "pipe"] });
    proc.stdin.write(JSON.stringify({ train_rows: args.trainRows, score_rows: args.scoreRows }));
    proc.stdin.end();
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (c: unknown) => {
      stdout += String(c);
    });
    proc.stderr.on("data", (c: unknown) => {
      stderr += String(c);
    });
    proc.on("close", (code: number | null) => {
      cleanup();
      if (code !== 0) {
        reject(new Error(`Forward totals scorer failed (${String(code)}): ${stderr || stdout}`));
        return;
      }
      try {
        const parsed = JSON.parse(stdout) as { error?: string; detail?: string; rows?: ScoredForwardRow[] };
        if (parsed.error) {
          reject(new Error(`${parsed.error}: ${parsed.detail ?? ""}`.trim()));
          return;
        }
        resolve(parsed.rows ?? []);
      } catch {
        reject(new Error(`Failed to parse forward totals scorer output: ${stdout || stderr}`));
      }
    });
    proc.on("error", (err: unknown) => {
      cleanup();
      reject(err);
    });
  });
}

function pickForStrategy(strategy: StrategyDefinition, rows: ScoredForwardRow[]): Array<{
  row: ScoredForwardRow;
  pickSide: "OVER" | "UNDER";
  rankWithinDay: number;
  selectionReason: string[];
}> {
  const ordered = [...rows].sort((a, b) => b.confidence - a.confidence);
  const topCount = Math.max(1, Math.floor((strategy.topConfPct / 100) * ordered.length));
  const candidate = ordered.slice(0, topCount);
  const out: Array<{ row: ScoredForwardRow; pickSide: "OVER" | "UNDER"; rankWithinDay: number; selectionReason: string[] }> = [];
  let rank = 0;
  for (const row of candidate) {
    const reasons = [...strategy.filterTags];
    if (strategy.requireLowVolatility) {
      const vol = row.combined_team_total_env_std_10;
      if (vol != null && vol > strategy.maxVolatilityStd10) continue;
      reasons.push("volatility_ok");
    }
    if (strategy.maxAbsLineMinusCombinedEnv != null) {
      const x = row.line_minus_combined_team_env;
      if (x != null && Math.abs(x) >= strategy.maxAbsLineMinusCombinedEnv) continue;
      reasons.push("lineenv_ok");
    }
    if (strategy.maxTotalLine != null) {
      if (row.total_line > strategy.maxTotalLine) continue;
      reasons.push("total_line_cap_ok");
    }
    const pickSide: "OVER" | "UNDER" = row.p_over >= 0.5 ? "OVER" : "UNDER";
    if (strategy.underOnly && pickSide !== "UNDER") continue;
    rank += 1;
    out.push({ row, pickSide, rankWithinDay: rank, selectionReason: reasons });
  }
  return out;
}

export async function runGameTotalForward(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const dateStr = parseDateFlag(flags.date);
  await ensureForwardTable();

  const trainRows = await loadTrainRowsForTotalsModel(dateStr);
  const scoreRows = await loadForwardRowsForDate(dateStr);
  const scored = await scoreForwardRows({ trainRows, scoreRows });
  if (scored.length === 0) {
    console.log(`No eligible games with total lines found for ${dateStr}.`);
    return;
  }

  console.log(`\\n=== Totals Forward Paper-Trade Generation (${dateStr}) ===`);
  for (const strategy of TOTALS_FORWARD_STRATEGIES) {
    const picks = pickForStrategy(strategy, scored);
    let inserted = 0;
    for (const p of picks) {
      const spreadAbs = p.row.spread_home == null ? null : Math.abs(p.row.spread_home);
      const contextDiagnostics = {
        combined_team_env: p.row.combined_team_total_env_last_5,
        line_minus_combined_team_env: p.row.line_minus_combined_team_env,
        home_team_total_env_last_5_avg: p.row.home_team_total_env_last_5_avg,
        away_team_total_env_last_5_avg: p.row.away_team_total_env_last_5_avg,
        combined_team_total_env_std_10: p.row.combined_team_total_env_std_10,
        spread_bucket: spreadBucket(spreadAbs),
        total_line_bucket: totalLineBucket(p.row.total_line),
      };
      const strategySnapshot = {
        strategyName: strategy.strategyName,
        modelVariant: strategy.modelVariant,
        topConfPct: strategy.topConfPct,
        requireLowVolatility: strategy.requireLowVolatility,
        maxVolatilityStd10: strategy.maxVolatilityStd10,
        maxAbsLineMinusCombinedEnv: strategy.maxAbsLineMinusCombinedEnv ?? null,
        underOnly: strategy.underOnly ?? false,
        maxTotalLine: strategy.maxTotalLine ?? null,
      };
      const selectionArrayLiteral = `{${p.selectionReason.map((r) => `"${r.replaceAll('"', '\\"')}"`).join(",")}}`;
      const result = await prisma.$executeRawUnsafe(
        `INSERT INTO "TotalForwardPick"
         ("id","strategyName","modelVariant","gameId","gameDate","season","homeTeam","awayTeam",
          "pickSide","marketLine","modelScore","confidence","rankWithinDay","selectionReason",
          "strategySnapshot","contextDiagnostics","result")
         VALUES
         ('${crypto.randomUUID()}','${sqlEsc(strategy.strategyName)}','${sqlEsc(strategy.modelVariant)}','${sqlEsc(p.row.game_id)}',
          '${sqlEsc(p.row.game_date)}'::date,${p.row.season},'${sqlEsc(p.row.home_team)}','${sqlEsc(p.row.away_team)}',
          '${p.pickSide}',${p.row.total_line},${p.row.p_over},${p.row.confidence},${p.rankWithinDay},
          '${sqlEsc(selectionArrayLiteral)}'::text[],
          '${sqlEsc(JSON.stringify(strategySnapshot))}'::jsonb,
          '${sqlEsc(JSON.stringify(contextDiagnostics))}'::jsonb,
          'PENDING')
         ON CONFLICT ("strategyName","gameId") DO NOTHING`,
      );
      if (result > 0) inserted += 1;
    }

    console.log(`\\n[${strategy.strategyName}] picks=${picks.length} inserted=${inserted}`);
    if (picks.length === 0) {
      console.log("  No picks for this strategy.");
      continue;
    }
    for (const p of picks) {
      const tags = p.selectionReason.join(",");
      console.log(
        `  ${p.row.away_team} @ ${p.row.home_team} | ${p.pickSide} ${p.row.total_line} | ` +
          `score=${(p.row.p_over * 100).toFixed(1)}% over | conf=${(p.row.confidence * 100).toFixed(1)}% | tags=${tags}`,
      );
    }
  }
}

export async function runGameTotalForwardResolve(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const onlyDate = flags.date;
  await ensureForwardTable();
  const whereDate = onlyDate && /^\\d{4}-\\d{2}-\\d{2}$/.test(onlyDate)
    ? `AND fp."gameDate" = '${sqlEsc(onlyDate)}'::date`
    : "";
  const pending = await prisma.$queryRawUnsafe<Array<{
    id: string;
    strategyName: string;
    gameId: string;
    gameDate: Date | string;
    pickSide: "OVER" | "UNDER";
    marketLine: number;
    homeScore: number;
    awayScore: number;
    status: string | null;
  }>>(
    `SELECT
      fp."id",
      fp."strategyName",
      fp."gameId",
      fp."gameDate",
      fp."pickSide",
      fp."marketLine",
      g."homeScore",
      g."awayScore",
      g."status"
     FROM "TotalForwardPick" fp
     JOIN "Game" g ON g.id = fp."gameId"
     WHERE fp."result" = 'PENDING'
       ${whereDate}
       AND g."homeScore" > 0
       AND g."awayScore" > 0
       AND g.status ILIKE '%Final%'`,
  );

  if (pending.length === 0) {
    console.log("No unresolved picks ready for resolution.");
    return;
  }
  console.log("\\n=== Totals Forward Resolve ===");
  let updated = 0;
  for (const row of pending) {
    const actualTotal = row.homeScore + row.awayScore;
    const result: "WIN" | "LOSS" | "PUSH" =
      actualTotal === row.marketLine
        ? "PUSH"
        : row.pickSide === "OVER"
          ? actualTotal > row.marketLine
            ? "WIN"
            : "LOSS"
          : actualTotal < row.marketLine
            ? "WIN"
            : "LOSS";
    await prisma.$executeRawUnsafe(
      `UPDATE "TotalForwardPick"
       SET "actualTotalPoints" = ${actualTotal},
           "result" = '${result}',
           "resolvedAt" = NOW(),
           "updatedAt" = NOW()
       WHERE "id" = '${sqlEsc(row.id)}'`,
    );
    updated += 1;
    console.log(
      `  [${row.strategyName}] ${row.gameId} | ${row.pickSide} ${row.marketLine} | actual ${actualTotal} -> ${result}`,
    );
  }
  console.log(`Resolved ${updated} picks.`);

  const cumulative = await prisma.$queryRawUnsafe<Array<{
    strategyName: string;
    wins: number;
    losses: number;
    pushes: number;
    total: number;
  }>>(
    `SELECT
      fp."strategyName",
      SUM(CASE WHEN fp."result"='WIN' THEN 1 ELSE 0 END)::int as wins,
      SUM(CASE WHEN fp."result"='LOSS' THEN 1 ELSE 0 END)::int as losses,
      SUM(CASE WHEN fp."result"='PUSH' THEN 1 ELSE 0 END)::int as pushes,
      COUNT(*)::int as total
     FROM "TotalForwardPick" fp
     GROUP BY fp."strategyName"
     ORDER BY fp."strategyName"`,
  );
  console.log("\\nCumulative forward records:");
  for (const c of cumulative) {
    const decisions = c.wins + c.losses;
    const hit = decisions > 0 ? (c.wins / decisions) * 100 : null;
    console.log(
      `  ${c.strategyName}: ${c.wins}-${c.losses}-${c.pushes} (total ${c.total})` +
        `${hit == null ? "" : ` | hit ${hit.toFixed(1)}%`}`,
    );
  }
}

function computeStreaks(results: Array<"WIN" | "LOSS" | "PUSH">): {
  currentStreak: string;
  longestWinStreak: number;
  longestColdStreak: number;
} {
  let longestWin = 0;
  let longestCold = 0;
  let curWin = 0;
  let curCold = 0;
  for (const r of results) {
    if (r === "WIN") {
      curWin += 1;
      curCold = 0;
    } else if (r === "LOSS") {
      curCold += 1;
      curWin = 0;
    } else {
      curWin = 0;
      curCold = 0;
    }
    if (curWin > longestWin) longestWin = curWin;
    if (curCold > longestCold) longestCold = curCold;
  }
  let currentStreak = "none";
  let streakLen = 0;
  for (let i = results.length - 1; i >= 0; i--) {
    const r = results[i];
    if (r === "PUSH") break;
    if (i === results.length - 1) {
      currentStreak = r === "WIN" ? "W" : "L";
      streakLen = 1;
      continue;
    }
    if ((currentStreak === "W" && r === "WIN") || (currentStreak === "L" && r === "LOSS")) {
      streakLen += 1;
    } else {
      break;
    }
  }
  if (currentStreak !== "none") currentStreak = `${currentStreak}${streakLen}`;
  return { currentStreak, longestWinStreak: longestWin, longestColdStreak: longestCold };
}

function fmtPct(n: number | null): string {
  return n == null ? "n/a" : `${(n * 100).toFixed(1)}%`;
}

export async function runGameTotalForwardReport(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const minSample = Number.isFinite(Number(flags.minSample)) ? Math.max(5, Number(flags.minSample)) : 20;
  await ensureForwardTable();
  const rows = await prisma.$queryRawUnsafe<ForwardPickRecord[]>(
    `SELECT * FROM "TotalForwardPick" ORDER BY "createdAt" ASC`,
  );
  if (rows.length === 0) {
    console.log("No forward-tracked totals picks found.");
    return;
  }

  const byStrategy = new Map<string, ForwardPickRecord[]>();
  for (const row of rows) {
    if (!byStrategy.has(row.strategyName)) byStrategy.set(row.strategyName, []);
    byStrategy.get(row.strategyName)!.push(row);
  }

  type Summary = {
    strategy: string;
    total: number;
    wins: number;
    losses: number;
    pushes: number;
    hitRate: number | null;
    overHitRate: number | null;
    underHitRate: number | null;
    currentStreak: string;
    longestWinStreak: number;
    longestColdStreak: number;
    last7HitRate: number | null;
    last14HitRate: number | null;
    lineBuckets: Record<string, number>;
    spreadBuckets: Record<string, number>;
  };
  const summaries: Summary[] = [];

  for (const [strategy, picks] of byStrategy.entries()) {
    const decided = picks.filter((p) => p.result === "WIN" || p.result === "LOSS" || p.result === "PUSH");
    const wins = decided.filter((p) => p.result === "WIN").length;
    const losses = decided.filter((p) => p.result === "LOSS").length;
    const pushes = decided.filter((p) => p.result === "PUSH").length;
    const decisions = wins + losses;
    const overDecisions = decided.filter((p) => p.pickSide === "OVER" && (p.result === "WIN" || p.result === "LOSS"));
    const underDecisions = decided.filter((p) => p.pickSide === "UNDER" && (p.result === "WIN" || p.result === "LOSS"));
    const overWins = overDecisions.filter((p) => p.result === "WIN").length;
    const underWins = underDecisions.filter((p) => p.result === "WIN").length;
    const decidedResults = decided.map((p) => p.result);
    const streaks = computeStreaks(decidedResults);

    const decidedNoPush = decided.filter((p) => p.result !== "PUSH");
    const last7 = decidedNoPush.slice(-7);
    const last14 = decidedNoPush.slice(-14);
    const last7Wins = last7.filter((p) => p.result === "WIN").length;
    const last14Wins = last14.filter((p) => p.result === "WIN").length;

    const lineBuckets = { low_le_220: 0, mid_220_235: 0, high_gt_235: 0, unknown: 0 };
    const spreadBuckets = { close_le_3: 0, mid_3_9: 0, blowout_ge_9: 0, unknown: 0 };
    for (const p of picks) {
      const ctx = (p.contextDiagnostics ?? {}) as Record<string, unknown>;
      const lb = String(ctx.total_line_bucket ?? "unknown");
      const sb = String(ctx.spread_bucket ?? "unknown");
      if (lb in lineBuckets) lineBuckets[lb as keyof typeof lineBuckets] += 1;
      else lineBuckets.unknown += 1;
      if (sb in spreadBuckets) spreadBuckets[sb as keyof typeof spreadBuckets] += 1;
      else spreadBuckets.unknown += 1;
    }

    summaries.push({
      strategy,
      total: picks.length,
      wins,
      losses,
      pushes,
      hitRate: decisions > 0 ? wins / decisions : null,
      overHitRate: overDecisions.length > 0 ? overWins / overDecisions.length : null,
      underHitRate: underDecisions.length > 0 ? underWins / underDecisions.length : null,
      currentStreak: streaks.currentStreak,
      longestWinStreak: streaks.longestWinStreak,
      longestColdStreak: streaks.longestColdStreak,
      last7HitRate: last7.length > 0 ? last7Wins / last7.length : null,
      last14HitRate: last14.length > 0 ? last14Wins / last14.length : null,
      lineBuckets,
      spreadBuckets,
    });
  }

  console.log("\\n=== Totals Forward Report (Forward-only) ===");
  console.log(
    "strategy | picks | W-L-P | hit | over hit | under hit | cur streak | long W | long cold | last7 | last14",
  );
  for (const s of summaries) {
    console.log(
      `${s.strategy} | ${s.total} | ${s.wins}-${s.losses}-${s.pushes} | ${fmtPct(s.hitRate)} | ${fmtPct(s.overHitRate)} | ${fmtPct(s.underHitRate)} | ` +
        `${s.currentStreak} | ${s.longestWinStreak} | ${s.longestColdStreak} | ${fmtPct(s.last7HitRate)} | ${fmtPct(s.last14HitRate)}`,
    );
  }

  console.log("\\nContext breakdowns:");
  for (const s of summaries) {
    console.log(`- ${s.strategy}`);
    console.log(
      `  line buckets: low=${s.lineBuckets.low_le_220}, mid=${s.lineBuckets.mid_220_235}, high=${s.lineBuckets.high_gt_235}, unknown=${s.lineBuckets.unknown}`,
    );
    console.log(
      `  spread buckets: close=${s.spreadBuckets.close_le_3}, mid=${s.spreadBuckets.mid_3_9}, blowout=${s.spreadBuckets.blowout_ge_9}, unknown=${s.spreadBuckets.unknown}`,
    );
  }

  const eligible = summaries.filter((s) => s.wins + s.losses >= minSample);
  console.log("\\nRecommendation:");
  if (eligible.length === 0) {
    console.log(
      `  insufficient sample: no strategy has at least ${minSample} resolved (non-push) picks; keep default strategy.`,
    );
    return;
  }
  const bestForward = [...eligible].sort((a, b) => (b.hitRate ?? -1) - (a.hitRate ?? -1))[0];
  const mostStable = [...eligible].sort((a, b) =>
    a.longestColdStreak - b.longestColdStreak || (b.hitRate ?? -1) - (a.hitRate ?? -1),
  )[0];
  console.log(
    `  current best forward hit rate: ${bestForward.strategy} (${fmtPct(bestForward.hitRate)}) with sample ${bestForward.wins + bestForward.losses}`,
  );
  console.log(
    `  most stable strategy: ${mostStable.strategy} (longest cold streak ${mostStable.longestColdStreak})`,
  );
}

type OverlapGroup = {
  gameId: string;
  gameDate: string;
  homeTeam: string;
  awayTeam: string;
  season: number;
  strategyRows: Array<{
    strategyName: string;
    pickSide: "OVER" | "UNDER";
    marketLine: number;
    modelScore: number;
    confidence: number;
    result: "WIN" | "LOSS" | "PUSH" | "PENDING";
    actualTotalPoints: number | null;
  }>;
  overlapSize: number;
  isUnique: boolean;
  isOverlap: boolean;
  isFullConsensus: boolean;
  hasSideConsensus: boolean;
  hasSideConflict: boolean;
  consensusSide: "OVER" | "UNDER" | null;
  isResolved: boolean;
  consensusOutcome: "WIN" | "LOSS" | "PUSH" | "CONFLICT" | "MIXED" | "PENDING";
};

function parseDateRangeFlags(flags: CliFlags): { since: string | null; until: string | null } {
  const since = flags.since && /^\d{4}-\d{2}-\d{2}$/.test(flags.since) ? flags.since : null;
  const until = flags.until && /^\d{4}-\d{2}-\d{2}$/.test(flags.until) ? flags.until : null;
  return { since, until };
}

function resolvedNoPush(results: Array<"WIN" | "LOSS" | "PUSH" | "PENDING">): Array<"WIN" | "LOSS"> {
  return results.filter((r): r is "WIN" | "LOSS" => r === "WIN" || r === "LOSS");
}

function computeSubsetStats(label: string, groups: OverlapGroup[]): {
  label: string;
  picks: number;
  wins: number;
  losses: number;
  pushes: number;
  hitRate: number | null;
  currentStreak: string;
  longestColdStreak: number;
} {
  const outcomes = groups.map((g) => g.consensusOutcome);
  const wins = outcomes.filter((o) => o === "WIN").length;
  const losses = outcomes.filter((o) => o === "LOSS").length;
  const pushes = outcomes.filter((o) => o === "PUSH").length;
  const decided = wins + losses;
  const streakSource = outcomes.filter((o): o is "WIN" | "LOSS" | "PUSH" => o === "WIN" || o === "LOSS" || o === "PUSH");
  const streak = computeStreaks(streakSource);
  return {
    label,
    picks: groups.length,
    wins,
    losses,
    pushes,
    hitRate: decided > 0 ? wins / decided : null,
    currentStreak: streak.currentStreak,
    longestColdStreak: streak.longestColdStreak,
  };
}

function buildOverlapGroups(rows: ForwardPickRecord[]): OverlapGroup[] {
  const trackedNames = new Set(TOTALS_FORWARD_STRATEGIES.map((s) => s.strategyName));
  const filtered = rows.filter((r) => trackedNames.has(r.strategyName));
  const grouped = new Map<string, ForwardPickRecord[]>();
  for (const row of filtered) {
    const dateKey =
      row.gameDate instanceof Date ? row.gameDate.toISOString().slice(0, 10) : String(row.gameDate).slice(0, 10);
    const key = `${row.gameId}|${dateKey}|${row.homeTeam}|${row.awayTeam}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(row);
  }

  const groups: OverlapGroup[] = [];
  for (const list of grouped.values()) {
    const base = list[0]!;
    const strategyRows = list.map((r) => ({
      strategyName: r.strategyName,
      pickSide: r.pickSide,
      marketLine: r.marketLine,
      modelScore: r.modelScore,
      confidence: r.confidence,
      result: r.result,
      actualTotalPoints: r.actualTotalPoints,
    }));
    const uniqueSides = new Set(strategyRows.map((r) => r.pickSide));
    const overlapSize = strategyRows.length;
    const hasSideConsensus = uniqueSides.size === 1;
    const hasSideConflict = uniqueSides.size > 1;
    const consensusSide = hasSideConsensus ? strategyRows[0]!.pickSide : null;
    const allResolved = strategyRows.every((r) => r.result !== "PENDING");
    let consensusOutcome: OverlapGroup["consensusOutcome"] = "PENDING";
    if (hasSideConflict) {
      consensusOutcome = "CONFLICT";
    } else if (!allResolved) {
      consensusOutcome = "PENDING";
    } else {
      const outcomeSet = new Set(strategyRows.map((r) => r.result));
      if (outcomeSet.size === 1) {
        const only = strategyRows[0]!.result;
        if (only === "WIN" || only === "LOSS" || only === "PUSH") consensusOutcome = only;
        else consensusOutcome = "PENDING";
      } else {
        consensusOutcome = "MIXED";
      }
    }
    groups.push({
      gameId: base.gameId,
      gameDate:
        base.gameDate instanceof Date ? base.gameDate.toISOString().slice(0, 10) : String(base.gameDate).slice(0, 10),
      homeTeam: base.homeTeam,
      awayTeam: base.awayTeam,
      season: base.season,
      strategyRows,
      overlapSize,
      isUnique: overlapSize === 1,
      isOverlap: overlapSize >= 2,
      isFullConsensus: overlapSize === TOTALS_FORWARD_STRATEGIES.length,
      hasSideConsensus,
      hasSideConflict,
      consensusSide,
      isResolved: allResolved,
      consensusOutcome,
    });
  }
  return groups.sort((a, b) => (a.gameDate < b.gameDate ? -1 : a.gameDate > b.gameDate ? 1 : a.gameId.localeCompare(b.gameId)));
}

export async function runGameTotalForwardOverlapReport(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const minSample = Number.isFinite(Number(flags.minSample)) ? Math.max(5, Number(flags.minSample)) : 20;
  const outputJson = flags.json === "true" || flags.json === "1";
  const { since, until } = parseDateRangeFlags(flags);
  await ensureForwardTable();

  const whereClauses = ["1=1"];
  if (since) whereClauses.push(`fp."gameDate" >= '${sqlEsc(since)}'::date`);
  if (until) whereClauses.push(`fp."gameDate" <= '${sqlEsc(until)}'::date`);
  const rows = await prisma.$queryRawUnsafe<ForwardPickRecord[]>(
    `SELECT fp.*
     FROM "TotalForwardPick" fp
     WHERE ${whereClauses.join(" AND ")}
     ORDER BY fp."gameDate" ASC, fp."createdAt" ASC`,
  );

  if (rows.length === 0) {
    if (outputJson) {
      console.log(JSON.stringify({
        filters: { since, until, minSample },
        message: "No forward-tracked totals picks found for the requested window.",
      }, null, 2));
      return;
    }
    console.log("No forward-tracked totals picks found for the requested window.");
    return;
  }

  const groups = buildOverlapGroups(rows);
  const totalGroups = groups.length;
  const uniqueGroups = groups.filter((g) => g.isUnique);
  const overlapGroups = groups.filter((g) => g.isOverlap);
  const fullConsensusGroups = groups.filter((g) => g.isFullConsensus);
  const sideConsensusGroups = groups.filter((g) => g.hasSideConsensus);
  const sideConflictGroups = groups.filter((g) => g.hasSideConflict);
  const resolvedGroups = groups.filter((g) => g.isResolved);
  const resolvedUnique = resolvedGroups.filter((g) => g.isUnique && (g.consensusOutcome === "WIN" || g.consensusOutcome === "LOSS" || g.consensusOutcome === "PUSH"));
  const resolvedOverlap = resolvedGroups.filter((g) => g.isOverlap && g.hasSideConsensus && (g.consensusOutcome === "WIN" || g.consensusOutcome === "LOSS" || g.consensusOutcome === "PUSH"));
  const resolvedFullConsensus = resolvedGroups.filter((g) => g.isFullConsensus && g.hasSideConsensus && (g.consensusOutcome === "WIN" || g.consensusOutcome === "LOSS" || g.consensusOutcome === "PUSH"));
  const resolvedSideConsensus = resolvedGroups.filter((g) => g.isOverlap && g.hasSideConsensus && (g.consensusOutcome === "WIN" || g.consensusOutcome === "LOSS" || g.consensusOutcome === "PUSH"));

  const breakdownBySize = [1, 2, 3].map((size) => {
    const sizeGroups = groups.filter((g) => g.overlapSize === size);
    const sizeResolved = sizeGroups.filter((g) => g.consensusOutcome === "WIN" || g.consensusOutcome === "LOSS" || g.consensusOutcome === "PUSH");
    const wins = sizeResolved.filter((g) => g.consensusOutcome === "WIN").length;
    const losses = sizeResolved.filter((g) => g.consensusOutcome === "LOSS").length;
    const pushes = sizeResolved.filter((g) => g.consensusOutcome === "PUSH").length;
    const decisions = wins + losses;
    return {
      overlapSize: size,
      games: sizeGroups.length,
      resolvedGames: sizeResolved.length,
      wins,
      losses,
      pushes,
      hitRate: decisions > 0 ? wins / decisions : null,
    };
  });

  const tracked = TOTALS_FORWARD_STRATEGIES.map((s) => s.strategyName);
  const pairwise: Array<{
    strategyA: string;
    strategyB: string;
    overlapGames: number;
    sameSideCount: number;
    conflictCount: number;
    resolvedSameSideWins: number;
    resolvedSameSideLosses: number;
    resolvedSameSidePushes: number;
    resolvedSameSideHitRate: number | null;
  }> = [];
  for (let i = 0; i < tracked.length; i++) {
    for (let j = i + 1; j < tracked.length; j++) {
      const a = tracked[i]!;
      const b = tracked[j]!;
      let overlapGames = 0;
      let sameSideCount = 0;
      let conflictCount = 0;
      let resolvedWins = 0;
      let resolvedLosses = 0;
      let resolvedPushes = 0;
      for (const g of groups) {
        const ra = g.strategyRows.find((r) => r.strategyName === a);
        const rb = g.strategyRows.find((r) => r.strategyName === b);
        if (!ra || !rb) continue;
        overlapGames += 1;
        if (ra.pickSide === rb.pickSide) {
          sameSideCount += 1;
          if (ra.result !== "PENDING" && rb.result !== "PENDING") {
            if (ra.result === rb.result) {
              if (ra.result === "WIN") resolvedWins += 1;
              else if (ra.result === "LOSS") resolvedLosses += 1;
              else if (ra.result === "PUSH") resolvedPushes += 1;
            }
          }
        } else {
          conflictCount += 1;
        }
      }
      const decisions = resolvedWins + resolvedLosses;
      pairwise.push({
        strategyA: a,
        strategyB: b,
        overlapGames,
        sameSideCount,
        conflictCount,
        resolvedSameSideWins: resolvedWins,
        resolvedSameSideLosses: resolvedLosses,
        resolvedSameSidePushes: resolvedPushes,
        resolvedSameSideHitRate: decisions > 0 ? resolvedWins / decisions : null,
      });
    }
  }

  const subsetStats = {
    unique: computeSubsetStats("unique", resolvedUnique),
    overlap: computeSubsetStats("overlap_side_consensus_only", resolvedOverlap),
    sideConsensusOverlap: computeSubsetStats("side_consensus_overlap", resolvedSideConsensus),
    fullConsensus: computeSubsetStats("full_consensus", resolvedFullConsensus),
    overlapUnderConsensus: computeSubsetStats(
      "overlap_under_consensus",
      resolvedSideConsensus.filter((g) => g.consensusSide === "UNDER"),
    ),
    overlapOverConsensus: computeSubsetStats(
      "overlap_over_consensus",
      resolvedSideConsensus.filter((g) => g.consensusSide === "OVER"),
    ),
  };

  const recentDetails = [...groups]
    .sort((a, b) => (a.gameDate < b.gameDate ? 1 : a.gameDate > b.gameDate ? -1 : b.gameId.localeCompare(a.gameId)))
    .slice(0, 20)
    .map((g) => ({
      gameDate: g.gameDate,
      matchup: `${g.awayTeam} @ ${g.homeTeam}`,
      strategies: g.strategyRows.map((r) => r.strategyName),
      sidesByStrategy: Object.fromEntries(g.strategyRows.map((r) => [r.strategyName, r.pickSide])),
      linesByStrategy: Object.fromEntries(g.strategyRows.map((r) => [r.strategyName, r.marketLine])),
      scoresByStrategy: Object.fromEntries(g.strategyRows.map((r) => [r.strategyName, r.modelScore])),
      overlapSize: g.overlapSize,
      consensusStatus: g.hasSideConflict ? "CONFLICT" : g.hasSideConsensus ? "SIDE_CONSENSUS" : "NONE",
      consensusSide: g.consensusSide,
      resolved: g.isResolved,
      consensusOutcome: g.consensusOutcome,
      actualTotalPoints: g.strategyRows.find((r) => r.actualTotalPoints != null)?.actualTotalPoints ?? null,
      perStrategyOutcome: Object.fromEntries(g.strategyRows.map((r) => [r.strategyName, r.result])),
    }));

  const sufficientUnique = subsetStats.unique.wins + subsetStats.unique.losses >= minSample;
  const sufficientConsensus = subsetStats.sideConsensusOverlap.wins + subsetStats.sideConsensusOverlap.losses >= minSample;
  const sufficientFull = subsetStats.fullConsensus.wins + subsetStats.fullConsensus.losses >= minSample;
  const enoughToJudgeOverlap = sufficientUnique && sufficientConsensus;
  const sideConsensusOutperforming =
    enoughToJudgeOverlap &&
    subsetStats.sideConsensusOverlap.hitRate != null &&
    subsetStats.unique.hitRate != null &&
    subsetStats.sideConsensusOverlap.hitRate > subsetStats.unique.hitRate;

  const recommendation = {
    enoughForwardDataForOverlapVsUnique: enoughToJudgeOverlap,
    sideConsensusOutperformingUnique: enoughToJudgeOverlap ? sideConsensusOutperforming : null,
    fullConsensusTooSparse: !sufficientFull,
    sideConflictsNeedAttention: sideConflictGroups.length > 0,
    message: !enoughToJudgeOverlap
      ? `Insufficient sample (min ${minSample}) to judge overlap vs unique reliably.`
      : sideConsensusOutperforming
        ? "Side-consensus overlap is currently outperforming unique picks on forward data."
        : "Side-consensus overlap is not currently outperforming unique picks on forward data.",
  };

  const outputPayload = {
    filters: { since, until, minSample },
    topLevelSummary: {
      totalForwardTrackedGames: totalGroups,
      gamesSelectedByExactlyOneStrategy: uniqueGroups.length,
      gamesSelectedByTwoOrMoreStrategies: overlapGroups.length,
      gamesSelectedByAllTrackedStrategies: fullConsensusGroups.length,
      gamesWithSideConsensus: sideConsensusGroups.length,
      gamesWithSideConflict: sideConflictGroups.length,
    },
    performanceSummary: {
      uniquePicks: subsetStats.unique,
      overlapPicks: subsetStats.overlap,
      fullConsensusPicks: subsetStats.fullConsensus,
      sideConsensusOverlapPicks: subsetStats.sideConsensusOverlap,
      sideConflictCount: sideConflictGroups.length,
    },
    overlapSizeBreakdown: breakdownBySize,
    pairwiseOverlap: pairwise,
    subsetPerformance: subsetStats,
    recentOverlapDetails: recentDetails,
    recommendation,
  };

  if (outputJson) {
    console.log(JSON.stringify(outputPayload, null, 2));
    return;
  }

  console.log("\\n=== Totals Forward Overlap Report ===");
  if (since || until) {
    console.log(`window: ${since ?? "beginning"} -> ${until ?? "latest"}`);
  }
  console.log(
    `total games=${totalGroups} | unique=${uniqueGroups.length} | overlap(2+)=${overlapGroups.length} | ` +
      `full consensus=${fullConsensusGroups.length} | side consensus=${sideConsensusGroups.length} | conflicts=${sideConflictGroups.length}`,
  );

  console.log("\\nPerformance summary (resolved, forward-only):");
  const perfRows = [
    subsetStats.unique,
    subsetStats.overlap,
    subsetStats.fullConsensus,
    subsetStats.sideConsensusOverlap,
    subsetStats.overlapUnderConsensus,
    subsetStats.overlapOverConsensus,
  ];
  for (const s of perfRows) {
    const decided = s.wins + s.losses;
    const insufficient = decided < minSample ? ` [insufficient sample < ${minSample}]` : "";
    console.log(
      `  ${s.label}: picks=${s.picks} W-L-P=${s.wins}-${s.losses}-${s.pushes} hit=${fmtPct(s.hitRate)} ` +
        `streak=${s.currentStreak} long_cold=${s.longestColdStreak}${insufficient}`,
    );
  }
  console.log(`  side_conflict_count=${sideConflictGroups.length} (excluded from consensus performance)`);

  console.log("\\nBreakdown by overlap size:");
  for (const b of breakdownBySize) {
    console.log(
      `  size=${b.overlapSize}: games=${b.games} resolved=${b.resolvedGames} ` +
        `W-L-P=${b.wins}-${b.losses}-${b.pushes} hit=${fmtPct(b.hitRate)}`,
    );
  }

  console.log("\\nPairwise overlap:");
  for (const p of pairwise) {
    console.log(
      `  ${p.strategyA} x ${p.strategyB} | overlap=${p.overlapGames} same_side=${p.sameSideCount} conflicts=${p.conflictCount} ` +
        `resolved_same_side_hit=${fmtPct(p.resolvedSameSideHitRate)}`,
    );
  }

  console.log("\\nRecent overlap details:");
  for (const row of recentDetails) {
    const sides = Object.entries(row.sidesByStrategy).map(([k, v]) => `${k}:${v}`).join(", ");
    const lines = Object.entries(row.linesByStrategy).map(([k, v]) => `${k}:${v}`).join(", ");
    console.log(
      `  ${row.gameDate} | ${row.matchup} | n=${row.overlapSize} | ${row.consensusStatus}` +
        `${row.consensusSide ? `(${row.consensusSide})` : ""} | outcome=${row.consensusOutcome}` +
        ` | sides=[${sides}] | lines=[${lines}]`,
    );
  }

  console.log("\\nRecommendation:");
  console.log(`  enough data for overlap vs unique: ${recommendation.enoughForwardDataForOverlapVsUnique ? "yes" : "no"}`);
  console.log(
    `  side-consensus overlap outperforming unique: ${
      recommendation.sideConsensusOutperformingUnique == null
        ? "insufficient sample"
        : recommendation.sideConsensusOutperformingUnique
          ? "yes"
          : "no"
    }`,
  );
  console.log(`  full consensus too sparse: ${recommendation.fullConsensusTooSparse ? "yes" : "no"}`);
  console.log(`  side conflicts need attention: ${recommendation.sideConflictsNeedAttention ? "yes" : "no"}`);
  console.log(`  note: ${recommendation.message}`);
}

