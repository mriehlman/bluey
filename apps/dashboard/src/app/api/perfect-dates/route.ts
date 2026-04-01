import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

export const dynamic = "force-dynamic";

function sqlEsc(value: string): string {
  return value.replaceAll("'", "''");
}

/** Returns YYYY-MM-DD strings for dates where all actionable graded picks (for the filter) hit (perfect days). */
export async function GET(req: Request) {
  const url = new URL(req.url);
  const month = url.searchParams.get("month"); // YYYY-MM
  const filter = url.searchParams.get("filter") ?? "all"; // game | player | all
  const mlFilter = url.searchParams.get("mlFilter") ?? "all"; // all | ml_only | no_ml
  const lane = (url.searchParams.get("lane") ?? "all").toLowerCase(); // all | moneyline | spread | total | ...
  const modelVersion = url.searchParams.get("modelVersion") ?? "all"; // all | live | <snapshot name>
  const gateMode = (url.searchParams.get("gateMode") ?? "legacy").toLowerCase(); // legacy | strict
  const gameMinVotes = Math.max(1, Number(url.searchParams.get("gameMinVotes") ?? "1") || 1);
  const playerMinVotes = Math.max(1, Number(url.searchParams.get("playerMinVotes") ?? "1") || 1);
  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    return NextResponse.json(
      { error: "Requires ?month=YYYY-MM" },
      { status: 400 },
    );
  }
  if (!["game", "player", "all"].includes(filter)) {
    return NextResponse.json(
      { error: "filter must be game, player, or all" },
      { status: 400 },
    );
  }
  if (!["all", "ml_only", "no_ml"].includes(mlFilter)) {
    return NextResponse.json(
      { error: "mlFilter must be all, ml_only, or no_ml" },
      { status: 400 },
    );
  }
  if (
    ![
      "all",
      "moneyline",
      "spread",
      "total",
      "player_points",
      "player_rebounds",
      "player_assists",
      "other_prop",
      "other",
    ].includes(lane)
  ) {
    return NextResponse.json(
      { error: "lane must be all, moneyline, spread, total, player_points, player_rebounds, player_assists, other_prop, or other" },
      { status: 400 },
    );
  }
  if (!["legacy", "strict"].includes(gateMode)) {
    return NextResponse.json(
      { error: "gateMode must be legacy or strict" },
      { status: 400 },
    );
  }

  const [year, mon] = month.split("-").map(Number);
  const start = `${year}-${String(mon).padStart(2, "0")}-01`;
  const lastDay = new Date(year, mon, 0).getDate();
  const end = `${year}-${String(mon).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;

  // Backward-compatible schema guard: older DBs may not yet have scope columns.
  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "actionabilityVersion" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "modelVersionName" text`,
    );
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "gateMode" text`,
    );
  } catch {
    // If table is missing entirely, query fallback below will return empty dates.
  }

  // Outcome type filter: base = part before ':', player = PLAYER_* or HOME_TOP_* or AWAY_TOP_*
  const baseExpr = `SPLIT_PART(l."outcomeType"::text, ':', 1)`;
  const typeFilter =
    filter === "game"
      ? `AND NOT (${baseExpr} LIKE 'PLAYER_%' OR ${baseExpr} LIKE 'HOME_TOP_%' OR ${baseExpr} LIKE 'AWAY_TOP_%')`
      : filter === "player"
        ? `AND (${baseExpr} LIKE 'PLAYER_%' OR ${baseExpr} LIKE 'HOME_TOP_%' OR ${baseExpr} LIKE 'AWAY_TOP_%')`
        : "";
  const modelVersionFilter =
    modelVersion === "all"
      ? ""
      : modelVersion === "live"
        ? `AND COALESCE(s."modelVersionName", 'live') = 'live'`
        : `AND s."modelVersionName" = '${sqlEsc(modelVersion)}'`;
  const mlFilterSql =
    mlFilter === "all"
      ? ""
      : mlFilter === "ml_only"
        ? `AND COALESCE(s."mlInvolved", FALSE) = TRUE`
        : `AND COALESCE(s."mlInvolved", FALSE) = FALSE`;
  const laneExpr = `
    COALESCE(
      NULLIF(s."laneTag", ''),
      CASE
        WHEN ${baseExpr.replaceAll('l."', 's."')} LIKE '%WIN' THEN 'moneyline'
        WHEN ${baseExpr.replaceAll('l."', 's."')} LIKE '%COVERED' THEN 'spread'
        WHEN ${baseExpr.replaceAll('l."', 's."')} LIKE '%OVER%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE '%UNDER%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE 'TOTAL_%' THEN 'total'
        WHEN ${baseExpr.replaceAll('l."', 's."')} = 'PLAYER_10_PLUS_REBOUNDS' OR ${baseExpr.replaceAll('l."', 's."')} LIKE '%REBOUNDER%' THEN 'player_rebounds'
        WHEN ${baseExpr.replaceAll('l."', 's."')} = 'PLAYER_10_PLUS_ASSISTS' OR ${baseExpr.replaceAll('l."', 's."')} LIKE '%ASSIST%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE '%PLAYMAKER%' THEN 'player_assists'
        WHEN ${baseExpr.replaceAll('l."', 's."')} = 'PLAYER_30_PLUS' OR ${baseExpr.replaceAll('l."', 's."')} = 'PLAYER_40_PLUS' OR ${baseExpr.replaceAll('l."', 's."')} LIKE '%SCORER%' THEN 'player_points'
        WHEN ${baseExpr.replaceAll('l."', 's."')} LIKE 'PLAYER_%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE 'HOME_TOP_%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE 'AWAY_TOP_%' THEN 'other_prop'
        ELSE 'other'
      END
    )
  `;
  const laneFilterSql =
    lane === "all"
      ? ""
      : `AND ${laneExpr} = '${sqlEsc(lane)}'`;
  const gateModeExpr = `COALESCE(s."gateMode", CASE WHEN COALESCE(s."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;
  const gateModeFilter = `AND ${gateModeExpr} = '${sqlEsc(gateMode)}'`;

  const isPlayerExpr = `(${baseExpr.replaceAll('l."', 's."')} LIKE 'PLAYER_%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE 'HOME_TOP_%' OR ${baseExpr.replaceAll('l."', 's."')} LIKE 'AWAY_TOP_%')`;
  const votesFilterSql =
    gameMinVotes <= 1 && playerMinVotes <= 1
      ? ""
      : `AND CASE WHEN ${isPlayerExpr} THEN COALESCE(s."votes", 1) >= ${playerMinVotes} ELSE COALESCE(s."votes", 1) >= ${gameMinVotes} END`;

  try {
    const rows = await prisma.$queryRawUnsafe<
      Array<{ date: string }>
    >(
      `SELECT TO_CHAR(s."date", 'YYYY-MM-DD') as "date"
       FROM "SuggestedPlayLedger" s
       WHERE s."date" >= '${start}'
         AND s."date" <= '${end}'
         AND s."settledHit" IS NOT NULL
         ${modelVersionFilter}
         ${gateModeFilter}
         ${typeFilter.replaceAll('l."', 's."')}
         ${mlFilterSql}
         ${laneFilterSql}
         ${votesFilterSql}
       GROUP BY s."date"
       HAVING BOOL_AND(s."settledHit" = TRUE)
          AND COUNT(*) > 0`,
    );

    const dates = rows.map((r) => r.date).filter(Boolean);
    return NextResponse.json({ dates, modelVersion, gateMode, lane });
  } catch (err) {
    const e = err as { code?: string };
    if (e?.code === "P2021" || (e?.code === "P2010")) {
      return NextResponse.json({ dates: [] });
    }
    throw err;
  }
}
