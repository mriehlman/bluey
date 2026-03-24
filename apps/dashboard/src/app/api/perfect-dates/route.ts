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
  const modelVersion = url.searchParams.get("modelVersion") ?? "all"; // all | live | <snapshot name>
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

  const [year, mon] = month.split("-").map(Number);
  const start = `${year}-${String(mon).padStart(2, "0")}-01`;
  const lastDay = new Date(year, mon, 0).getDate();
  const end = `${year}-${String(mon).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;

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
        ? `AND COALESCE(cp."runContext"->>'modelVersionName', 'live') = 'live'`
        : `AND cp."runContext"->>'modelVersionName' = '${sqlEsc(modelVersion)}'`;
  const mlFilterSql =
    mlFilter === "all"
      ? ""
      : mlFilter === "ml_only"
        ? `AND COALESCE(s."mlInvolved", FALSE) = TRUE`
        : `AND COALESCE(s."mlInvolved", FALSE) = FALSE`;

  try {
    const rows = await prisma.$queryRawUnsafe<
      Array<{ date: string }>
    >(
      `WITH latest_run_per_date AS (
         SELECT DISTINCT ON (g."date"::date)
           g."date"::date AS "gameDate",
           cp."runId" AS "runId"
         FROM "CanonicalPrediction" cp
         JOIN "Game" g ON g."id" = cp."gameId"
         WHERE cp."runId" IS NOT NULL
           AND g."date" >= '${start}'
           AND g."date" <= '${end}'
           ${modelVersionFilter}
         ORDER BY g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
       ),
      canonical_games AS (
        SELECT DISTINCT
           g."date"::date AS "gameDate",
          cp."gameId" AS "gameId"
         FROM "CanonicalPrediction" cp
         JOIN "Game" g ON g."id" = cp."gameId"
         JOIN latest_run_per_date lr
           ON lr."gameDate" = g."date"::date
          AND lr."runId" = cp."runId"
       )
      SELECT TO_CHAR(s."date", 'YYYY-MM-DD') as "date"
      FROM canonical_games cg
      JOIN "SuggestedPlayLedger" s
        ON s."date" = cg."gameDate"
       AND s."gameId" = cg."gameId"
       AND s."isActionable" = TRUE
       AND s."settledHit" IS NOT NULL
       WHERE 1=1
         ${typeFilter.replaceAll('l."', 's."')}
        ${mlFilterSql}
       GROUP BY s."date"
      HAVING BOOL_AND(s."settledHit" = TRUE)
         AND COUNT(*) > 0`,
    );

    const dates = rows.map((r) => r.date).filter(Boolean);
    return NextResponse.json({ dates, modelVersion });
  } catch (err) {
    const e = err as { code?: string };
    if (e?.code === "P2021" || (e?.code === "P2010")) {
      return NextResponse.json({ dates: [] });
    }
    throw err;
  }
}
