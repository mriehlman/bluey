import { prisma } from "@bluey/db";
import { NextResponse } from "next/server";

interface LedgerRow {
  date: Date;
  season: number;
  outcomeType: string;
  displayLabel: string | null;
  priceAmerican: number | null;
  posteriorHitRate: number;
  metaScore: number | null;
  settledHit: boolean;
  mlInvolved: boolean;
  laneTag: string | null;
  homeCode: string | null;
  awayCode: string | null;
}

function sqlEsc(value: string): string {
  return value.replaceAll("'", "''");
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const modelVersion = url.searchParams.get("modelVersion") ?? "all";
  const oddsMode = (url.searchParams.get("oddsMode") ?? "all").toLowerCase();
  const gateMode = (url.searchParams.get("gateMode") ?? "all").toLowerCase();
  const lane = (url.searchParams.get("lane") ?? "all").toLowerCase();
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
      {
        error:
          "lane must be all, moneyline, spread, total, player_points, player_rebounds, player_assists, other_prop, or other",
      },
      { status: 400 },
    );
  }
  const oddsModeFilter =
    oddsMode === "require"
      ? `AND COALESCE(cp."runContext"->>'oddsMode','full') = 'require'`
      : oddsMode === "ignore"
        ? `AND COALESCE(cp."runContext"->>'oddsMode','full') = 'ignore'`
        : oddsMode === "full"
          ? `AND COALESCE(cp."runContext"->>'oddsMode','full') = 'full'`
          : "";
  const modelVersionFilter =
    modelVersion === "all"
      ? ""
      : modelVersion === "live"
        ? `AND COALESCE(cp."runContext"->>'modelVersionName', 'live') = 'live'`
        : `AND cp."runContext"->>'modelVersionName' = '${sqlEsc(modelVersion)}'`;
  const gateModeFilter =
    gateMode === "strict"
      ? `AND COALESCE((cp."runContext"->>'strictGates')::boolean, FALSE) = TRUE`
      : gateMode === "legacy"
        ? `AND COALESCE((cp."runContext"->>'strictGates')::boolean, FALSE) = FALSE`
        : "";
  const laneFilter =
    lane === "all"
      ? ""
      : `AND COALESCE(NULLIF(s."laneTag", ''), 'other') = '${sqlEsc(lane)}'`;

  await prisma.$executeRawUnsafe(
    `ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "mlInvolved" boolean NOT NULL DEFAULT FALSE`,
  );
  const picks = await prisma.$queryRawUnsafe<LedgerRow[]>(
    `
    WITH latest_run_per_date AS (
      SELECT DISTINCT ON (g."date"::date)
        g."date"::date AS "gameDate",
        cp."runId" AS "runId"
      FROM "CanonicalPrediction" cp
      JOIN "Game" g ON g."id" = cp."gameId"
      WHERE cp."runId" IS NOT NULL
        ${modelVersionFilter}
        ${oddsModeFilter}
        ${gateModeFilter}
      ORDER BY g."date"::date, cp."runStartedAt" DESC NULLS LAST, cp."generatedAt" DESC
    ),
    canonical_latest AS (
      SELECT
        g."date"::date AS "gameDate",
        g."season" AS "season",
        cp."gameId" AS "gameId",
        cp."selection" AS "outcomeType"
      FROM "CanonicalPrediction" cp
      JOIN "Game" g ON g."id" = cp."gameId"
      JOIN latest_run_per_date lr
        ON lr."gameDate" = g."date"::date
       AND lr."runId" = cp."runId"
    )
    SELECT
      s."date",
      cl."season" AS "season",
      s."outcomeType",
      s."displayLabel",
      s."priceAmerican",
      s."posteriorHitRate",
      s."metaScore",
      s."settledHit",
      COALESCE(s."mlInvolved", FALSE) as "mlInvolved",
      s."laneTag" as "laneTag",
      ht."code" as "homeCode",
      at2."code" as "awayCode"
    FROM canonical_latest cl
    JOIN "Game" g ON g."id" = cl."gameId"
    LEFT JOIN "Team" ht ON ht."id" = g."homeTeamId"
    LEFT JOIN "Team" at2 ON at2."id" = g."awayTeamId"
    JOIN LATERAL (
      SELECT s.*
      FROM "SuggestedPlayLedger" s
      WHERE s."date" = cl."gameDate"
        AND s."gameId" = cl."gameId"
        AND s."outcomeType" = cl."outcomeType"
        AND s."settledHit" IS NOT NULL
        ${laneFilter}
      ORDER BY s."confidence" DESC NULLS LAST, s."updatedAt" DESC
      LIMIT 1
    ) s ON TRUE
    ORDER BY s."date" ASC
  `,
  );

  const seasons = [...new Set(picks.map((p) => p.season))].sort((a, b) => a - b);

  const byDate = new Map<string, LedgerRow[]>();
  for (const p of picks) {
    const d = (p.date instanceof Date ? p.date : new Date(p.date)).toISOString().slice(0, 10);
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d)!.push(p);
  }

  const days = [...byDate.entries()].map(([date, dayPicks]) => ({
    date,
    season: dayPicks[0]?.season ?? 0,
    picks: dayPicks.map((p) => ({
      gameLabel: `${p.awayCode ?? "?"} @ ${p.homeCode ?? "?"}`,
      label: p.displayLabel ?? p.outcomeType,
      outcomeType: p.outcomeType,
      odds: p.priceAmerican ?? -110,
      hit: p.settledHit,
      mlInvolved: Boolean(p.mlInvolved),
      laneTag: p.laneTag ?? null,
      meta: p.metaScore,
      posterior: p.posteriorHitRate,
    })),
  }));

  return NextResponse.json({ days, seasons, modelVersion, oddsMode, gateMode, lane });
}
