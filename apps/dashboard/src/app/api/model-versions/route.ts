import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

export const dynamic = "force-dynamic";

export async function GET() {
  const versions = await prisma.modelVersion.findMany({
    select: {
      id: true,
      name: true,
      description: true,
      isActive: true,
      stats: true,
      createdAt: true,
    },
    orderBy: { createdAt: "desc" },
  });

  const coverageRows = await prisma.$queryRawUnsafe<
    Array<{ version: string; gradedDates: number; gradedPicks: number }>
  >(
    `WITH run_rows AS (
       SELECT
         COALESCE(cp."runContext"->>'modelVersionName', 'live') AS "version",
         g."date"::date AS "gameDate",
         cp."runId" AS "runId",
         cp."runStartedAt" AS "runStartedAt",
         cp."generatedAt" AS "generatedAt"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE cp."runId" IS NOT NULL
     ),
     latest_run_per_date AS (
       SELECT DISTINCT ON ("version", "gameDate")
         "version", "gameDate", "runId"
       FROM run_rows
       ORDER BY "version", "gameDate", "runStartedAt" DESC NULLS LAST, "generatedAt" DESC
     ),
     canonical_latest AS (
       SELECT
         lr."version" AS "version",
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType"
       FROM latest_run_per_date lr
       JOIN "CanonicalPrediction" cp ON cp."runId" = lr."runId"
       JOIN "Game" g ON g."id" = cp."gameId" AND g."date"::date = lr."gameDate"
       WHERE COALESCE(cp."runContext"->>'modelVersionName', 'live') = lr."version"
     ),
     graded AS (
       SELECT
         cl."version",
         cl."gameDate"
       FROM canonical_latest cl
       JOIN LATERAL (
         SELECT 1
         FROM "SuggestedPlayLedger" s
         WHERE s."date" = cl."gameDate"
           AND s."gameId" = cl."gameId"
           AND s."outcomeType" = cl."outcomeType"
           AND s."settledHit" IS NOT NULL
         LIMIT 1
       ) x ON TRUE
     )
     SELECT
       "version",
       COUNT(*)::int AS "gradedPicks",
       COUNT(DISTINCT "gameDate")::int AS "gradedDates"
     FROM graded
     GROUP BY "version"`,
  );

  const coverageByVersion = new Map(
    coverageRows.map((r) => [r.version, { gradedDates: r.gradedDates, gradedPicks: r.gradedPicks }]),
  );

  return NextResponse.json({
    versions: versions.map((v) => ({
      ...v,
      coverage: coverageByVersion.get(v.name) ?? { gradedDates: 0, gradedPicks: 0 },
    })),
    liveCoverage: coverageByVersion.get("live") ?? { gradedDates: 0, gradedPicks: 0 },
  });
}

export async function POST(req: Request) {
  const body = await req.json();
  const { action, name } = body as { action: string; name?: string };

  if (action === "activate" && name) {
    const version = await prisma.modelVersion.findUnique({ where: { name } });
    if (!version) {
      return NextResponse.json({ error: `Version "${name}" not found` }, { status: 404 });
    }
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
    await prisma.modelVersion.update({
      where: { id: version.id },
      data: { isActive: true },
    });
    return NextResponse.json({ ok: true, message: `Version "${name}" activated` });
  }

  if (action === "deactivate") {
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
    return NextResponse.json({ ok: true, message: "All versions deactivated. Using live data." });
  }

  return NextResponse.json({ error: "Invalid action. Use activate or deactivate." }, { status: 400 });
}
