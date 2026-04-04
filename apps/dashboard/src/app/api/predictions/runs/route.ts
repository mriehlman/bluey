import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

type PredictionRunRow = {
  runId: string;
  runStartedAt: Date | string | null;
  runContext: unknown;
  predictionCount: number;
  firstGeneratedAt: Date | string | null;
  lastGeneratedAt: Date | string | null;
};

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

async function canonicalPredictionTableExists(): Promise<boolean> {
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."CanonicalPrediction"') IS NOT NULL as "exists"`,
  );
  return Boolean(rows[0]?.exists);
}

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");
  const requestedLimit = Number.parseInt(url.searchParams.get("limit") ?? "100", 10);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0
    ? Math.min(500, requestedLimit)
    : 100;
  const exists = await canonicalPredictionTableExists();
  if (!exists) {
    return NextResponse.json({
      date: dateStr,
      count: 0,
      runs: [],
      message: "CanonicalPrediction table not found yet.",
    });
  }
  const where = dateStr
    ? `WHERE cp."generatedAt"::date = '${sqlEsc(dateStr)}'::date AND cp."runId" IS NOT NULL`
    : `WHERE cp."runId" IS NOT NULL`;

  try {
    const rows = await prisma.$queryRawUnsafe<PredictionRunRow[]>(
      `WITH grouped AS (
      SELECT
        cp."runId" as "runId",
        MAX(cp."runStartedAt") as "runStartedAt",
        COUNT(*)::int as "predictionCount",
        MIN(cp."generatedAt") as "firstGeneratedAt",
        MAX(cp."generatedAt") as "lastGeneratedAt"
      FROM "CanonicalPrediction" cp
      ${where}
      GROUP BY cp."runId"
    )
    SELECT
      g."runId",
      g."runStartedAt",
      g."predictionCount",
      g."firstGeneratedAt",
      g."lastGeneratedAt",
      (
        SELECT cp2."runContext"
        FROM "CanonicalPrediction" cp2
        WHERE cp2."runId" = g."runId"
        ORDER BY cp2."runStartedAt" DESC NULLS LAST, cp2."generatedAt" DESC
        LIMIT 1
      ) as "runContext"
    FROM grouped g
    ORDER BY g."runStartedAt" DESC NULLS LAST, g."lastGeneratedAt" DESC
      LIMIT ${limit}`,
    );

    return NextResponse.json({
      date: dateStr,
      count: rows.length,
      runs: rows.map((row) => ({
        runId: row.runId,
        runStartedAt: row.runStartedAt,
        runContext: row.runContext,
        predictionCount: row.predictionCount,
        firstGeneratedAt: row.firstGeneratedAt,
        lastGeneratedAt: row.lastGeneratedAt,
      })),
    });
  } catch (error) {
    return NextResponse.json({
      date: dateStr,
      count: 0,
      runs: [],
      message:
        error instanceof Error
          ? `Runs unavailable: ${error.message}`
          : "Runs unavailable.",
    });
  }
}
