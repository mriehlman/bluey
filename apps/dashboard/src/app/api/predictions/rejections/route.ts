import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

type CanonicalRejectionRow = {
  id: string;
  runId: string | null;
  runDate: Date | string;
  gameId: string;
  patternId: string;
  reasons: string[] | null;
};

async function canonicalRejectionTableExists(): Promise<boolean> {
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."CanonicalPredictionRejection"') IS NOT NULL as "exists"`,
  );
  return Boolean(rows[0]?.exists);
}

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");
  const runId = url.searchParams.get("runId");
  const gameId = url.searchParams.get("gameId");
  const requestedLimit = Number.parseInt(url.searchParams.get("limit") ?? "500", 10);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0
    ? Math.min(2000, requestedLimit)
    : 500;

  const where: string[] = ["1=1"];
  if (dateStr) where.push(`r."runDate" = '${sqlEsc(dateStr)}'::date`);
  if (runId) where.push(`r."runId" = '${sqlEsc(runId)}'`);
  if (gameId) where.push(`r."gameId" = '${sqlEsc(gameId)}'`);
  const tableExists = await canonicalRejectionTableExists();
  if (!tableExists) {
    return NextResponse.json({
      date: dateStr,
      runId,
      gameId,
      count: 0,
      rejections: [],
      message: "CanonicalPredictionRejection table not found yet.",
    });
  }

  try {
    const rows = await prisma.$queryRawUnsafe<CanonicalRejectionRow[]>(
      `SELECT r."id", r."runId", r."runDate", r."gameId", r."patternId", r."reasons"
       FROM "CanonicalPredictionRejection" r
       WHERE ${where.join(" AND ")}
       ORDER BY r."runDate" DESC, r."gameId" ASC, r."patternId" ASC
       LIMIT ${limit}`,
    );

    return NextResponse.json({
      date: dateStr,
      runId,
      gameId,
      count: rows.length,
      rejections: rows.map((row) => ({
        id: row.id,
        runId: row.runId,
        runDate: row.runDate,
        gameId: row.gameId,
        patternId: row.patternId,
        reasons: row.reasons ?? [],
      })),
    });
  } catch (error) {
    try {
      const legacyWhere: string[] = ["1=1"];
      if (dateStr) legacyWhere.push(`r."runDate" = '${sqlEsc(dateStr)}'::date`);
      if (gameId) legacyWhere.push(`r."gameId" = '${sqlEsc(gameId)}'`);
      const rows = await prisma.$queryRawUnsafe<Array<{
        id: string;
        runDate: Date | string;
        gameId: string;
        patternId: string;
        reasons: string[] | null;
      }>>(
        `SELECT r."id", r."runDate", r."gameId", r."patternId", r."reasons"
         FROM "CanonicalPredictionRejection" r
         WHERE ${legacyWhere.join(" AND ")}
         ORDER BY r."runDate" DESC, r."gameId" ASC, r."patternId" ASC
         LIMIT ${limit}`,
      );
      return NextResponse.json({
        date: dateStr,
        runId,
        gameId,
        count: rows.length,
        rejections: rows.map((row) => ({
          id: row.id,
          runId: null,
          runDate: row.runDate,
          gameId: row.gameId,
          patternId: row.patternId,
          reasons: row.reasons ?? [],
        })),
        message: runId
          ? "Run filter unavailable for legacy rejection records (runId column missing)."
          : "Loaded legacy rejection records (runId column missing).",
      });
    } catch (fallbackError) {
      return NextResponse.json({
        date: dateStr,
        runId,
        gameId,
        count: 0,
        rejections: [],
        message:
          fallbackError instanceof Error
            ? `Rejections unavailable: ${fallbackError.message}`
            : error instanceof Error
              ? `Rejections unavailable: ${error.message}`
              : "Rejections unavailable.",
      });
    }
  }
}
