import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

export const dynamic = "force-dynamic";

type PatternRow = {
  id: string;
  outcomeType: string;
  conditions: string[];
  discoverySource: string;
  trainStats: {
    n: number;
    wins: number;
    rawHitRate: number;
    posteriorHitRate: number;
    edge: number;
    lift: number;
  };
  valStats: {
    n: number;
    wins: number;
    rawHitRate: number;
    posteriorHitRate: number;
    edge: number;
    lift: number;
  };
  forwardStats: {
    n: number;
    wins: number;
    rawHitRate: number;
    posteriorHitRate: number;
    edge: number;
    lift: number;
  };
  posteriorHitRate: number;
  edge: number;
  score: number;
  lift: number;
  n: number;
  status: string;
};

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const status = url.searchParams.get("status") ?? "deployed";
  const outcomeType = url.searchParams.get("outcomeType");
  const requestedLimit = Number.parseInt(url.searchParams.get("limit") ?? "50", 10);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0
    ? Math.min(200, requestedLimit)
    : 50;

  const whereClauses = [`"status" = '${sqlEsc(status)}'`];
  if (outcomeType) {
    whereClauses.push(`"outcomeType" = '${sqlEsc(outcomeType)}'`);
  }

  const patterns = await prisma.$queryRawUnsafe<PatternRow[]>(
    `SELECT
      "id","outcomeType","conditions","discoverySource",
      "trainStats","valStats","forwardStats",
      "posteriorHitRate","edge","score","lift","n","status"
     FROM "PatternV2"
     WHERE ${whereClauses.join(" AND ")}
     ORDER BY "score" DESC
     LIMIT ${limit}`,
  );

  const outcomes = await prisma.$queryRawUnsafe<Array<{ outcomeType: string }>>(
    `SELECT DISTINCT "outcomeType" FROM "PatternV2" ORDER BY "outcomeType"`,
  );

  return NextResponse.json({
    status,
    outcomeType: outcomeType ?? null,
    outcomes: outcomes.map((o) => o.outcomeType),
    count: patterns.length,
    patterns,
  });
}
