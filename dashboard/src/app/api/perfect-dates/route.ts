import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

/** Returns YYYY-MM-DD strings for dates where all actionable graded picks (for the filter) hit (perfect days). */
export async function GET(req: Request) {
  const url = new URL(req.url);
  const month = url.searchParams.get("month"); // YYYY-MM
  const filter = url.searchParams.get("filter") ?? "all"; // game | player | all
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

  try {
    const rows = await prisma.$queryRawUnsafe<
      Array<{ date: string }>
    >(
      `SELECT TO_CHAR(l."date", 'YYYY-MM-DD') as "date"
       FROM "SuggestedPlayLedger" l
       WHERE l."isActionable" = TRUE
         AND l."settledHit" IS NOT NULL
         AND l."date" >= '${start}'
         AND l."date" <= '${end}'
         ${typeFilter}
       GROUP BY l."date"
       HAVING COUNT(*) = SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)
          AND COUNT(*) > 0`,
    );

    const dates = rows.map((r) => r.date).filter(Boolean);
    return NextResponse.json({ dates });
  } catch (err) {
    const e = err as { code?: string };
    if (e?.code === "P2021" || (e?.code === "P2010")) {
      return NextResponse.json({ dates: [] });
    }
    throw err;
  }
}
