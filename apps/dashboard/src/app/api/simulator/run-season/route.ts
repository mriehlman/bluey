import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import { predictGames } from "@bluey/core/features/predictGames";

export const dynamic = "force-dynamic";

type Body = {
  season?: number;
  modelVersion?: string;
  oddsMode?: "full" | "require" | "ignore";
  strictGates?: boolean;
};

function asDateRangeForSeason(season: number): { from: string; to: string } {
  return { from: `${season}-10-01`, to: `${season + 1}-06-30` };
}

export async function POST(req: Request) {
  const body = (await req.json().catch(() => ({}))) as Body;
  const season = Number(body.season);
  if (!Number.isFinite(season) || season < 2000) {
    return NextResponse.json({ error: "season is required" }, { status: 400 });
  }
  const oddsMode = body.oddsMode ?? "full";
  if (!["full", "require", "ignore"].includes(oddsMode)) {
    return NextResponse.json({ error: "invalid oddsMode" }, { status: 400 });
  }
  const modelVersion = body.modelVersion ?? "all";
  const strictGates = body.strictGates === true;
  if (modelVersion === "all") {
    return NextResponse.json(
      { error: "Select a specific model version (or live) before running season picks." },
      { status: 400 },
    );
  }

  const { from, to } = asDateRangeForSeason(season);
  const rows = await prisma.$queryRawUnsafe<Array<{ d: string }>>(
    `SELECT to_char(date, 'YYYY-MM-DD') AS d
     FROM "Game"
     WHERE date >= '${from}'::date
       AND date <= '${to}'::date
     GROUP BY date
     ORDER BY date ASC`,
  );
  const dates = rows.map((r) => String(r.d)).filter(Boolean);

  let ok = 0;
  let failed = 0;
  const failures: Array<{ date: string; error: string }> = [];
  for (const d of dates) {
    const args = ["--date", d, "--oddsMode", oddsMode, "--modelVersion", modelVersion];
    args.push("--strictGates", strictGates ? "true" : "false");
    try {
      await predictGames(args);
      ok += 1;
    } catch (err) {
      failed += 1;
      failures.push({ date: d, error: err instanceof Error ? err.message : String(err) });
    }
  }

  return NextResponse.json({
    season,
    modelVersion,
    oddsMode,
    strictGates,
    dates: dates.length,
    ok,
    failed,
    failures: failures.slice(0, 20),
  });
}
