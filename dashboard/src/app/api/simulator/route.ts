import { prisma } from "@/lib/prisma";
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
  homeCode: string | null;
  awayCode: string | null;
}

export async function GET() {
  const picks = await prisma.$queryRaw<LedgerRow[]>`
    SELECT
      s."date", s."season", s."outcomeType", s."displayLabel", s."priceAmerican",
      s."posteriorHitRate", s."metaScore", s."settledHit",
      ht."code" as "homeCode", at2."code" as "awayCode"
    FROM "SuggestedPlayLedger" s
    JOIN "Game" g ON g."id" = s."gameId"
    LEFT JOIN "Team" ht ON ht."id" = g."homeTeamId"
    LEFT JOIN "Team" at2 ON at2."id" = g."awayTeamId"
    WHERE s."settledHit" IS NOT NULL
    ORDER BY s."date" ASC
  `;

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
      meta: p.metaScore,
      posterior: p.posteriorHitRate,
    })),
  }));

  return NextResponse.json({ days, seasons });
}
