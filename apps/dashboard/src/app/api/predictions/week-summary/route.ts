import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import { getEasternDateFromUtc } from "@/lib/format";

export const dynamic = "force-dynamic";

interface DaySummary {
  date: string;
  games: number;
  picks: number;
  hits: number;
  settled: number;
}

function getSundayOfWeek(dateStr: string): Date {
  const m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) throw new Error(`Invalid date: ${dateStr}`);
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  d.setUTCDate(d.getUTCDate() - d.getUTCDay());
  return d;
}

async function ledgerTableExists(): Promise<boolean> {
  const rows = await prisma.$queryRawUnsafe<Array<{ exists: boolean }>>(
    `SELECT to_regclass('public."SuggestedPlayLedger"') IS NOT NULL as "exists"`,
  );
  return Boolean(rows[0]?.exists);
}

async function ensureLedgerColumns(): Promise<void> {
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "modelVersionName" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "gateMode" text`);
  await prisma.$executeRawUnsafe(`ALTER TABLE "SuggestedPlayLedger" ADD COLUMN IF NOT EXISTS "actionabilityVersion" text`);
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateParam = url.searchParams.get("weekOf") ?? getEasternDateFromUtc(new Date());
  const modelVersion = url.searchParams.get("model") ?? "live";
  const gateMode = url.searchParams.get("gateMode") ?? "legacy";

  const sunday = getSundayOfWeek(dateParam);

  const weekDates: string[] = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(sunday);
    d.setUTCDate(d.getUTCDate() + i);
    weekDates.push(d.toISOString().slice(0, 10));
  }

  const sundayDate = new Date(`${weekDates[0]}T00:00:00.000Z`);
  const saturdayDate = new Date(`${weekDates[6]}T00:00:00.000Z`);

  const gameCounts = await prisma.game.groupBy({
    by: ["date"],
    where: { date: { gte: sundayDate, lte: saturdayDate } },
    _count: { id: true },
  });

  const gamesByDate = new Map<string, number>();
  for (const row of gameCounts) {
    const key = (row.date instanceof Date ? row.date.toISOString() : String(row.date)).slice(0, 10);
    gamesByDate.set(key, row._count.id);
  }

  const ledgerByDate = new Map<string, { picks: number; settled: number; hits: number }>();

  try {
    if (await ledgerTableExists()) {
      await ensureLedgerColumns();

      const gateModeExpr = `COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END)`;

      const ledgerRows = await prisma.$queryRawUnsafe<
        Array<{ date: Date; picks: bigint; settled: bigint; hits: bigint }>
      >(
        `SELECT
          l.date,
          COUNT(*)::bigint as picks,
          COUNT(*) FILTER (WHERE l."settledResult" IN ('HIT','MISS'))::bigint as settled,
          COUNT(*) FILTER (WHERE l."settledResult" = 'HIT')::bigint as hits
        FROM "SuggestedPlayLedger" l
        WHERE l.date >= $1 AND l.date <= $2
          AND COALESCE(l."modelVersionName",'live') = $3
          AND ${gateModeExpr} = $4
        GROUP BY l.date`,
        sundayDate,
        saturdayDate,
        modelVersion,
        gateMode,
      );

      for (const row of ledgerRows) {
        const key = (row.date instanceof Date ? row.date.toISOString() : String(row.date)).slice(0, 10);
        ledgerByDate.set(key, {
          picks: Number(row.picks),
          settled: Number(row.settled),
          hits: Number(row.hits),
        });
      }
    }
  } catch (e) {
    console.error("[week-summary] ledger query failed:", String(e).slice(0, 300));
  }

  const days: DaySummary[] = weekDates.map((d) => ({
    date: d,
    games: gamesByDate.get(d) ?? 0,
    picks: ledgerByDate.get(d)?.picks ?? 0,
    hits: ledgerByDate.get(d)?.hits ?? 0,
    settled: ledgerByDate.get(d)?.settled ?? 0,
  }));

  return NextResponse.json({ weekOf: dateParam, sunday: weekDates[0], saturday: weekDates[6], days });
}
