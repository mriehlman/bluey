import { prisma } from "../packages/db/src";

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

async function main() {
  const model = process.argv[2] ?? "v1.7-player-points-filtered-2026-03-28";
  const from = process.argv[3] ?? "2025-10-01";
  const to = process.argv[4] ?? "2026-03-28";

  const rows = await prisma.$queryRawUnsafe<
    Array<{
      gate: string;
      lane: string;
      outcomeType: string;
      rows: number;
      gradedRows: number;
      hits: number;
      hitRate: number | null;
      avgPosterior: number | null;
    }>
  >(
    `SELECT
      COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) AS gate,
      COALESCE(NULLIF(l."laneTag", ''), 'unknown') AS lane,
      l."outcomeType" AS "outcomeType",
      COUNT(*)::int AS rows,
      SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS "gradedRows",
      SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)::int AS hits,
      CASE
        WHEN SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END) > 0
        THEN SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)::float8
          / SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::float8
        ELSE NULL
      END AS "hitRate",
      AVG(l."posteriorHitRate")::float8 AS "avgPosterior"
    FROM "SuggestedPlayLedger" l
    WHERE COALESCE(l."modelVersionName",'live') = '${sqlEsc(model)}'
      AND l."date" >= '${sqlEsc(from)}'::date
      AND l."date" <= '${sqlEsc(to)}'::date
      AND (
        l."outcomeType" LIKE '%REBOUND%'
        OR l."outcomeType" LIKE 'PLAYER_10_PLUS_REBOUNDS:%'
      )
    GROUP BY 1,2,3
    ORDER BY gate, "hitRate" DESC NULLS LAST, "gradedRows" DESC`,
  );

  const byGate = new Map<string, { rows: number; gradedRows: number; hits: number }>();
  for (const r of rows) {
    const agg = byGate.get(r.gate) ?? { rows: 0, gradedRows: 0, hits: 0 };
    agg.rows += r.rows;
    agg.gradedRows += r.gradedRows;
    agg.hits += r.hits;
    byGate.set(r.gate, agg);
  }

  console.log(
    JSON.stringify(
      {
        model,
        from,
        to,
        gateSummary: [...byGate.entries()].map(([gate, v]) => ({
          gate,
          rows: v.rows,
          gradedRows: v.gradedRows,
          hitRate: v.gradedRows > 0 ? v.hits / v.gradedRows : null,
        })),
        outcomes: rows,
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
