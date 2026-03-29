import { prisma } from "../packages/db/src";

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

async function main() {
  const model = process.argv[2] ?? "v1.6-hybrid-with-player-strict-2026-03-28";
  const from = process.argv[3] ?? "2025-10-01";
  const to = process.argv[4] ?? "2026-03-28";

  const rows = await prisma.$queryRawUnsafe<
    Array<{
      gate: string;
      lane: string;
      rows: number;
      graded_rows: number;
      hits: number;
      avg_posterior: number | null;
    }>
  >(
    `SELECT
      COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) AS gate,
      COALESCE(NULLIF(l."laneTag", ''), 'unknown') AS lane,
      COUNT(*)::int AS rows,
      SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS graded_rows,
      SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)::int AS hits,
      AVG(l."posteriorHitRate")::float8 AS avg_posterior
     FROM "SuggestedPlayLedger" l
     WHERE COALESCE(l."modelVersionName",'live') = '${sqlEsc(model)}'
       AND l."date" >= '${sqlEsc(from)}'::date
       AND l."date" <= '${sqlEsc(to)}'::date
     GROUP BY 1,2
     ORDER BY 1,2`,
  );

  const byGate = new Map<string, { rows: number; graded: number; hits: number }>();
  for (const r of rows) {
    const agg = byGate.get(r.gate) ?? { rows: 0, graded: 0, hits: 0 };
    agg.rows += r.rows;
    agg.graded += r.graded_rows;
    agg.hits += r.hits;
    byGate.set(r.gate, agg);
  }

  const gateSummary = [...byGate.entries()].map(([gate, v]) => ({
    gate,
    rows: v.rows,
    gradedRows: v.graded,
    hitRate: v.graded > 0 ? v.hits / v.graded : null,
  }));

  console.log(
    JSON.stringify(
      {
        model,
        from,
        to,
        gateSummary,
        laneBreakdown: rows.map((r) => ({
          gate: r.gate,
          lane: r.lane,
          rows: r.rows,
          gradedRows: r.graded_rows,
          hitRate: r.graded_rows > 0 ? r.hits / r.graded_rows : null,
          avgPosterior: r.avg_posterior,
        })),
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
