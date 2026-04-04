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
      outcomeType: string;
      rows: number;
      gradedRows: number;
      hits: number;
      hitRate: number | null;
      avgPosterior: number | null;
    }>
  >(
    `SELECT
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
      AND COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) = 'legacy'
      AND l."date" >= '${sqlEsc(from)}'::date
      AND l."date" <= '${sqlEsc(to)}'::date
      AND COALESCE(NULLIF(l."laneTag", ''), 'other') = 'player_points'
    GROUP BY 1
    ORDER BY "hitRate" DESC NULLS LAST, "gradedRows" DESC`,
  );

  console.log(JSON.stringify({ model, from, to, outcomeTypes: rows }, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
