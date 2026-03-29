import { prisma } from "../packages/db/src";

async function main() {
  const model = process.argv[2] ?? "v1.4-totals-strict-2026-03-28";

  const byGate = await prisma.$queryRawUnsafe<
    Array<{
      model: string;
      gate: string;
      rows: number;
      actionable_rows: number;
      graded_rows: number;
    }>
  >(
    `SELECT
      COALESCE("modelVersionName", 'live') AS model,
      COALESCE("gateMode", CASE
        WHEN COALESCE("actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict'
        ELSE 'legacy'
      END) AS gate,
      COUNT(*)::int AS rows,
      SUM(CASE WHEN "isActionable" THEN 1 ELSE 0 END)::int AS actionable_rows,
      SUM(CASE WHEN "settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS graded_rows
     FROM "SuggestedPlayLedger"
     WHERE COALESCE("modelVersionName", 'live') = '${model.replaceAll("'", "''")}'
     GROUP BY 1,2
     ORDER BY 1,2`,
  );

  const byDate = await prisma.$queryRawUnsafe<
    Array<{ date: string; rows: number; actionable_rows: number; graded_rows: number }>
  >(
    `SELECT
      "date"::text AS date,
      COUNT(*)::int AS rows,
      SUM(CASE WHEN "isActionable" THEN 1 ELSE 0 END)::int AS actionable_rows,
      SUM(CASE WHEN "settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS graded_rows
     FROM "SuggestedPlayLedger"
     WHERE COALESCE("modelVersionName", 'live') = '${model.replaceAll("'", "''")}'
     GROUP BY 1
     ORDER BY 1 DESC
     LIMIT 30`,
  );

  console.log(JSON.stringify({ model, byGate, byDate }, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
