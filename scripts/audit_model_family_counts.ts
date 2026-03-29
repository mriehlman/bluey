import { prisma } from "../packages/db/src";

function familyExpr(alias: string): string {
  const base = `SPLIT_PART(${alias}."outcomeType"::text, ':', 1)`;
  return `
    CASE
      WHEN ${base} LIKE 'PLAYER_%' OR ${base} LIKE 'HOME_TOP_%' OR ${base} LIKE 'AWAY_TOP_%' THEN 'PLAYER'
      WHEN ${base} LIKE '%COVER%' OR ${base} LIKE '%SPREAD%' THEN 'SPREAD'
      WHEN ${base} LIKE '%WIN%' OR ${base} LIKE '%MONEYLINE%' THEN 'MONEYLINE'
      WHEN ${base} LIKE 'TOTAL_%' OR ${base} LIKE '%OVER%' OR ${base} LIKE '%UNDER%' THEN 'TOTAL'
      ELSE 'OTHER'
    END
  `;
}

async function main() {
  const model = process.argv[2] ?? "v1.5-hybrid-totals-plus-game-2026-03-28";

  const byFamily = await prisma.$queryRawUnsafe<
    Array<{ family: string; rows: number; graded_rows: number }>
  >(
    `SELECT
      ${familyExpr("l")} AS family,
      COUNT(*)::int AS rows,
      SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS graded_rows
     FROM "SuggestedPlayLedger" l
     WHERE COALESCE(l."modelVersionName",'live') = '${model.replaceAll("'", "''")}'
     GROUP BY 1
     ORDER BY rows DESC`,
  );

  const byLane = await prisma.$queryRawUnsafe<
    Array<{ lane: string; rows: number }>
  >(
    `SELECT
      COALESCE(NULLIF(l."laneTag", ''), 'unknown') AS lane,
      COUNT(*)::int AS rows
     FROM "SuggestedPlayLedger" l
     WHERE COALESCE(l."modelVersionName",'live') = '${model.replaceAll("'", "''")}'
     GROUP BY 1
     ORDER BY rows DESC`,
  );

  console.log(JSON.stringify({ model, byFamily, byLane }, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
