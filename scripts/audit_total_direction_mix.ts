import { prisma } from "../packages/db/src";

async function main() {
  const model = process.argv[2] ?? "v1.5-hybrid-totals-plus-game-2026-03-28";

  const rows = await prisma.$queryRawUnsafe<
    Array<{ gate: string; direction: string; rows: number }>
  >(
    `SELECT
      COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) AS gate,
      CASE
        WHEN SPLIT_PART(l."outcomeType"::text, ':', 1) LIKE '%UNDER%' THEN 'UNDER'
        WHEN SPLIT_PART(l."outcomeType"::text, ':', 1) LIKE '%OVER%' THEN 'OVER'
        ELSE 'OTHER'
      END AS direction,
      COUNT(*)::int AS rows
     FROM "SuggestedPlayLedger" l
     WHERE COALESCE(l."modelVersionName", 'live') = '${model.replaceAll("'", "''")}'
     GROUP BY 1,2
     ORDER BY 1,2`,
  );

  const patternRows = await prisma.modelVersion.findUnique({
    where: { name: model },
    select: { deployedPatterns: true },
  });

  const patterns = ((patternRows?.deployedPatterns as unknown as Array<{ outcomeType: string }>) ?? []).filter(
    (p) => (p.outcomeType ?? "").includes("TOTAL_"),
  );
  let patternOver = 0;
  let patternUnder = 0;
  for (const p of patterns) {
    const base = (p.outcomeType ?? "").split(":")[0] ?? "";
    if (base.includes("UNDER")) patternUnder += 1;
    if (base.includes("OVER")) patternOver += 1;
  }

  console.log(
    JSON.stringify(
      {
        model,
        ledgerDirectionMix: rows,
        totalPatternDirectionMix: {
          totalPatterns: patterns.length,
          overPatterns: patternOver,
          underPatterns: patternUnder,
        },
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
