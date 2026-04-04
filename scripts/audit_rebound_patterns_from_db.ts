import { prisma } from "../packages/db/src";

type PatternRow = {
  id: string;
  status: string;
  outcomeType: string;
  posteriorHitRate: number | null;
  score: number | null;
  n: number | null;
};

async function main() {
  const rows = (await prisma.patternV2.findMany({
    where: {
      OR: [
        { outcomeType: { contains: "REBOUND" } },
        { outcomeType: { contains: "REBOUNDER" } },
      ],
    },
    select: {
      id: true,
      status: true,
      outcomeType: true,
      posteriorHitRate: true,
      score: true,
      n: true,
    },
    orderBy: [{ posteriorHitRate: "desc" }, { n: "desc" }],
    take: 200,
  })) as PatternRow[];

  const byStatus = new Map<string, number>();
  for (const r of rows) byStatus.set(r.status, (byStatus.get(r.status) ?? 0) + 1);

  console.log(
    JSON.stringify(
      {
        total: rows.length,
        byStatus: Object.fromEntries(byStatus.entries()),
        top: rows.slice(0, 40),
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
