import { prisma } from "../packages/db/src";

type Row = {
  id: string;
  outcomeType: string;
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
};

async function main() {
  const rows = (await prisma.patternV2.findMany({
    where: {
      status: "deployed",
      OR: [{ outcomeType: { contains: "REBOUND" } }, { outcomeType: { contains: "REBOUNDER" } }],
    },
    select: {
      id: true,
      outcomeType: true,
      posteriorHitRate: true,
      edge: true,
      score: true,
      n: true,
    },
    orderBy: [{ score: "desc" }],
  })) as Row[];

  const avg = (vals: number[]) => (vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null);
  const edges = rows.map((r) => r.edge);
  const post = rows.map((r) => r.posteriorHitRate);
  const nVals = rows.map((r) => r.n);

  console.log(
    JSON.stringify(
      {
        count: rows.length,
        avgEdge: avg(edges),
        avgPosterior: avg(post),
        avgN: avg(nVals),
        top20: rows.slice(0, 20),
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
