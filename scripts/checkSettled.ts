import { prisma } from "../packages/db/src/index.ts";

const rows = await prisma.$queryRawUnsafe<Array<{ total: bigint; settled: bigint; hits: bigint }>>(
  `SELECT COUNT(*) as total, COUNT("settledHit") as settled, SUM(CASE WHEN "settledHit" = true THEN 1 ELSE 0 END) as hits FROM "CanonicalPrediction"`
);
const r = rows[0];
console.log(`Total predictions: ${r.total}`);
console.log(`Settled: ${r.settled}`);
console.log(`Hits: ${r.hits}`);
console.log(`Hit rate: ${(Number(r.hits) / Number(r.settled) * 100).toFixed(1)}%`);

const unsettled = await prisma.$queryRawUnsafe<Array<{ selection: string; cnt: bigint }>>(
  `SELECT LEFT("selection", 40) as selection, COUNT(*) as cnt FROM "CanonicalPrediction" WHERE "settledHit" IS NULL GROUP BY LEFT("selection", 40) ORDER BY cnt DESC LIMIT 20`
);
console.log("\nUnsettled by selection type:");
for (const u of unsettled) console.log(`  ${u.selection}: ${u.cnt}`);

process.exit(0);
