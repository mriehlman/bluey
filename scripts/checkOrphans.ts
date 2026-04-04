import { prisma } from "../packages/db/src/index.ts";

const orphans = await prisma.$queryRawUnsafe<Array<{ cnt: bigint }>>(
  `SELECT COUNT(*) as cnt FROM "CanonicalPrediction" cp WHERE NOT EXISTS (SELECT 1 FROM "Game" g WHERE g."id" = cp."gameId")`
);
console.log("Orphaned predictions (no matching game):", orphans[0].cnt);

const total = await prisma.$queryRawUnsafe<Array<{ cnt: bigint }>>(
  `SELECT COUNT(*) as cnt FROM "CanonicalPrediction"`
);
console.log("Total predictions:", total[0].cnt);

const withGame = await prisma.$queryRawUnsafe<Array<{ cnt: bigint }>>(
  `SELECT COUNT(*) as cnt FROM "CanonicalPrediction" cp WHERE EXISTS (SELECT 1 FROM "Game" g WHERE g."id" = cp."gameId")`
);
console.log("Predictions with matching game:", withGame[0].cnt);

process.exit(0);
