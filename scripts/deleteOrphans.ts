import { prisma } from "../packages/db/src/index.ts";

const before = await prisma.$queryRawUnsafe<Array<{ cnt: bigint }>>(
  `SELECT COUNT(*) as cnt FROM "CanonicalPrediction"`
);
console.log("Before:", before[0].cnt, "predictions");

const deleted = await prisma.$executeRawUnsafe(
  `DELETE FROM "CanonicalPrediction" WHERE NOT EXISTS (SELECT 1 FROM "Game" g WHERE g."id" = "gameId")`
);
console.log("Deleted orphans:", deleted);

const after = await prisma.$queryRawUnsafe<Array<{ cnt: bigint }>>(
  `SELECT COUNT(*) as cnt FROM "CanonicalPrediction"`
);
console.log("After:", after[0].cnt, "predictions");

process.exit(0);
