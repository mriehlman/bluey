import { prisma } from "../packages/db/src/index.ts";

await prisma.$executeRawUnsafe(
  `ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "settledHit" BOOLEAN`
);
console.log("Column added");

await prisma.$executeRawUnsafe(
  `CREATE INDEX IF NOT EXISTS "CanonicalPrediction_settledHit_idx" ON "CanonicalPrediction" ("settledHit")`
);
console.log("Index created");

// Backfill settledHit from game results for final games
// HOME_WIN / AWAY_WIN
let updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = (g."homeScore" > g."awayScore")
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'HOME_WIN%'
    AND g."status" LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("HOME_WIN settled:", updated);

updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = (g."awayScore" > g."homeScore")
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'AWAY_WIN%'
    AND g."status" LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("AWAY_WIN settled:", updated);

// TOTAL_OVER variants
for (const threshold of [220, 230, 240, 250]) {
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (g."homeScore" + g."awayScore" > ${threshold})
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE 'TOTAL_OVER_${threshold}%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`TOTAL_OVER_${threshold} settled:`, updated);
}

// MARGIN_UNDER variants
for (const threshold of [5, 10]) {
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (ABS(g."homeScore" - g."awayScore") < ${threshold})
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE 'MARGIN_UNDER_${threshold}%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`MARGIN_UNDER_${threshold} settled:`, updated);
}

// PLAYER_30_PLUS — any player scored 30+
updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND s."points" >= 30
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_30_PLUS%'
    AND g."status" LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_30_PLUS settled:", updated);

// PLAYER_10_PLUS_REBOUNDS
updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND s."rebounds" >= 10
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_10_PLUS_REBOUNDS%'
    AND g."status" LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_10_PLUS_REBOUNDS settled:", updated);

// PLAYER_5_PLUS_THREES
updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND COALESCE(s."fg3m", 0) >= 5
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_5_PLUS_THREES%'
    AND g."status" LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_5_PLUS_THREES settled:", updated);

// TOP_SCORER_25_PLUS (home/away) — top scorer by ppg on that team scored 25+
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (
      SELECT COALESCE(MAX(s."points"), 0) >= 25
      FROM "PlayerGameStat" s
      WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
    )
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE '${side}_TOP_SCORER_25_PLUS%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_SCORER_25_PLUS settled:`, updated);
}

// TOP_SCORER_20_PLUS
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (
      SELECT COALESCE(MAX(s."points"), 0) >= 20
      FROM "PlayerGameStat" s
      WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
    )
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE '${side}_TOP_SCORER_20_PLUS%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_SCORER_20_PLUS settled:`, updated);
}

// TOP_SCORER_EXCEEDS_AVG — top scorer's game points > their season ppg average
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (
      SELECT s."points" > COALESCE(pc."ppg", 0)
      FROM "PlayerGameStat" s
      JOIN "PlayerGameContext" pc ON pc."gameId" = s."gameId" AND pc."playerId" = s."playerId"
      WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
      ORDER BY pc."ppg" DESC NULLS LAST
      LIMIT 1
    )
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE '${side}_TOP_SCORER_EXCEEDS_AVG%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_SCORER_EXCEEDS_AVG settled:`, updated);
}

// TOP_REBOUNDER_EXCEEDS_AVG
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (
      SELECT s."rebounds" > COALESCE(pc."rpg", 0)
      FROM "PlayerGameStat" s
      JOIN "PlayerGameContext" pc ON pc."gameId" = s."gameId" AND pc."playerId" = s."playerId"
      WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
      ORDER BY pc."rpg" DESC NULLS LAST
      LIMIT 1
    )
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE '${side}_TOP_REBOUNDER_EXCEEDS_AVG%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_REBOUNDER_EXCEEDS_AVG settled:`, updated);
}

// TOP_ASSIST_EXCEEDS_AVG
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (
      SELECT s."assists" > COALESCE(pc."apg", 0)
      FROM "PlayerGameStat" s
      JOIN "PlayerGameContext" pc ON pc."gameId" = s."gameId" AND pc."playerId" = s."playerId"
      WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
      ORDER BY pc."apg" DESC NULLS LAST
      LIMIT 1
    )
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE '${side}_TOP_ASSIST_EXCEEDS_AVG%'
      AND g."status" LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_ASSIST_EXCEEDS_AVG settled:`, updated);
}

// Summary
const total = await prisma.canonicalPrediction.count();
const settled = await prisma.canonicalPrediction.count({ where: { settledHit: { not: null } } });
const hits = await prisma.canonicalPrediction.count({ where: { settledHit: true } });
console.log(`\nDone. ${settled}/${total} predictions settled. ${hits} hits.`);

process.exit(0);
