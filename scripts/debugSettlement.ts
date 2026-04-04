import { prisma } from "../packages/db/src/index.ts";

const rows = await prisma.$queryRawUnsafe<Array<{ status: string; cnt: bigint }>>(
  `SELECT TRIM(g."status") as status, COUNT(*) as cnt
   FROM "CanonicalPrediction" cp
   JOIN "Game" g ON g."id" = cp."gameId"
   WHERE cp."settledHit" IS NULL
   GROUP BY TRIM(g."status")
   ORDER BY cnt DESC`
);
console.log("Unsettled predictions by game status:");
for (const r of rows) console.log(`  "${r.status}": ${r.cnt}`);

// Try settling with TRIM
let updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = (g."homeScore" > g."awayScore")
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'HOME_WIN%'
    AND TRIM(g."status") LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("\nHOME_WIN with TRIM:", updated);

updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = (g."awayScore" > g."homeScore")
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'AWAY_WIN%'
    AND TRIM(g."status") LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("AWAY_WIN with TRIM:", updated);

for (const threshold of [220, 230, 240, 250]) {
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (g."homeScore" + g."awayScore" > ${threshold})
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE 'TOTAL_OVER_${threshold}%'
      AND TRIM(g."status") LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`TOTAL_OVER_${threshold} with TRIM:`, updated);
}

for (const threshold of [5, 10]) {
  updated = await prisma.$executeRawUnsafe(`
    UPDATE "CanonicalPrediction" cp
    SET "settledHit" = (ABS(g."homeScore" - g."awayScore") < ${threshold})
    FROM "Game" g
    WHERE g."id" = cp."gameId"
      AND cp."selection" LIKE 'MARGIN_UNDER_${threshold}%'
      AND TRIM(g."status") LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`MARGIN_UNDER_${threshold} with TRIM:`, updated);
}

// Player outcomes
updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND s."points" >= 30
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_30_PLUS%'
    AND TRIM(g."status") LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_30_PLUS:", updated);

updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND s."rebounds" >= 10
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_10_PLUS_REBOUNDS%'
    AND TRIM(g."status") LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_10_PLUS_REBOUNDS:", updated);

updated = await prisma.$executeRawUnsafe(`
  UPDATE "CanonicalPrediction" cp
  SET "settledHit" = EXISTS (
    SELECT 1 FROM "PlayerGameStat" s WHERE s."gameId" = cp."gameId" AND COALESCE(s."fg3m", 0) >= 5
  )
  FROM "Game" g
  WHERE g."id" = cp."gameId"
    AND cp."selection" LIKE 'PLAYER_5_PLUS_THREES%'
    AND TRIM(g."status") LIKE '%Final%'
    AND cp."settledHit" IS NULL
`);
console.log("PLAYER_5_PLUS_THREES:", updated);

// Team-specific player outcomes
for (const side of ["HOME", "AWAY"]) {
  const teamCol = side === "HOME" ? "homeTeamId" : "awayTeamId";
  for (const stat of ["20", "25"]) {
    updated = await prisma.$executeRawUnsafe(`
      UPDATE "CanonicalPrediction" cp
      SET "settledHit" = (
        SELECT COALESCE(MAX(s."points"), 0) >= ${stat}
        FROM "PlayerGameStat" s
        WHERE s."gameId" = cp."gameId" AND s."teamId" = g."${teamCol}"
      )
      FROM "Game" g
      WHERE g."id" = cp."gameId"
        AND cp."selection" LIKE '${side}_TOP_SCORER_${stat}_PLUS%'
        AND TRIM(g."status") LIKE '%Final%'
        AND cp."settledHit" IS NULL
    `);
    console.log(`${side}_TOP_SCORER_${stat}_PLUS:`, updated);
  }

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
      AND TRIM(g."status") LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_SCORER_EXCEEDS_AVG:`, updated);

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
      AND TRIM(g."status") LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_REBOUNDER_EXCEEDS_AVG:`, updated);

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
      AND TRIM(g."status") LIKE '%Final%'
      AND cp."settledHit" IS NULL
  `);
  console.log(`${side}_TOP_ASSIST_EXCEEDS_AVG:`, updated);
}

// Final summary
const summary = await prisma.$queryRawUnsafe<Array<{ total: bigint; settled: bigint; hits: bigint }>>(
  `SELECT COUNT(*) as total, COUNT("settledHit") as settled, SUM(CASE WHEN "settledHit" = true THEN 1 ELSE 0 END) as hits FROM "CanonicalPrediction"`
);
const s = summary[0];
console.log(`\nFinal: ${s.settled}/${s.total} settled. ${s.hits} hits (${(Number(s.hits) / Number(s.settled) * 100).toFixed(1)}%)`);

process.exit(0);
