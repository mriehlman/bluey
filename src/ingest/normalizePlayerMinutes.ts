import { prisma } from "../db/prisma.js";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (!args[i].startsWith("--")) continue;
    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

type PreviewRow = {
  id: string;
  gameId: string;
  playerId: number;
  minutes: number;
  minutesRaw: string | null;
  expectedSeconds: number;
};

/**
 * One-time repair for legacy rows where minutes were stored as whole minutes
 * while minutesRaw kept the original MM:SS value.
 */
export async function normalizePlayerMinutes(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const apply = (flags.apply ?? "false") === "true";

  const candidates = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'`,
  );
  const candidateCount = candidates[0]?.count ?? 0;

  const mismatches = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutes" <> (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int)`,
  );
  const mismatchCount = mismatches[0]?.count ?? 0;

  const preview = await prisma.$queryRawUnsafe<PreviewRow[]>(
    `SELECT
       "id",
       "gameId",
       "playerId",
       "minutes",
       "minutesRaw",
       (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) as "expectedSeconds"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutes" <> (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int)
     ORDER BY "minutesRaw" DESC
     LIMIT 10`,
  );

  const invalidSeconds = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND split_part("minutesRaw", ':', 2)::int >= 60`,
  );
  const invalidSecondsCount = invalidSeconds[0]?.count ?? 0;

  const invalidSecondsPreview = await prisma.$queryRawUnsafe<PreviewRow[]>(
    `SELECT
       "id",
       "gameId",
       "playerId",
       "minutes",
       "minutesRaw",
       (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) as "expectedSeconds"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND split_part("minutesRaw", ':', 2)::int >= 60
     ORDER BY "minutesRaw" DESC
     LIMIT 10`,
  );

  const suspiciousTotals = await prisma.$queryRawUnsafe<
    Array<{ playerId: number; gameId: string; minutes: number; minutesRaw: string | null; expectedSeconds: number }>
  >(
    `SELECT
       "playerId",
       "gameId",
       "minutes",
       "minutesRaw",
       (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) as "expectedSeconds"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) > 4200
     ORDER BY "expectedSeconds" DESC
     LIMIT 10`,
  );
  const rawNeedsCanonicalization = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutesRaw" <> (
         ((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) / 60)::int::text
         || ':' ||
         lpad((((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) % 60)::int)::text, 2, '0')
       )`,
  );
  const rawNeedsCanonicalizationCount = rawNeedsCanonicalization[0]?.count ?? 0;

  console.log("\n=== Normalize Player Minutes ===");
  console.log(`Candidate rows with MM:SS minutesRaw: ${candidateCount}`);
  console.log(`Rows needing repair: ${mismatchCount}`);
  console.log(`Rows with invalid seconds (SS >= 60): ${invalidSecondsCount}`);
  console.log(`Rows with non-canonical minutesRaw: ${rawNeedsCanonicalizationCount}`);
  if (preview.length > 0) {
    console.log("\nPreview (up to 10 rows):");
    for (const row of preview) {
      console.log(
        `  playerId=${row.playerId} gameId=${row.gameId} raw=${row.minutesRaw} stored=${row.minutes} expected=${row.expectedSeconds}`,
      );
    }
  }
  if (invalidSecondsPreview.length > 0) {
    console.log("\nInvalid MM:SS preview (up to 10 rows):");
    for (const row of invalidSecondsPreview) {
      const total = row.expectedSeconds;
      const norm = `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
      console.log(
        `  playerId=${row.playerId} gameId=${row.gameId} raw=${row.minutesRaw} normalized=${norm}`,
      );
    }
  }
  if (suspiciousTotals.length > 0) {
    console.log("\nSuspiciously high minute totals (>70 min) preview:");
    for (const row of suspiciousTotals) {
      console.log(
        `  playerId=${row.playerId} gameId=${row.gameId} raw=${row.minutesRaw} totalSeconds=${row.expectedSeconds}`,
      );
    }
  }

  if (!apply) {
    console.log("\nDry run only. Re-run with --apply true to update rows.");
    return;
  }

  if (mismatchCount === 0 && rawNeedsCanonicalizationCount === 0) {
    console.log("\nNo changes needed.");
    return;
  }

  await prisma.$executeRawUnsafe(
    `UPDATE "PlayerGameStat"
     SET "minutes" = (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int)
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutes" <> (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int)`,
  );

  // Canonicalize malformed minutesRaw values (e.g., 9:60 -> 10:00) and pad seconds.
  await prisma.$executeRawUnsafe(
    `UPDATE "PlayerGameStat"
     SET "minutesRaw" = (
       ((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) / 60)::int::text
       || ':' ||
       lpad((((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) % 60)::int)::text, 2, '0')
     )
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutesRaw" <> (
         ((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) / 60)::int::text
         || ':' ||
         lpad((((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) % 60)::int)::text, 2, '0')
       )`,
  );

  const remaining = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutes" <> (split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int)`,
  );
  const remainingCount = remaining[0]?.count ?? 0;
  const remainingInvalidSeconds = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND split_part("minutesRaw", ':', 2)::int >= 60`,
  );
  const remainingInvalidSecondsCount = remainingInvalidSeconds[0]?.count ?? 0;
  const remainingRawNeedsCanonicalization = await prisma.$queryRawUnsafe<Array<{ count: number }>>(
    `SELECT COUNT(*)::int as "count"
     FROM "PlayerGameStat"
     WHERE "minutesRaw" ~ '^\\d+:\\d{1,2}$'
       AND "minutesRaw" <> (
         ((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) / 60)::int::text
         || ':' ||
         lpad((((split_part("minutesRaw", ':', 1)::int * 60 + split_part("minutesRaw", ':', 2)::int) % 60)::int)::text, 2, '0')
       )`,
  );
  const remainingRawNeedsCanonicalizationCount = remainingRawNeedsCanonicalization[0]?.count ?? 0;

  console.log(`\nUpdated rows: ${mismatchCount - remainingCount}`);
  console.log(`Remaining mismatches: ${remainingCount}`);
  console.log(`Remaining invalid seconds values: ${remainingInvalidSecondsCount}`);
  console.log(
    `Canonicalized minutesRaw rows: ${rawNeedsCanonicalizationCount - remainingRawNeedsCanonicalizationCount}`,
  );
  console.log(`Remaining non-canonical minutesRaw rows: ${remainingRawNeedsCanonicalizationCount}`);
}
