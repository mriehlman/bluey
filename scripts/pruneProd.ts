/**
 * Prunes the production DB to keep it within Neon free-tier limits.
 * Removes stale predictions, old prop odds, and tables that should
 * never exist in prod.
 *
 * Usage:
 *   bun run prune:prod
 *   bun run prune:prod -- --dry-run
 *   bun run prune:prod -- --prediction-days 60 --prop-days 14
 */
import { PrismaClient } from "../packages/db/src/index.ts";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg?.startsWith("--")) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = "true";
      }
    }
  }
  return flags;
}

async function prune() {
  const url = process.env.PROD_DATABASE_URL;
  if (!url) throw new Error("PROD_DATABASE_URL is not set");

  const flags = parseFlags(process.argv.slice(2));
  const dryRun = flags["dry-run"] === "true";
  const predictionRetentionDays = Number(flags["prediction-days"] ?? 90);
  const propRetentionDays = Number(flags["prop-days"] ?? 30);
  const rejectionRetentionDays = Number(flags["rejection-days"] ?? 30);

  console.log(`\n=== Prune Production DB${dryRun ? " (DRY RUN)" : ""} ===`);
  console.log(`Target: ${url.replace(/:[^:@]+@/, ":***@")}`);
  console.log(`Retention: predictions=${predictionRetentionDays}d, props=${propRetentionDays}d, rejections=${rejectionRetentionDays}d\n`);

  const prisma = new PrismaClient({ datasources: { db: { url } } });

  try {
    const predCutoff = new Date();
    predCutoff.setDate(predCutoff.getDate() - predictionRetentionDays);

    const propCutoff = new Date();
    propCutoff.setDate(propCutoff.getDate() - propRetentionDays);

    const rejCutoff = new Date();
    rejCutoff.setDate(rejCutoff.getDate() - rejectionRetentionDays);

    // Strip heavy JSON from old canonical predictions instead of deleting them
    const staleCanonical = await prisma.$queryRawUnsafe<[{ count: bigint }]>(
      `SELECT count(*)::bigint as count FROM "CanonicalPrediction"
       WHERE "generatedAt" < $1
         AND "featureSnapshotPayload" IS NOT NULL
         AND jsonb_array_length(COALESCE("featureSnapshotPayload"->'tokens', '[]'::jsonb)) > 0`,
      predCutoff,
    ).catch(() => [{ count: 0n }]);
    const staleCanonicalCount = Number(staleCanonical[0]?.count ?? 0);

    if (staleCanonicalCount > 0 && !dryRun) {
      await prisma.$executeRawUnsafe(
        `UPDATE "CanonicalPrediction"
         SET "featureSnapshotPayload" = jsonb_build_object(
           'feature_schema_version', "featureSnapshotPayload"->>'feature_schema_version',
           'generated_from', "featureSnapshotPayload"->>'generated_from',
           'tokens_stripped', true
         ),
         "modelVotes" = '[]'::jsonb,
         "voteWeightBreakdown" = NULL
         WHERE "generatedAt" < $1
           AND "featureSnapshotPayload" IS NOT NULL
           AND jsonb_array_length(COALESCE("featureSnapshotPayload"->'tokens', '[]'::jsonb)) > 0`,
        predCutoff,
      );
    }
    console.log(`  CanonicalPrediction JSON stripped: ${staleCanonicalCount} rows${dryRun ? " (would strip)" : ""}`);

    // Delete old rejections
    const oldRejections = await prisma.canonicalPredictionRejection.count({
      where: { createdAt: { lt: rejCutoff } },
    }).catch(() => 0);
    if (oldRejections > 0 && !dryRun) {
      await prisma.canonicalPredictionRejection.deleteMany({
        where: { createdAt: { lt: rejCutoff } },
      });
    }
    console.log(`  CanonicalPredictionRejection deleted: ${oldRejections} rows${dryRun ? " (would delete)" : ""}`);

    // Delete old player prop odds
    const oldProps = await prisma.playerPropOdds.count({
      where: { fetchedAt: { lt: propCutoff } },
    }).catch(() => 0);
    if (oldProps > 0 && !dryRun) {
      await prisma.playerPropOdds.deleteMany({
        where: { fetchedAt: { lt: propCutoff } },
      });
    }
    console.log(`  PlayerPropOdds deleted: ${oldProps} rows${dryRun ? " (would delete)" : ""}`);

    // Drop tables that should never be in prod
    const zombieTables = [
      "PatternV2Hit",
      "GamePattern",
      "GamePatternHit",
      "BacktestRun",
      "BacktestResult",
      "OrphanPlayerStat",
      "PredictionLog",
      "IngestDay",
      "ExternalIdMap",
    ];

    for (const table of zombieTables) {
      const exists = await prisma.$queryRawUnsafe<[{ exists: boolean }]>(
        `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '${table}')`,
      ).catch(() => [{ exists: false }]);

      if (exists[0]?.exists) {
        const count = await prisma.$queryRawUnsafe<[{ count: bigint }]>(
          `SELECT count(*)::bigint as count FROM "${table}"`,
        ).catch(() => [{ count: 0n }]);
        const rowCount = Number(count[0]?.count ?? 0);

        if (rowCount > 0 && !dryRun) {
          await prisma.$executeRawUnsafe(`TRUNCATE TABLE "${table}" CASCADE`);
        }
        if (rowCount > 0) {
          console.log(`  ${table} truncated: ${rowCount} rows${dryRun ? " (would truncate)" : ""}`);
        }
      }
    }

    // Report DB size
    const sizeResult = await prisma.$queryRawUnsafe<[{ size: string }]>(
      `SELECT pg_size_pretty(pg_database_size(current_database())) as size`,
    ).catch(() => [{ size: "unknown" }]);
    console.log(`\n  Current DB size: ${sizeResult[0]?.size}`);

    // Per-table sizes
    const tableSizes = await prisma.$queryRawUnsafe<{ table: string; size: string; rows: bigint }[]>(
      `SELECT
         relname as table,
         pg_size_pretty(pg_total_relation_size(c.oid)) as size,
         c.reltuples::bigint as rows
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'public' AND c.relkind = 'r'
       ORDER BY pg_total_relation_size(c.oid) DESC
       LIMIT 15`,
    ).catch(() => []);

    if (tableSizes.length > 0) {
      console.log("\n  Top tables by size:");
      for (const t of tableSizes) {
        console.log(`    ${t.table.padEnd(30)} ${t.size.padStart(10)}  (~${Number(t.rows).toLocaleString()} rows)`);
      }
    }

    console.log(`\nDone!${dryRun ? " (No changes made — dry run)" : ""}\n`);
  } finally {
    await prisma.$disconnect();
  }
}

prune().catch((err) => {
  console.error("Prune failed:", err);
  process.exit(1);
});
