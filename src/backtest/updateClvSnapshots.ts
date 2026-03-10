import { prisma } from "../db/prisma.js";

type LedgerRow = {
  id: string;
  gameId: string;
  market: string | null;
  line: number | null;
  targetPlayerId: number | null;
  priceAmerican: number | null;
  betImpliedProb: number | null;
};

type PropOddsRow = {
  overPrice: number | null;
  fetchedAt: Date;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else if (args[i].startsWith("--")) {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

function impliedProbFromAmerican(american: number | null): number | null {
  if (american == null || !Number.isFinite(american) || american === 0) return null;
  return american < 0 ? (-american) / (-american + 100) : 100 / (american + 100);
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

export async function updateSuggestedPlayClv(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const days = Math.max(1, Number(flags.days ?? 14));
  const limit = Math.max(10, Number(flags.limit ?? 1000));
  const settledOnly = (flags["settled-only"] ?? "true") !== "false";
  const where: string[] = [
    `l."isActionable" = TRUE`,
    `l."market" IS NOT NULL`,
    `l."line" IS NOT NULL`,
    `l."targetPlayerId" IS NOT NULL`,
    `l."priceAmerican" IS NOT NULL`,
    `l."date" >= NOW()::date - INTERVAL '${days} days'`,
    `l."closePriceAmerican" IS NULL`,
  ];
  if (settledOnly) where.push(`l."settledResult" IN ('HIT','MISS')`);

  const rows = await prisma.$queryRawUnsafe<LedgerRow[]>(
    `SELECT
      l."id" as "id",
      l."gameId" as "gameId",
      l."market" as "market",
      l."line" as "line",
      l."targetPlayerId" as "targetPlayerId",
      l."priceAmerican" as "priceAmerican",
      l."betImpliedProb" as "betImpliedProb"
     FROM "SuggestedPlayLedger" l
     WHERE ${where.join(" AND ")}
     ORDER BY l."date" DESC
     LIMIT ${limit}`,
  );

  console.log("\n=== Update SuggestedPlayLedger CLV Snapshots ===\n");
  console.log(`Scope: rows=${rows.length}, days=${days}, settledOnly=${settledOnly}, limit=${limit}`);
  if (rows.length === 0) return;

  let updated = 0;
  let skipped = 0;
  for (const row of rows) {
    const market = row.market ?? "";
    const line = row.line ?? null;
    const playerId = row.targetPlayerId ?? null;
    if (!market || line == null || playerId == null) {
      skipped++;
      continue;
    }
    const latest = await prisma.$queryRawUnsafe<PropOddsRow[]>(
      `SELECT p."overPrice" as "overPrice", p."fetchedAt" as "fetchedAt"
       FROM "PlayerPropOdds" p
       WHERE p."gameId" = '${sqlEsc(row.gameId)}'
         AND p."playerId" = ${playerId}
         AND p."market" = '${sqlEsc(market)}'
         AND p."line" IS NOT NULL
         AND ABS(p."line" - ${line}) <= 0.5
       ORDER BY p."fetchedAt" DESC
       LIMIT 1`,
    );
    const closePrice = latest[0]?.overPrice ?? null;
    const closeImplied = impliedProbFromAmerican(closePrice);
    const betImplied = row.betImpliedProb ?? impliedProbFromAmerican(row.priceAmerican);
    if (closePrice == null || closeImplied == null || betImplied == null) {
      skipped++;
      continue;
    }
    const clvDeltaProb = closeImplied - betImplied;
    const clvDeltaCents = closePrice - (row.priceAmerican ?? closePrice);
    const clvStatus = clvDeltaProb > 0.002 ? "POSITIVE" : clvDeltaProb < -0.002 ? "NEGATIVE" : "FLAT";
    await prisma.$executeRawUnsafe(
      `UPDATE "SuggestedPlayLedger"
       SET "closePriceAmerican" = ${closePrice},
           "closeImpliedProb" = ${closeImplied},
           "clvDeltaProb" = ${clvDeltaProb},
           "clvDeltaCents" = ${clvDeltaCents},
           "clvStatus" = '${clvStatus}',
           "updatedAt" = NOW()
       WHERE "id" = '${sqlEsc(row.id)}'`,
    );
    updated++;
  }

  console.log(`Updated ${updated} rows. Skipped ${skipped} rows (missing close snapshot match).`);
}
