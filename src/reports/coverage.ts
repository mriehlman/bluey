import { prisma } from "../db/prisma.js";

export async function coverageReport(): Promise<void> {
  console.log("=== Coverage Report ===\n");

  const gamesBySeason = await prisma.game.groupBy({
    by: ["season"],
    _count: { id: true },
    orderBy: { season: "asc" },
  });

  console.log("Games by Season:");
  for (const row of gamesBySeason) {
    console.log(`  Season ${row.season}: ${row._count.id} games`);
  }
  const totalGames = gamesBySeason.reduce((sum, r) => sum + r._count.id, 0);
  console.log(`  Total: ${totalGames}\n`);

  const statsBySeason = await prisma.$queryRaw<
    { season: number; count: bigint }[]
  >`
    SELECT g.season, COUNT(ps.id)::bigint AS count
    FROM "PlayerGameStat" ps
    JOIN "Game" g ON g.id = ps."gameId"
    GROUP BY g.season
    ORDER BY g.season
  `;

  console.log("PlayerGameStat Rows by Season:");
  let totalStats = 0n;
  for (const row of statsBySeason) {
    console.log(`  Season ${row.season}: ${row.count.toLocaleString()}`);
    totalStats += row.count;
  }
  console.log(`  Total: ${totalStats.toLocaleString()}\n`);

  const orphansByReason = await prisma.orphanPlayerStat.groupBy({
    by: ["reason"],
    _count: { id: true },
    orderBy: { reason: "asc" },
  });

  console.log("Orphan Counts by Reason:");
  if (orphansByReason.length === 0) {
    console.log("  (none logged yet -- re-run ingest:playerstats to populate)");
  }
  for (const row of orphansByReason) {
    console.log(`  ${row.reason}: ${row._count.id.toLocaleString()}`);
  }

  const orphansBySeason = await prisma.orphanPlayerStat.groupBy({
    by: ["season"],
    _count: { id: true },
    orderBy: { season: "asc" },
  });

  if (orphansBySeason.length > 0) {
    console.log("\nOrphan Counts by Season:");
    for (const row of orphansBySeason) {
      console.log(`  Season ${row.season ?? "unknown"}: ${row._count.id.toLocaleString()}`);
    }
  }
  console.log();

  const nightsNoStats = await prisma.$queryRaw<
    { date: Date; game_count: bigint }[]
  >`
    SELECT g.date, COUNT(DISTINCT g.id)::bigint AS game_count
    FROM "Game" g
    LEFT JOIN "PlayerGameStat" ps ON ps."gameId" = g.id
    WHERE ps.id IS NULL
    GROUP BY g.date
    ORDER BY g.date
  `;

  console.log(`Nights with Games but No Stats: ${nightsNoStats.length}`);
  if (nightsNoStats.length > 0) {
    const show = nightsNoStats.slice(0, 20);
    for (const row of show) {
      console.log(`  ${row.date.toISOString().slice(0, 10)} (${row.game_count} games)`);
    }
    if (nightsNoStats.length > 20) {
      console.log(`  ... and ${nightsNoStats.length - 20} more`);
    }
  }
  console.log();

  const topMissing = await prisma.orphanPlayerStat.groupBy({
    by: ["sourceGameId"],
    _count: { id: true },
    orderBy: { _count: { id: "desc" } },
    take: 20,
  });

  console.log("Top Missing sourceGameIds (from OrphanPlayerStat):");
  if (topMissing.length === 0) {
    console.log("  (none logged yet)");
  }
  for (const row of topMissing) {
    console.log(`  gameId ${row.sourceGameId}: ${row._count.id} orphan rows`);
  }

  console.log("\n=== End Coverage Report ===");
}
