import { prisma } from "../db/prisma.js";
import { CATALOG } from "../features/eventCatalog.js";

export async function eventCoverageReport(): Promise<void> {
  console.log("=== Event Catalog Coverage Report ===\n");

  const nightsProcessed = await prisma.nightEvent.count({
    where: { eventKey: "NIGHT_PROCESSED" },
  });

  const seasonCounts = await prisma.nightEvent.groupBy({
    by: ["season"],
    where: { eventKey: "NIGHT_PROCESSED" },
    _count: { id: true },
    orderBy: { season: "asc" },
  });

  console.log(`Total nights processed: ${nightsProcessed}`);
  console.log(`Seasons: ${seasonCounts.map((s) => s.season).join(", ")}\n`);

  const allEvents = await prisma.nightEvent.groupBy({
    by: ["eventKey", "season"],
    _count: { id: true },
    orderBy: [{ eventKey: "asc" }, { season: "asc" }],
  });

  interface EventStats {
    total: number;
    perSeason: Record<number, number>;
  }

  const eventMap = new Map<string, EventStats>();

  for (const row of allEvents) {
    let stats = eventMap.get(row.eventKey);
    if (!stats) {
      stats = { total: 0, perSeason: {} };
      eventMap.set(row.eventKey, stats);
    }
    stats.total += row._count.id;
    stats.perSeason[row.season] = row._count.id;
  }

  const seasons = seasonCounts.map((s) => s.season);
  const nightsBySeason: Record<number, number> = {};
  for (const s of seasonCounts) {
    nightsBySeason[s.season] = s._count.id;
  }

  const catalogKeys = CATALOG.map((d) => d.key);

  const seasonHeaders = seasons.map((s) => String(s).padStart(8)).join("");
  console.log(
    `${"Event".padEnd(40)}  ${"Total".padStart(6)}  ${"Rate".padStart(6)}  ${"Min".padStart(5)}  ${"Max".padStart(5)}${seasonHeaders}`
  );
  console.log("-".repeat(75 + seasons.length * 8));

  const results: {
    key: string;
    total: number;
    rate: number;
    min: number;
    max: number;
    perSeason: Record<number, number>;
  }[] = [];

  for (const key of catalogKeys) {
    const stats = eventMap.get(key);
    const total = stats?.total ?? 0;
    const perSeason = stats?.perSeason ?? {};

    const seasonValues = seasons.map((s) => perSeason[s] ?? 0);
    const min = seasonValues.length > 0 ? Math.min(...seasonValues) : 0;
    const max = seasonValues.length > 0 ? Math.max(...seasonValues) : 0;
    const rate = nightsProcessed > 0 ? total / nightsProcessed : 0;

    results.push({ key, total, rate, min, max, perSeason });

    const seasonCols = seasons
      .map((s) => String(perSeason[s] ?? 0).padStart(8))
      .join("");

    console.log(
      `${key.padEnd(40)}  ${String(total).padStart(6)}  ${(rate * 100).toFixed(1).padStart(5)}%  ${String(min).padStart(5)}  ${String(max).padStart(5)}${seasonCols}`
    );
  }

  // Infra events
  for (const infraKey of ["NIGHT_PROCESSED", "STATS_PRESENT"]) {
    const stats = eventMap.get(infraKey);
    if (stats) {
      const total = stats.total;
      const rate = nightsProcessed > 0 ? total / nightsProcessed : 0;
      const seasonValues = seasons.map((s) => stats.perSeason[s] ?? 0);
      const min = seasonValues.length > 0 ? Math.min(...seasonValues) : 0;
      const max = seasonValues.length > 0 ? Math.max(...seasonValues) : 0;

      const seasonCols = seasons
        .map((s) => String(stats.perSeason[s] ?? 0).padStart(8))
        .join("");

      console.log(
        `${("* " + infraKey).padEnd(40)}  ${String(total).padStart(6)}  ${(rate * 100).toFixed(1).padStart(5)}%  ${String(min).padStart(5)}  ${String(max).padStart(5)}${seasonCols}`
      );
    }
  }

  // Diagnostics
  console.log("\n--- Diagnostics ---\n");

  const broken = results.filter((r) => r.total === 0);
  if (broken.length > 0) {
    console.log("BROKEN EVENTS (0 hits ever):");
    for (const r of broken) {
      console.log(`  ${r.key}`);
    }
  } else {
    console.log("No broken events (all have at least 1 hit).");
  }

  const overlyCommon = results.filter((r) => r.rate >= 0.8);
  if (overlyCommon.length > 0) {
    console.log("\nOVERLY COMMON EVENTS (hit >= 80% of nights):");
    for (const r of overlyCommon) {
      console.log(`  ${r.key} — ${(r.rate * 100).toFixed(1)}%`);
    }
  }

  const lowVariance = results.filter(
    (r) => r.total > 0 && r.max > 0 && r.min === 0 && seasons.length > 1
  );
  if (lowVariance.length > 0) {
    console.log("\nSEASON GAPS (hits in some seasons but 0 in others):");
    for (const r of lowVariance) {
      const zeroSeasons = seasons.filter((s) => (r.perSeason[s] ?? 0) === 0);
      console.log(`  ${r.key} — missing in season(s): ${zeroSeasons.join(", ")}`);
    }
  }

  console.log("\n=== End Event Coverage Report ===");
}
