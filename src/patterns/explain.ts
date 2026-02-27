import { prisma } from "../db/prisma.js";
import { stabilityScore } from "./scoring.js";
import type { PatternMetrics } from "./scoring.js";

export async function explainPattern(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const patternId = flags.patternId;
  const patternKey = flags.patternKey;

  if (!patternId && !patternKey) {
    console.error("Usage: patterns:explain --patternId <id> or --patternKey <KEY1+KEY2>");
    process.exit(1);
  }

  const pattern = await prisma.pattern.findUnique({
    where: patternId ? { id: patternId } : { patternKey: patternKey! },
    include: {
      hits: { orderBy: { date: "asc" } },
    },
  });

  if (!pattern) {
    console.error(`Pattern not found: ${patternId ?? patternKey}`);
    process.exit(1);
  }

  const metrics: PatternMetrics = {
    occurrences: pattern.occurrences,
    seasons: pattern.seasons,
    perSeason: pattern.perSeason as Record<number, number>,
    longestGapDays: pattern.longestGapDays,
    legs: pattern.legs,
  };
  const score = stabilityScore(metrics);

  console.log("=== Pattern Explain ===\n");
  console.log(`  Pattern Key:     ${pattern.patternKey}`);
  console.log(`  Event Keys:      ${pattern.eventKeys.join(", ")}`);
  console.log(`  Legs:            ${pattern.legs}`);
  console.log(`  Occurrences:     ${pattern.occurrences}`);
  console.log(`  Seasons:         ${pattern.seasons}`);
  console.log(`  Stability Score: ${score.toFixed(2)}`);
  console.log(`  Longest Gap:     ${pattern.longestGapDays ?? "-"} days`);
  console.log(`  Last Hit:        ${pattern.lastHitDate?.toISOString().slice(0, 10) ?? "-"}`);

  const perSeason = pattern.perSeason as Record<string, number>;
  console.log("\n  Per-Season Breakdown:");
  for (const [season, count] of Object.entries(perSeason)) {
    console.log(`    Season ${season}: ${count} hits`);
  }

  console.log(`\n  Hit Dates (${pattern.hits.length}):\n`);
  console.log("  Date        Season  Meta");
  console.log("  " + "-".repeat(70));

  for (const hit of pattern.hits) {
    const dateStr = hit.date.toISOString().slice(0, 10);
    const meta = hit.meta as Record<string, unknown> | null;

    let metaSummary = "-";
    if (meta) {
      const parts: string[] = [];
      if (meta.gameCount != null) parts.push(`${meta.gameCount} games`);
      if (Array.isArray(meta.teamIds) && meta.teamIds.length > 0) {
        parts.push(`teams: [${meta.teamIds.join(",")}]`);
      }
      if (meta.eventMetas && typeof meta.eventMetas === "object") {
        const eventKeys = Object.keys(meta.eventMetas as object);
        parts.push(`events: ${eventKeys.length} keys`);
      }
      if (parts.length > 0) metaSummary = parts.join(" | ");
    }

    console.log(
      `  ${dateStr}  ${String(hit.season).padStart(6)}  ${metaSummary}`
    );
  }

  if (pattern.hits.some((h) => h.meta != null)) {
    console.log("\n  Detailed Event Metas per Hit:\n");
    for (const hit of pattern.hits) {
      const meta = hit.meta as Record<string, unknown> | null;
      if (!meta?.eventMetas) continue;
      const dateStr = hit.date.toISOString().slice(0, 10);
      console.log(`  ${dateStr}:`);
      const eventMetas = meta.eventMetas as Record<string, unknown>;
      for (const [key, val] of Object.entries(eventMetas)) {
        console.log(`    ${key}: ${JSON.stringify(val)}`);
      }
    }
  }

  console.log("\n=== End Explain ===");
}
