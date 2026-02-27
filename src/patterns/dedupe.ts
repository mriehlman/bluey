import { prisma } from "../db/prisma.js";

interface DedupeFlags {
  threshold: number;
  minOcc: number;
  top: number;
}

interface PatternSummary {
  id: string;
  patternKey: string;
  eventKeys: string[];
  legs: number;
  occurrences: number;
  overallScore: number | null;
  hitDates: Set<string>;
}

interface RedundancyResult {
  pattern: string;
  legs: number;
  occurrences: number;
  redundantWith: string;
  simplerLegs: number;
  simplerOccurrences: number;
  overlap: number;
  overlapPct: number;
  hitReduction: number;
  verdict: string;
}

function setIntersection(a: Set<string>, b: Set<string>): number {
  let count = 0;
  for (const d of a) {
    if (b.has(d)) count++;
  }
  return count;
}

function jaccard(a: Set<string>, b: Set<string>): number {
  const intersection = setIntersection(a, b);
  const union = a.size + b.size - intersection;
  return union > 0 ? intersection / union : 0;
}

export async function dedupePatterns(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const opts: DedupeFlags = {
    threshold: flags.threshold ? Number(flags.threshold) : 0.9,
    minOcc: flags.minOcc ? Number(flags.minOcc) : 3,
    top: flags.top ? Number(flags.top) : 50,
  };

  console.log(`=== Pattern Redundancy Analysis ===`);
  console.log(`  threshold: ${(opts.threshold * 100).toFixed(0)}%, minOcc: ${opts.minOcc}\n`);

  const patterns = await prisma.pattern.findMany({
    include: {
      hits: { select: { date: true } },
    },
    orderBy: { overallScore: "desc" },
  });

  const summaries: PatternSummary[] = patterns
    .map((p) => ({
      id: p.id,
      patternKey: p.patternKey,
      eventKeys: p.eventKeys,
      legs: p.legs,
      occurrences: p.occurrences,
      overallScore: p.overallScore,
      hitDates: new Set(p.hits.map((h) => h.date.toISOString().slice(0, 10))),
    }))
    .filter((s) => s.occurrences >= opts.minOcc);

  console.log(`Loaded ${patterns.length} patterns, ${summaries.length} pass minOcc >= ${opts.minOcc}\n`);

  const byLegs = new Map<number, PatternSummary[]>();
  for (const s of summaries) {
    let arr = byLegs.get(s.legs);
    if (!arr) {
      arr = [];
      byLegs.set(s.legs, arr);
    }
    arr.push(s);
  }

  const results: RedundancyResult[] = [];

  for (const complex of summaries) {
    if (complex.legs <= 1) continue;
    if (complex.hitDates.size === 0) continue;

    const complexKeys = new Set(complex.eventKeys);

    // Subset redundancy: containment = |A ∩ B| / |A| where A is the complex pattern
    for (let simplerLegCount = 1; simplerLegCount < complex.legs; simplerLegCount++) {
      const simplerPatterns = byLegs.get(simplerLegCount) ?? [];

      for (const simpler of simplerPatterns) {
        if (simpler.hitDates.size === 0) continue;

        const isSubset = simpler.eventKeys.every((k) => complexKeys.has(k));
        if (!isSubset) continue;

        const overlap = setIntersection(complex.hitDates, simpler.hitDates);
        const containment = overlap / complex.hitDates.size;

        if (containment >= opts.threshold) {
          const hitReduction = simpler.hitDates.size > 0
            ? 1 - complex.hitDates.size / simpler.hitDates.size
            : 0;

          results.push({
            pattern: complex.patternKey,
            legs: complex.legs,
            occurrences: complex.occurrences,
            redundantWith: simpler.patternKey,
            simplerLegs: simpler.legs,
            simplerOccurrences: simpler.occurrences,
            overlap,
            overlapPct: containment,
            hitReduction,
            verdict: containment >= 1.0 ? "FULLY_REDUNDANT" : "MOSTLY_REDUNDANT",
          });
        }
      }
    }

    // Near-duplicate: same legs, different keys, Jaccard similarity
    const sameLeg = byLegs.get(complex.legs) ?? [];
    for (const other of sameLeg) {
      if (other.patternKey <= complex.patternKey) continue;
      if (other.hitDates.size === 0) continue;

      const j = jaccard(complex.hitDates, other.hitDates);

      if (j >= opts.threshold) {
        results.push({
          pattern: complex.patternKey,
          legs: complex.legs,
          occurrences: complex.occurrences,
          redundantWith: other.patternKey,
          simplerLegs: other.legs,
          simplerOccurrences: other.occurrences,
          overlap: setIntersection(complex.hitDates, other.hitDates),
          overlapPct: j,
          hitReduction: 0,
          verdict: "NEAR_DUPLICATE",
        });
      }
    }
  }

  results.sort((a, b) => {
    if (a.verdict !== b.verdict) {
      const order = { FULLY_REDUNDANT: 0, MOSTLY_REDUNDANT: 1, NEAR_DUPLICATE: 2 };
      return (order[a.verdict as keyof typeof order] ?? 3) - (order[b.verdict as keyof typeof order] ?? 3);
    }
    return b.overlapPct - a.overlapPct;
  });

  const display = results.slice(0, opts.top);

  if (display.length === 0) {
    console.log("No redundant patterns found at this threshold.\n");
    return;
  }

  console.log(
    "  " +
    "Pattern".padEnd(45) +
    "Legs".padStart(4) +
    "  Occ".padStart(5) +
    "  Overlap".padStart(9) +
    "  HitRed".padStart(8) +
    "  Verdict".padStart(18) +
    "  SimpOcc".padStart(8) +
    "  Redundant With"
  );
  console.log("  " + "-".repeat(140));

  for (const r of display) {
    const hitRedStr = r.verdict === "NEAR_DUPLICATE" ? "--" : `${(r.hitReduction * 100).toFixed(0)}%`;
    console.log(
      "  " +
      r.pattern.padEnd(45) +
      String(r.legs).padStart(4) +
      String(r.occurrences).padStart(5) +
      `${(r.overlapPct * 100).toFixed(0)}%`.padStart(9) +
      hitRedStr.padStart(8) +
      r.verdict.padStart(18) +
      String(r.simplerOccurrences).padStart(8) +
      `  ${r.redundantWith}`
    );
  }

  const fullyRedundant = results.filter((r) => r.verdict === "FULLY_REDUNDANT").length;
  const mostlyRedundant = results.filter((r) => r.verdict === "MOSTLY_REDUNDANT").length;
  const nearDupes = results.filter((r) => r.verdict === "NEAR_DUPLICATE").length;
  const uniqueRedundantPatterns = new Set(results.map((r) => r.pattern)).size;

  console.log(`\n  Summary:`);
  console.log(`    Total flagged pairs:     ${results.length}`);
  console.log(`    Unique patterns flagged: ${uniqueRedundantPatterns} / ${summaries.length}`);
  console.log(`    Fully redundant:         ${fullyRedundant}`);
  console.log(`    Mostly redundant:        ${mostlyRedundant}`);
  console.log(`    Near duplicates (Jaccard): ${nearDupes}`);

  console.log(`\n  Overlap definitions:`);
  console.log(`    FULLY/MOSTLY_REDUNDANT: containment = |A ∩ B| / |A|  (A = complex pattern)`);
  console.log(`    NEAR_DUPLICATE:         Jaccard     = |A ∩ B| / |A ∪ B|`);
  console.log(`    HitRed = 1 - |complex|/|simpler|  (how much the extra leg narrows hits)`);

  // Lift / support for top patterns
  const topPatterns = summaries.filter((s) => s.overallScore != null).slice(0, 10);
  if (topPatterns.length > 0) {
    console.log(`\n  Lift / Support for Top ${topPatterns.length} Patterns:\n`);

    const totalNights = await prisma.nightEvent.count({
      where: { eventKey: "NIGHT_PROCESSED" },
    });

    console.log(
      "    " +
      "Pattern".padEnd(45) +
      "Support".padStart(8) +
      "  Hits".padStart(6) +
      "  Score".padStart(7)
    );
    console.log("    " + "-".repeat(70));

    for (const p of topPatterns) {
      const support = totalNights > 0 ? p.hitDates.size / totalNights : 0;
      console.log(
        "    " +
        p.patternKey.padEnd(45) +
        `${(support * 100).toFixed(1)}%`.padStart(8) +
        String(p.hitDates.size).padStart(6) +
        (p.overallScore?.toFixed(3) ?? "-").padStart(7)
      );
    }
  }

  console.log(`\n=== End Redundancy Analysis ===`);
}
