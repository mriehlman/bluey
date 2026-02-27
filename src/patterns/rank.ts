import { prisma } from "../db/prisma.js";
import { computePatternScores } from "./scoring.js";
import type { PatternMetrics } from "./scoring.js";
import type { Prisma } from "@prisma/client";

interface RankFlags {
  seasonFrom?: number;
  seasonTo?: number;
  legs?: number;
  top: number;
}

export async function rankPatterns(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const opts: RankFlags = {
    seasonFrom: flags.seasonFrom ? Number(flags.seasonFrom) : undefined,
    seasonTo: flags.seasonTo ? Number(flags.seasonTo) : undefined,
    legs: flags.legs ? Number(flags.legs) : undefined,
    top: flags.top ? Number(flags.top) : 25,
  };

  const where: Prisma.PatternWhereInput = {};
  if (opts.legs) where.legs = opts.legs;

  const allSeasons = await prisma.nightEvent.findMany({
    where: { eventKey: "NIGHT_PROCESSED" },
    select: { season: true },
    distinct: ["season"],
  });
  const seasonSet = new Set(allSeasons.map((s) => s.season));

  if (opts.seasonFrom) {
    for (const s of seasonSet) {
      if (s < opts.seasonFrom!) seasonSet.delete(s);
    }
  }
  if (opts.seasonTo) {
    for (const s of seasonSet) {
      if (s > opts.seasonTo!) seasonSet.delete(s);
    }
  }
  const totalSeasons = seasonSet.size;

  if (opts.seasonFrom || opts.seasonTo) {
    where.hits = {
      some: {
        season: {
          ...(opts.seasonFrom ? { gte: opts.seasonFrom } : {}),
          ...(opts.seasonTo ? { lte: opts.seasonTo } : {}),
        },
      },
    };
  }

  const patterns = await prisma.pattern.findMany({
    where,
    include: {
      hits: {
        orderBy: { date: "asc" },
        ...(opts.seasonFrom || opts.seasonTo
          ? {
              where: {
                season: {
                  ...(opts.seasonFrom ? { gte: opts.seasonFrom } : {}),
                  ...(opts.seasonTo ? { lte: opts.seasonTo } : {}),
                },
              },
            }
          : {}),
      },
    },
  });

  console.log(`Scoring ${patterns.length} patterns across ${totalSeasons} seasons...\n`);

  const referenceDate = new Date();

  interface ScoredPattern {
    patternKey: string;
    legs: number;
    occurrences: number;
    seasonsWithHits: number;
    avgHitsPerSeason: number;
    clusterShare: number;
    longestGapDays: number | null;
    lastHitDate: string | null;
    stabilityScore: number;
    rarityScore: number;
    balanceScore: number;
    recencyScore: number;
    overallScore: number;
    firstHits: string[];
  }

  const scored: ScoredPattern[] = [];

  for (const p of patterns) {
    const hitsInRange = p.hits;
    if (hitsInRange.length === 0) continue;

    const perSeason: Record<number, number> = {};
    for (const h of hitsInRange) {
      perSeason[h.season] = (perSeason[h.season] ?? 0) + 1;
    }

    const seasonsWithHits = Object.keys(perSeason).length;
    const maxSeasonHits = Math.max(...Object.values(perSeason), 0);
    const clusterShare = hitsInRange.length > 0 ? maxSeasonHits / hitsInRange.length : 0;

    let longestGapDays: number | null = null;
    if (hitsInRange.length >= 2) {
      longestGapDays = 0;
      for (let i = 1; i < hitsInRange.length; i++) {
        const gap = Math.round(
          Math.abs(hitsInRange[i].date.getTime() - hitsInRange[i - 1].date.getTime()) /
            (1000 * 60 * 60 * 24)
        );
        longestGapDays = Math.max(longestGapDays, gap);
      }
    }

    const lastHit = hitsInRange[hitsInRange.length - 1].date;

    const metrics: PatternMetrics = {
      occurrences: hitsInRange.length,
      seasons: seasonsWithHits,
      perSeason,
      longestGapDays,
      legs: p.legs,
      lastHitDate: lastHit,
    };

    const scores = computePatternScores(metrics, totalSeasons, referenceDate);

    await prisma.pattern.update({
      where: { id: p.id },
      data: {
        stabilityScore: scores.stabilityScore,
        rarityScore: scores.rarityScore,
        balanceScore: scores.balanceScore,
        recencyScore: scores.recencyScore,
        overallScore: scores.overallScore,
      },
    });

    scored.push({
      patternKey: p.patternKey,
      legs: p.legs,
      occurrences: hitsInRange.length,
      seasonsWithHits,
      avgHitsPerSeason: parseFloat((hitsInRange.length / seasonsWithHits).toFixed(2)),
      clusterShare: parseFloat(clusterShare.toFixed(3)),
      longestGapDays,
      lastHitDate: lastHit.toISOString().slice(0, 10),
      ...scores,
      firstHits: hitsInRange.slice(0, 3).map((h) => h.date.toISOString().slice(0, 10)),
    });
  }

  scored.sort((a, b) => b.overallScore - a.overallScore);
  const topN = scored.slice(0, opts.top);

  console.log(`=== Research Feed — Top ${topN.length} of ${scored.length} Patterns ===\n`);

  const header = [
    "#".padStart(3),
    "Overall".padStart(7),
    "Stab".padStart(5),
    "Bal".padStart(5),
    "Rare".padStart(5),
    "Rec".padStart(5),
    "Legs".padStart(4),
    "Occ".padStart(4),
    "Sns".padStart(4),
    "Avg/S".padStart(6),
    "Clstr".padStart(6),
    "MaxGap".padStart(7),
    "LastHit".padStart(11),
    "Pattern",
  ].join("  ");

  console.log(header);
  console.log("-".repeat(header.length + 20));

  for (let i = 0; i < topN.length; i++) {
    const p = topN[i];
    const row = [
      String(i + 1).padStart(3),
      p.overallScore.toFixed(3).padStart(7),
      p.stabilityScore.toFixed(2).padStart(5),
      p.balanceScore.toFixed(2).padStart(5),
      p.rarityScore.toFixed(2).padStart(5),
      p.recencyScore.toFixed(2).padStart(5),
      String(p.legs).padStart(4),
      String(p.occurrences).padStart(4),
      String(p.seasonsWithHits).padStart(4),
      p.avgHitsPerSeason.toFixed(1).padStart(6),
      p.clusterShare.toFixed(2).padStart(6),
      String(p.longestGapDays ?? "-").padStart(7),
      (p.lastHitDate ?? "-").padStart(11),
      p.patternKey,
    ].join("  ");
    console.log(row);
  }

  console.log(`\n  First 3 hit dates for top ${Math.min(5, topN.length)}:\n`);
  for (let i = 0; i < Math.min(5, topN.length); i++) {
    const p = topN[i];
    console.log(`  ${i + 1}. ${p.patternKey}`);
    console.log(`     ${p.firstHits.join(", ")}`);
  }

  console.log(`\nScored ${scored.length} patterns. Scores persisted to DB.`);
}
