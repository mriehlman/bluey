import { prisma } from "../db/prisma.js";
import { CATALOG } from "../features/eventCatalog.js";
import { stabilityScore, computePatternScores } from "./scoring.js";
import type { PatternMetrics } from "./scoring.js";
import { DEFAULT_FILTER_CONFIG } from "./config.js";
import type { PatternFilterConfig } from "./config.js";
import type { Prisma } from "@prisma/client";

interface DateEntry {
  season: number;
  keys: Set<string>;
}

function generateCombos(items: string[], maxSize: number): string[][] {
  const results: string[][] = [];
  const sorted = [...items].sort();

  function recurse(start: number, current: string[]) {
    if (current.length > 0 && current.length <= maxSize) {
      results.push([...current]);
    }
    if (current.length === maxSize) return;
    for (let i = start; i < sorted.length; i++) {
      current.push(sorted[i]);
      recurse(i + 1, current);
      current.pop();
    }
  }

  recurse(0, []);
  return results;
}

function daysBetween(a: Date, b: Date): number {
  return Math.round(Math.abs(b.getTime() - a.getTime()) / (1000 * 60 * 60 * 24));
}

export async function searchPatterns(configOverrides?: Partial<PatternFilterConfig>): Promise<void> {
  const cfg = { ...DEFAULT_FILTER_CONFIG, ...configOverrides };
  const events = await prisma.nightEvent.findMany({
    orderBy: { date: "asc" },
  });

  console.log(`Loaded ${events.length} NightEvent rows`);
  console.log(`Filter config: minOcc=${cfg.minOccurrences} minSeasons=${cfg.minSeasonsWithHits} maxCluster=${cfg.maxClusterShare} avgHits=${cfg.minAvgHitsPerSeason}-${cfg.maxAvgHitsPerSeason}`);

  const dateMap = new Map<string, DateEntry>();
  const eventMetaIndex = new Map<string, unknown>();
  for (const e of events) {
    const dateStr = e.date.toISOString().slice(0, 10);
    let entry = dateMap.get(dateStr);
    if (!entry) {
      entry = { season: e.season, keys: new Set() };
      dateMap.set(dateStr, entry);
    }
    entry.keys.add(e.eventKey);
    if (e.meta != null) {
      eventMetaIndex.set(`${dateStr}::${e.eventKey}`, e.meta);
    }
  }

  const allKeys = CATALOG.map((d) => d.key);
  const presentKeys = allKeys.filter((k) =>
    [...dateMap.values()].some((e) => e.keys.has(k))
  );

  console.log(`${presentKeys.length} event keys present in data`);

  const combos = generateCombos(presentKeys, 3);
  console.log(`Generated ${combos.length} candidate patterns (size 1-3)`);

  const sortedDates = [...dateMap.keys()].sort();

  interface PatternCandidate {
    patternKey: string;
    eventKeys: string[];
    legs: number;
    hitDates: { date: string; season: number }[];
    metrics: PatternMetrics;
    score: number;
  }

  const qualifying: PatternCandidate[] = [];

  for (const combo of combos) {
    const hitDates: { date: string; season: number }[] = [];

    for (const dateStr of sortedDates) {
      const entry = dateMap.get(dateStr)!;
      if (combo.every((k) => entry.keys.has(k))) {
        hitDates.push({ date: dateStr, season: entry.season });
      }
    }

    if (hitDates.length < cfg.minOccurrences) continue;

    const perSeason: Record<number, number> = {};
    for (const h of hitDates) {
      perSeason[h.season] = (perSeason[h.season] ?? 0) + 1;
    }

    const seasons = Object.keys(perSeason).length;
    if (seasons < cfg.minSeasonsWithHits) continue;

    const maxSeasonHits = Math.max(...Object.values(perSeason));
    if (maxSeasonHits / hitDates.length > cfg.maxClusterShare) continue;

    if (hitDates.length < seasons * cfg.minAvgHitsPerSeason || hitDates.length > seasons * cfg.maxAvgHitsPerSeason) continue;

    let longestGapDays: number | null = null;
    if (hitDates.length >= 2) {
      longestGapDays = 0;
      for (let i = 1; i < hitDates.length; i++) {
        const gap = daysBetween(new Date(hitDates[i - 1].date), new Date(hitDates[i].date));
        longestGapDays = Math.max(longestGapDays, gap);
      }
    }

    const metrics: PatternMetrics = {
      occurrences: hitDates.length,
      seasons,
      perSeason,
      longestGapDays,
      legs: combo.length,
    };

    const score = stabilityScore(metrics);
    const patternKey = combo.join("+");

    qualifying.push({ patternKey, eventKeys: combo, legs: combo.length, hitDates, metrics, score });
  }

  console.log(`${qualifying.length} patterns passed filters`);

  const allSeasons = await prisma.nightEvent.findMany({
    where: { eventKey: "NIGHT_PROCESSED" },
    select: { season: true },
    distinct: ["season"],
  });
  const totalSeasons = new Set(allSeasons.map((s) => s.season)).size;
  const referenceDate = new Date();

  for (const p of qualifying) {
    const lastHitDate = new Date(p.hitDates[p.hitDates.length - 1].date);

    const metricsForScoring: PatternMetrics = {
      ...p.metrics,
      lastHitDate,
    };
    const scores = computePatternScores(metricsForScoring, totalSeasons, referenceDate);

    await prisma.pattern.upsert({
      where: { patternKey: p.patternKey },
      update: {
        eventKeys: p.eventKeys,
        legs: p.legs,
        occurrences: p.metrics.occurrences,
        seasons: p.metrics.seasons,
        perSeason: p.metrics.perSeason,
        longestGapDays: p.metrics.longestGapDays,
        lastHitDate,
        ...scores,
      },
      create: {
        patternKey: p.patternKey,
        eventKeys: p.eventKeys,
        legs: p.legs,
        occurrences: p.metrics.occurrences,
        seasons: p.metrics.seasons,
        perSeason: p.metrics.perSeason,
        longestGapDays: p.metrics.longestGapDays,
        lastHitDate,
        ...scores,
      },
    });

    const patternRecord = await prisma.pattern.findUnique({
      where: { patternKey: p.patternKey },
      select: { id: true },
    });

    if (patternRecord) {
      await prisma.patternHit.createMany({
        data: p.hitDates.map((h) => {
          const eventMetas: Record<string, unknown> = {};
          const allTeamIds = new Set<number>();
          let gameCount: number | undefined;

          for (const ek of p.eventKeys) {
            const m = eventMetaIndex.get(`${h.date}::${ek}`);
            if (m != null) {
              eventMetas[ek] = m;
              const meta = m as Record<string, unknown>;
              if (Array.isArray(meta.teamIds)) {
                for (const t of meta.teamIds) allTeamIds.add(t as number);
              }
              if (meta.gameCount != null && gameCount == null) {
                gameCount = meta.gameCount as number;
              }
            }
          }

          const nightProcessed = eventMetaIndex.get(`${h.date}::NIGHT_PROCESSED`);
          if (nightProcessed != null && gameCount == null) {
            gameCount = (nightProcessed as Record<string, unknown>).gameCount as number | undefined;
          }

          return {
            patternId: patternRecord.id,
            date: new Date(h.date),
            season: h.season,
            meta: {
              gameCount: gameCount ?? null,
              teamIds: [...allTeamIds],
              eventMetas: eventMetas as Record<string, Prisma.InputJsonValue>,
            },
          };
        }),
        skipDuplicates: true,
      });
    }
  }

  const ranked = qualifying.sort((a, b) => b.score - a.score);
  const top = ranked.slice(0, 20);

  console.log(`\n=== Top ${top.length} Patterns by Stability Score ===\n`);
  console.log(
    "Rank  Score   Legs  Occ  Seasons  LongestGap  LastHit     Pattern"
  );
  console.log("-".repeat(90));

  for (let i = 0; i < top.length; i++) {
    const p = top[i];
    const lastHit = p.hitDates[p.hitDates.length - 1].date;
    console.log(
      `${String(i + 1).padStart(4)}  ` +
        `${p.score.toFixed(2).padStart(6)}  ` +
        `${String(p.legs).padStart(4)}  ` +
        `${String(p.metrics.occurrences).padStart(3)}  ` +
        `${String(p.metrics.seasons).padStart(7)}  ` +
        `${String(p.metrics.longestGapDays ?? "-").padStart(10)}  ` +
        `${lastHit}  ` +
        `${p.patternKey}`
    );
  }

  console.log(`\nDone. ${qualifying.length} patterns stored.`);
}
