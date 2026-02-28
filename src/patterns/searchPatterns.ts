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

  const invertedIndex = new Map<string, Set<string>>();
  for (const key of presentKeys) {
    const dates = new Set<string>();
    for (const [dateStr, entry] of dateMap) {
      if (entry.keys.has(key)) dates.add(dateStr);
    }
    invertedIndex.set(key, dates);
  }

  const viableKeys = presentKeys.filter(
    (k) => (invertedIndex.get(k)?.size ?? 0) >= cfg.minOccurrences,
  );

  console.log(`${viableKeys.length} keys viable (>= ${cfg.minOccurrences} hits), ${presentKeys.length - viableKeys.length} pruned`);

  const combos = generateCombos(viableKeys, 4);
  console.log(`Generated ${combos.length} candidate patterns (size 1-4)`);

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
    const sets = combo.map((k) => invertedIndex.get(k)!);
    sets.sort((a, b) => a.size - b.size);

    let current = sets[0];
    for (let i = 1; i < sets.length; i++) {
      if (current.size < cfg.minOccurrences) break;
      const next = sets[i];
      const intersection = new Set<string>();
      for (const d of current) {
        if (next.has(d)) intersection.add(d);
      }
      current = intersection;
    }

    if (current.size < cfg.minOccurrences) continue;

    const hitDates = [...current]
      .sort()
      .map((d) => ({ date: d, season: dateMap.get(d)!.season }));

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
    const metricsForScoring: PatternMetrics = { ...p.metrics, lastHitDate };
    const scores = computePatternScores(metricsForScoring, totalSeasons, referenceDate);
    p.score = scores.overallScore;
  }

  qualifying.sort((a, b) => b.score - a.score);

  const toStore = qualifying.slice(0, cfg.maxResults);
  if (qualifying.length > cfg.maxResults) {
    console.log(`Capped to top ${cfg.maxResults} by overallScore (from ${qualifying.length})`);
  }

  console.log(`Persisting ${toStore.length} patterns...`);
  const startPersist = Date.now();

  for (let idx = 0; idx < toStore.length; idx++) {
    const p = toStore[idx];
    const lastHitDate = new Date(p.hitDates[p.hitDates.length - 1].date);

    const metricsForScoring: PatternMetrics = {
      ...p.metrics,
      lastHitDate,
    };
    const scores = computePatternScores(metricsForScoring, totalSeasons, referenceDate);

    const patternRecord = await prisma.pattern.upsert({
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

    if ((idx + 1) % 500 === 0 || idx + 1 === toStore.length) {
      const elapsed = ((Date.now() - startPersist) / 1000).toFixed(1);
      console.log(`  ${idx + 1} / ${toStore.length} persisted (${elapsed}s)`);
    }
  }

  const top = toStore.slice(0, 20);

  console.log(`\n=== Top ${top.length} Patterns by Overall Score ===\n`);
  console.log(
    "Rank  Score   Legs  Occ  Seasons  LongestGap  LastHit     Pattern"
  );
  console.log("-".repeat(90));

  for (let i = 0; i < top.length; i++) {
    const p = top[i];
    const lastHit = p.hitDates[p.hitDates.length - 1].date;
    console.log(
      `${String(i + 1).padStart(4)}  ` +
        `${p.score.toFixed(4).padStart(6)}  ` +
        `${String(p.legs).padStart(4)}  ` +
        `${String(p.metrics.occurrences).padStart(3)}  ` +
        `${String(p.metrics.seasons).padStart(7)}  ` +
        `${String(p.metrics.longestGapDays ?? "-").padStart(10)}  ` +
        `${lastHit}  ` +
        `${p.patternKey}`
    );
  }

  console.log(`\nDone. ${toStore.length} patterns stored.`);
}
