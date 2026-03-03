import { prisma } from "../db/prisma.js";
import { scorePattern } from "./gamePatternScoring.js";

interface SearchConfig {
  minSample: number;
  minHitRate: number;
  maxLegs: number;
  maxResults: number;
  minFrequency: number;
  minSeasons: number;
  /** Minimum edge over baseline (e.g. 0.02 = 2%) */
  minEdge?: number;
  /** Max share of hits in any single season (e.g. 0.8 = filter out patterns with 80%+ in one season) */
  maxSeasonShare?: number;
}

const DEFAULT_CONFIG: SearchConfig = {
  minSample: 15,
  minHitRate: 0.58,
  maxLegs: 4,
  maxResults: 2000,
  minFrequency: 12,
  minSeasons: 1,
  minEdge: 0.02,
  maxSeasonShare: 0.85,
};

interface EventRow {
  gameId: string;
  season: number;
  eventKey: string;
  type: string;
  side: string;
}

export async function searchGamePatterns(overrides?: Partial<SearchConfig>): Promise<void> {
  const config = { ...DEFAULT_CONFIG, ...overrides };
  console.log("\n=== Game Pattern Search ===\n");
  console.log(`Config: minSample=${config.minSample}, minHitRate=${config.minHitRate}, maxLegs=${config.maxLegs}, maxResults=${config.maxResults}`);

  console.log("\nLoading GameEvent rows...");
  const allEvents: EventRow[] = await prisma.gameEvent.findMany({
    select: { gameId: true, season: true, eventKey: true, type: true, side: true },
  });
  console.log(`  Loaded ${allEvents.length} events`);

  // Build inverted indexes: compositeKey -> Set<gameId>
  const conditionIndex = new Map<string, Set<string>>();
  const outcomeIndex = new Map<string, Set<string>>();

  // Track season for each game
  const gameSeason = new Map<string, number>();

  for (const evt of allEvents) {
    gameSeason.set(evt.gameId, evt.season);
    const compositeKey = `${evt.eventKey}:${evt.side}`;

    if (evt.type === "condition") {
      const set = conditionIndex.get(compositeKey) ?? new Set();
      set.add(evt.gameId);
      conditionIndex.set(compositeKey, set);
    } else {
      const set = outcomeIndex.get(compositeKey) ?? new Set();
      set.add(evt.gameId);
      outcomeIndex.set(compositeKey, set);
    }
  }

  // Filter to condition keys with sufficient frequency
  const viableConditions: string[] = [];
  for (const [key, games] of conditionIndex) {
    if (games.size >= config.minFrequency) {
      viableConditions.push(key);
    }
  }
  viableConditions.sort();
  console.log(`  ${viableConditions.length} viable condition keys (>= ${config.minFrequency} games)`);
  console.log(`  ${outcomeIndex.size} outcome keys`);

  // Get all seasons for scoring
  const allSeasons = new Set<number>();
  for (const s of gameSeason.values()) allSeasons.add(s);
  const totalSeasons = allSeasons.size;

  // Lightweight candidate - gameHits recomputed only for top N when storing
  type CandidateLight = {
    combo: string[];
    outcomeKey: string;
    sampleSize: number;
    hitCount: number;
    hitRate: number;
    seasons: number;
    perSeason: Record<string, number>;
    lastHitDate: Date | null;
  };

  const candidates: CandidateLight[] = [];
  let combosChecked = 0;

  function generateCombos(maxLegs: number): string[][] {
    const combos: string[][] = [];
    for (let i = 0; i < viableConditions.length; i++) {
      combos.push([viableConditions[i]]);
      if (maxLegs >= 2) {
        for (let j = i + 1; j < viableConditions.length; j++) {
          combos.push([viableConditions[i], viableConditions[j]]);
          if (maxLegs >= 3) {
            for (let k = j + 1; k < viableConditions.length; k++) {
              combos.push([viableConditions[i], viableConditions[j], viableConditions[k]]);
              if (maxLegs >= 4) {
                for (let m = k + 1; m < viableConditions.length; m++) {
                  combos.push([viableConditions[i], viableConditions[j], viableConditions[k], viableConditions[m]]);
                }
              }
            }
          }
        }
      }
    }
    return combos;
  }

  console.log("\nGenerating condition combinations...");
  const combos = generateCombos(config.maxLegs);
  console.log(`  ${combos.length} combinations to evaluate`);

  // Pre-fetch game dates for lastHitDate tracking
  const gameDates = new Map<string, Date>();
  const gameDateRows = await prisma.game.findMany({
    select: { id: true, date: true },
  });
  for (const row of gameDateRows) {
    gameDates.set(row.id, row.date);
  }

  console.log("\nSearching patterns...");
  let maxSample = 0;

  for (const combo of combos) {
    combosChecked++;

    // Intersect condition sets, smallest first
    const sets = combo.map((key) => conditionIndex.get(key)!);
    sets.sort((a, b) => a.size - b.size);

    let intersection = new Set(sets[0]);
    for (let s = 1; s < sets.length; s++) {
      const next = new Set<string>();
      for (const id of intersection) {
        if (sets[s].has(id)) next.add(id);
      }
      intersection = next;
      if (intersection.size < config.minSample) break;
    }

    if (intersection.size < config.minSample) continue;

    const sampleSize = intersection.size;
    if (sampleSize > maxSample) maxSample = sampleSize;

    // Check each outcome (lightweight - no gameHits allocation)
    for (const [outcomeKey, outcomeGames] of outcomeIndex) {
      let hitCount = 0;
      const perSeason: Record<string, number> = {};
      let lastHitDate: Date | null = null;

      for (const gameId of intersection) {
        if (outcomeGames.has(gameId)) {
          hitCount++;
          const season = gameSeason.get(gameId) ?? 0;
          const sKey = String(season);
          perSeason[sKey] = (perSeason[sKey] ?? 0) + 1;
          const gDate = gameDates.get(gameId);
          if (gDate && (!lastHitDate || gDate > lastHitDate)) lastHitDate = gDate;
        }
      }

      const hitRate = hitCount / sampleSize;
      if (hitRate < config.minHitRate) continue;

      const seasons = Object.keys(perSeason).length;
      if (seasons < config.minSeasons) continue;

      const edge = hitRate - 0.524;
      if (config.minEdge != null && edge < config.minEdge) continue;

      const seasonCounts = Object.values(perSeason);
      const maxSeasonShareVal = seasonCounts.length > 0 ? Math.max(...seasonCounts) / sampleSize : 1;
      if (config.maxSeasonShare != null && maxSeasonShareVal > config.maxSeasonShare) continue;

      candidates.push({
        combo: [...combo],
        outcomeKey,
        sampleSize,
        hitCount,
        hitRate,
        seasons,
        perSeason,
        lastHitDate,
      });
    }

    if (combosChecked % 500 === 0) {
      process.stdout.write(`  ${combosChecked}/${combos.length} combos checked, ${candidates.length} candidates so far\r`);
    }
  }

  console.log(`\n  Checked ${combosChecked} combos, found ${candidates.length} candidate patterns`);

  // Score all candidates (add scores to lightweight objs)
  type ScoredCandidate = CandidateLight & { confidenceScore: number; valueScore: number };
  const scored: ScoredCandidate[] = candidates.map((c) => {
    const { confidenceScore, valueScore } = scorePattern({
      hitRate: c.hitRate,
      sampleSize: c.sampleSize,
      seasons: c.seasons,
      totalSeasons,
      maxSample,
      perSeason: c.perSeason,
      lastHitDate: c.lastHitDate,
    });
    return { ...c, confidenceScore, valueScore };
  });

  // Sort by confidence, take top N
  scored.sort((a, b) => b.confidenceScore - a.confidenceScore || b.valueScore - a.valueScore);
  const topPatterns = scored.slice(0, config.maxResults);

  console.log(`\nStoring top ${topPatterns.length} patterns...`);

  // Helper: recompute intersection and gameHits for a pattern (only for top N)
  function getGameHits(c: CandidateLight): { gameId: string; season: number; hit: boolean }[] {
    const sets = c.combo.map((key) => conditionIndex.get(key)!);
    sets.sort((a, b) => a.size - b.size);
    let intersection = new Set(sets[0]);
    for (let s = 1; s < sets.length; s++) {
      const next = new Set<string>();
      for (const id of intersection) {
        if (sets[s].has(id)) next.add(id);
      }
      intersection = next;
    }
    const outcomeGames = outcomeIndex.get(c.outcomeKey)!;
    const out: { gameId: string; season: number; hit: boolean }[] = [];
    for (const gameId of intersection) {
      out.push({ gameId, season: gameSeason.get(gameId) ?? 0, hit: outcomeGames.has(gameId) });
    }
    return out;
  }

  // Clear existing game patterns
  await prisma.gamePatternHit.deleteMany({});
  await prisma.gamePattern.deleteMany({});

  let hitAccum: { patternId: string; gameId: string; season: number; hit: boolean }[] = [];
  const flushHits = async () => {
    if (hitAccum.length > 0) {
      await prisma.gamePatternHit.createMany({ data: hitAccum, skipDuplicates: true });
      hitAccum = [];
    }
  };

  let stored = 0;
  for (const pattern of topPatterns) {
    const patternKey = [...pattern.combo, "->", pattern.outcomeKey].join("|");
    const gp = await prisma.gamePattern.create({
      data: {
        patternKey,
        conditions: pattern.combo,
        outcome: pattern.outcomeKey,
        sampleSize: pattern.sampleSize,
        hitCount: pattern.hitCount,
        hitRate: pattern.hitRate,
        seasons: pattern.seasons,
        perSeason: pattern.perSeason as object,
        lastHitDate: pattern.lastHitDate,
        confidenceScore: pattern.confidenceScore,
        valueScore: pattern.valueScore,
      },
    });

    for (const h of getGameHits(pattern)) {
      hitAccum.push({ patternId: gp.id, gameId: h.gameId, season: h.season, hit: h.hit });
    }
    if (hitAccum.length >= 5000) await flushHits();

    stored++;
    if (stored % 100 === 0) {
      process.stdout.write(`  Stored ${stored}/${topPatterns.length}\r`);
    }
  }
  await flushHits();

  console.log(`\n=== Search complete: ${stored} GamePatterns stored ===`);

  // Print top 10 summary
  console.log("\nTop 10 patterns by confidence:\n");
  for (const p of topPatterns.slice(0, 10)) {
    console.log(`  ${p.combo.join(" + ")}  ->  ${p.outcomeKey}`);
    console.log(`    Hit rate: ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  ${p.seasons} seasons  |  confidence: ${p.confidenceScore.toFixed(3)}  |  value: ${p.valueScore.toFixed(3)}`);
  }
}
