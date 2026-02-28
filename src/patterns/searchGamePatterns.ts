import { prisma } from "../db/prisma.js";
import { scorePattern } from "./gamePatternScoring.js";

interface SearchConfig {
  minSample: number;
  minHitRate: number;
  maxLegs: number;
  maxResults: number;
  minFrequency: number;
  minSeasons: number;
}

const DEFAULT_CONFIG: SearchConfig = {
  minSample: 15,
  minHitRate: 0.58,
  maxLegs: 3,
  maxResults: 2000,
  minFrequency: 15,
  minSeasons: 1,
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

  // Generate combinations (1, 2, 3 legs)
  type Candidate = {
    patternKey: string;
    conditions: string[];
    outcome: string;
    sampleSize: number;
    hitCount: number;
    hitRate: number;
    seasons: number;
    perSeason: Record<string, number>;
    lastHitDate: Date | null;
    confidenceScore: number;
    valueScore: number;
    gameHits: { gameId: string; season: number; hit: boolean }[];
  };

  const candidates: Candidate[] = [];
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

    // Check each outcome
    for (const [outcomeKey, outcomeGames] of outcomeIndex) {
      let hitCount = 0;
      const gameHits: { gameId: string; season: number; hit: boolean }[] = [];
      const perSeason: Record<string, number> = {};
      let lastHitDate: Date | null = null;

      for (const gameId of intersection) {
        const hit = outcomeGames.has(gameId);
        const season = gameSeason.get(gameId) ?? 0;
        gameHits.push({ gameId, season, hit });

        if (hit) {
          hitCount++;
          const sKey = String(season);
          perSeason[sKey] = (perSeason[sKey] ?? 0) + 1;

          const gDate = gameDates.get(gameId);
          if (gDate && (!lastHitDate || gDate > lastHitDate)) {
            lastHitDate = gDate;
          }
        }
      }

      const hitRate = hitCount / sampleSize;
      if (hitRate < config.minHitRate) continue;

      const seasons = Object.keys(perSeason).length;
      if (seasons < config.minSeasons) continue;

      const patternKey = [...combo, "->", outcomeKey].join("|");

      candidates.push({
        patternKey,
        conditions: combo,
        outcome: outcomeKey,
        sampleSize,
        hitCount,
        hitRate,
        seasons,
        perSeason,
        lastHitDate,
        confidenceScore: 0,
        valueScore: 0,
        gameHits,
      });
    }

    if (combosChecked % 500 === 0) {
      process.stdout.write(`  ${combosChecked}/${combos.length} combos checked, ${candidates.length} candidates so far\r`);
    }
  }

  console.log(`\n  Checked ${combosChecked} combos, found ${candidates.length} candidate patterns`);

  // Score all candidates
  for (const c of candidates) {
    const scores = scorePattern({
      hitRate: c.hitRate,
      sampleSize: c.sampleSize,
      seasons: c.seasons,
      totalSeasons,
      maxSample,
      perSeason: c.perSeason,
    });
    c.confidenceScore = scores.confidenceScore;
    c.valueScore = scores.valueScore;
  }

  // Sort by confidence, take top N
  candidates.sort((a, b) => b.confidenceScore - a.confidenceScore || b.valueScore - a.valueScore);
  const topPatterns = candidates.slice(0, config.maxResults);

  console.log(`\nStoring top ${topPatterns.length} patterns...`);

  // Clear existing game patterns
  await prisma.gamePatternHit.deleteMany({});
  await prisma.gamePattern.deleteMany({});

  let stored = 0;
  for (const pattern of topPatterns) {
    const gp = await prisma.gamePattern.create({
      data: {
        patternKey: pattern.patternKey,
        conditions: pattern.conditions,
        outcome: pattern.outcome,
        sampleSize: pattern.sampleSize,
        hitCount: pattern.hitCount,
        hitRate: pattern.hitRate,
        seasons: pattern.seasons,
        perSeason: pattern.perSeason as any,
        lastHitDate: pattern.lastHitDate,
        confidenceScore: pattern.confidenceScore,
        valueScore: pattern.valueScore,
      },
    });

    // Store audit trail
    const hitBatch = pattern.gameHits.map((h) => ({
      patternId: gp.id,
      gameId: h.gameId,
      season: h.season,
      hit: h.hit,
    }));

    if (hitBatch.length > 0) {
      await prisma.gamePatternHit.createMany({
        data: hitBatch,
        skipDuplicates: true,
      });
    }

    stored++;
    if (stored % 100 === 0) {
      console.log(`  Stored ${stored}/${topPatterns.length} patterns`);
    }
  }

  console.log(`\n=== Search complete: ${stored} GamePatterns stored ===`);

  // Print top 10 summary
  console.log("\nTop 10 patterns by confidence:\n");
  for (const p of topPatterns.slice(0, 10)) {
    console.log(`  ${p.conditions.join(" + ")}  ->  ${p.outcome}`);
    console.log(`    Hit rate: ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  ${p.seasons} seasons  |  confidence: ${p.confidenceScore.toFixed(3)}  |  value: ${p.valueScore.toFixed(3)}`);
  }
}
