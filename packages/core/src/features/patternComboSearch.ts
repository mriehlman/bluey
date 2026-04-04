import { prisma } from "@bluey/db";

type PatternSummary = {
  id: string;
  outcomeType: string;
  score: number;
  posteriorHitRate: number;
  edge: number;
  n: number;
};

type PickRow = {
  date: string;
  pickId: string;
  hit: boolean;
};

type ComboResult = {
  patternIds: string[];
  comboSize: number;
  qualifiedDays: number;
  perfectDays: number;
  perfectDayRate: number | null;
  totalLegs: number;
  totalHits: number;
  legHitRate: number | null;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--") continue;
    if (arg.startsWith("--") && i + 1 < args.length) {
      const value = args[i + 1];
      if (!value.startsWith("--")) {
        flags[arg.slice(2)] = value;
        i++;
      }
    }
  }
  return flags;
}

function combinations<T>(items: T[], k: number): T[][] {
  const out: T[][] = [];
  const n = items.length;
  if (k <= 0 || k > n) return out;
  const idx = Array.from({ length: k }, (_, i) => i);

  while (true) {
    out.push(idx.map((i) => items[i]!));
    let i = k - 1;
    while (i >= 0 && idx[i] === n - k + i) i--;
    if (i < 0) break;
    idx[i] += 1;
    for (let j = i + 1; j < k; j++) {
      idx[j] = idx[j - 1]! + 1;
    }
  }
  return out;
}

function nChooseK(n: number, k: number): bigint {
  if (k < 0 || k > n) return 0n;
  const kk = Math.min(k, n - k);
  let num = 1n;
  let den = 1n;
  for (let i = 1; i <= kk; i++) {
    num *= BigInt(n - kk + i);
    den *= BigInt(i);
  }
  return num / den;
}

function isPlayerOutcome(outcomeType: string): boolean {
  const base = outcomeType.replace(/:.*$/, "");
  return base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_");
}

function comboMetric(patternPickRows: Map<string, PickRow[]>, patternIds: string[], minLegs: number): ComboResult {
  const mergedByDay = new Map<string, Map<string, boolean>>();
  for (const patternId of patternIds) {
    const rows = patternPickRows.get(patternId) ?? [];
    for (const row of rows) {
      const day = mergedByDay.get(row.date) ?? new Map<string, boolean>();
      // Dedup a pick if multiple selected patterns supported it.
      day.set(row.pickId, row.hit);
      mergedByDay.set(row.date, day);
    }
  }

  let qualifiedDays = 0;
  let perfectDays = 0;
  let totalLegs = 0;
  let totalHits = 0;
  for (const pickMap of mergedByDay.values()) {
    const legs = pickMap.size;
    if (legs < minLegs) continue;
    let hits = 0;
    for (const h of pickMap.values()) if (h) hits += 1;
    qualifiedDays += 1;
    totalLegs += legs;
    totalHits += hits;
    if (hits === legs) perfectDays += 1;
  }

  return {
    patternIds: [...patternIds],
    comboSize: patternIds.length,
    qualifiedDays,
    perfectDays,
    perfectDayRate: qualifiedDays > 0 ? perfectDays / qualifiedDays : null,
    totalLegs,
    totalHits,
    legHitRate: totalLegs > 0 ? totalHits / totalLegs : null,
  };
}

export async function searchPatternCombos(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const season = Number(flags.season ?? 2025);
  const minLegs = Math.max(1, Number(flags["min-legs"] ?? 8));
  const topPatterns = Math.max(2, Number(flags["top-patterns"] ?? 18));
  const maxComboSize = Math.max(1, Number(flags["max-combo-size"] ?? 3));
  const minPatternN = Math.max(1, Number(flags["min-pattern-n"] ?? 80));
  const topResults = Math.max(1, Number(flags["top-results"] ?? 20));
  const requireQualifiedDays = Math.max(0, Number(flags["min-qualified-days"] ?? 10));
  const maxPerOutcomeTypeRaw = Number(flags["max-per-outcome-type"] ?? 0);
  const maxPerOutcomeType = maxPerOutcomeTypeRaw > 0 ? Math.floor(maxPerOutcomeTypeRaw) : Number.POSITIVE_INFINITY;
  const modelVersion = flags["model-version"] ?? "live"; // live | all | <snapshot>
  const exhaustive = (flags.exhaustive ?? "false") === "true";
  const maxCombos = Math.max(1, Number(flags["max-combos"] ?? 500000));
  const json = (flags.json ?? "false") === "true";

  console.log("\n=== Pattern Combo Search ===\n");
  console.log(
    `season=${season}, minLegs=${minLegs}, topPatterns=${topPatterns}, maxComboSize=${maxComboSize}, minPatternN=${minPatternN}, minQualifiedDays=${requireQualifiedDays}, maxPerOutcomeType=${Number.isFinite(maxPerOutcomeType) ? maxPerOutcomeType : "inf"}, modelVersion=${modelVersion}, exhaustive=${exhaustive}, maxCombos=${maxCombos}\n`,
  );

  const modelVersionFilter =
    modelVersion === "all"
      ? ""
      : modelVersion === "live"
        ? `AND COALESCE(cp."runContext"->>'modelVersionName', 'live') = 'live'`
        : `AND cp."runContext"->>'modelVersionName' = '${modelVersion.replaceAll("'", "''")}'`;

  let patterns: PatternSummary[] = [];
  let eligiblePatternCount = 0;
  if (modelVersion !== "all" && modelVersion !== "live") {
    const snapshot = await prisma.modelVersion.findUnique({
      where: { name: modelVersion },
      select: { deployedPatterns: true },
    });
    if (!snapshot) {
      console.log(`Model version "${modelVersion}" not found.`);
      return;
    }
    const deployed = (snapshot.deployedPatterns as unknown[]) ?? [];
    const snapshotPatterns = deployed
      .map((raw) => {
        const p = raw as Record<string, unknown>;
        return {
          id: String(p.id ?? ""),
          outcomeType: String(p.outcomeType ?? ""),
          score: Number(p.score ?? 0),
          posteriorHitRate: Number(p.posteriorHitRate ?? 0),
          edge: Number(p.edge ?? 0),
          n: Number(p.n ?? 0),
        } satisfies PatternSummary;
      })
      .filter((p) => p.id && p.outcomeType && Number.isFinite(p.n) && p.n >= minPatternN)
      .sort((a, b) => (b.score - a.score) || (b.n - a.n));
    eligiblePatternCount = snapshotPatterns.length;
    patterns = exhaustive ? snapshotPatterns : snapshotPatterns.slice(0, topPatterns);
  } else if (modelVersion === "live") {
    // In live mode, source candidate patterns from season rows actually produced by live runs.
    // This avoids mismatch between current deployed PatternV2 and historical live canonical rows.
    const observed = await prisma.$queryRawUnsafe<Array<{ patternId: string; uses: number }>>(
      `SELECT
         pid AS "patternId",
         COUNT(*)::int AS "uses"
       FROM (
         SELECT UNNEST(cp."supportingPatterns") AS pid
         FROM "CanonicalPrediction" cp
         JOIN "Game" g ON g."id" = cp."gameId"
         WHERE g."season" = ${season}
           ${modelVersionFilter}
           AND cp."supportingPatterns" IS NOT NULL
       ) s
       GROUP BY pid
       ORDER BY COUNT(*) DESC
       ${exhaustive ? "" : `LIMIT ${Math.max(topPatterns * 5, topPatterns)}`}`,
    );
    const observedIds = observed.map((r) => r.patternId).filter(Boolean);
    if (observedIds.length > 0) {
      const rows = (await prisma.patternV2.findMany({
        where: { id: { in: observedIds } },
        select: {
          id: true,
          outcomeType: true,
          score: true,
          posteriorHitRate: true,
          edge: true,
          n: true,
        },
      })) as PatternSummary[];

      const byId = new Map(rows.map((r) => [r.id, r]));
      const merged: PatternSummary[] = [];
      for (const obs of observed) {
        const p = byId.get(obs.patternId);
        if (!p) {
          merged.push({
            id: obs.patternId,
            outcomeType: "UNKNOWN",
            score: 0,
            posteriorHitRate: 0,
            edge: 0,
            n: obs.uses,
          });
          continue;
        }
        merged.push(p);
      }
      patterns = merged
        .filter((p) => Number.isFinite(p.n) && p.n >= minPatternN);
      eligiblePatternCount = patterns.length;
      if (!exhaustive) patterns = patterns.slice(0, topPatterns);
    }
  } else {
    const all = (await prisma.patternV2.findMany({
      where: { status: "deployed", n: { gte: minPatternN } },
      orderBy: [{ score: "desc" }, { n: "desc" }],
      select: {
        id: true,
        outcomeType: true,
        score: true,
        posteriorHitRate: true,
        edge: true,
        n: true,
      },
    })) as PatternSummary[];
    eligiblePatternCount = all.length;
    patterns = exhaustive ? all : all.slice(0, topPatterns);
  }

  if (patterns.length === 0) {
    console.log("No deployed patterns found for the given constraints.");
    return;
  }

  const patternIds = patterns.map((p) => p.id);
  console.log(
    `pattern pool selected=${patternIds.length}${eligiblePatternCount > 0 ? ` (eligible=${eligiblePatternCount})` : ""}`,
  );
  let estimatedCombos = 0n;
  const maxSize = Math.min(maxComboSize, patternIds.length);
  for (let size = 1; size <= maxSize; size++) {
    estimatedCombos += nChooseK(patternIds.length, size);
  }
  console.log(`estimated combos to evaluate=${estimatedCombos.toString()}`);
  if (estimatedCombos > BigInt(maxCombos)) {
    console.log(
      `Aborting: estimated combos (${estimatedCombos.toString()}) exceed --max-combos=${maxCombos}. Increase --max-combos or reduce pool/size.`,
    );
    return;
  }

  const canonicalRows = await prisma.$queryRawUnsafe<
    Array<{
      date: string;
      gameId: string;
      outcomeType: string;
      supportingPatterns: string[];
      settledHit: boolean;
      targetPlayerName: string | null;
      market: string | null;
      priceAmerican: number | null;
    }>
  >(
    `WITH canonical_latest AS (
       SELECT DISTINCT ON (g."date"::date, cp."gameId", cp."selection")
         g."date"::date AS "gameDate",
         cp."gameId" AS "gameId",
         cp."selection" AS "outcomeType",
         cp."supportingPatterns" AS "supportingPatterns"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE g."season" = ${season}
         ${modelVersionFilter}
         AND cp."supportingPatterns" && ARRAY[${patternIds.map((id) => `'${id.replaceAll("'", "''")}'`).join(",")}]::text[]
       ORDER BY
         g."date"::date,
         cp."gameId",
         cp."selection",
         cp."generatedAt" DESC,
         cp."runStartedAt" DESC NULLS LAST
     )
     SELECT
       TO_CHAR(s."date",'YYYY-MM-DD') AS "date",
       s."gameId" AS "gameId",
       s."outcomeType" AS "outcomeType",
       cl."supportingPatterns" AS "supportingPatterns",
       s."settledHit" AS "settledHit",
       s."targetPlayerName" AS "targetPlayerName",
       s."market" AS "market",
       s."priceAmerican" AS "priceAmerican"
     FROM canonical_latest cl
     JOIN "SuggestedPlayLedger" s
       ON s."date" = cl."gameDate"
      AND s."gameId" = cl."gameId"
      AND s."outcomeType" = cl."outcomeType"
      AND s."isActionable" = TRUE
      AND s."settledHit" IS NOT NULL`,
  );

  if (canonicalRows.length === 0 && modelVersion !== "all") {
    const coverage = await prisma.$queryRawUnsafe<Array<{ modelVersionName: string; rows: number }>>(
      `SELECT
         COALESCE(cp."runContext"->>'modelVersionName', 'live') AS "modelVersionName",
         COUNT(*)::int AS "rows"
       FROM "CanonicalPrediction" cp
       JOIN "Game" g ON g."id" = cp."gameId"
       WHERE g."season" = ${season}
       GROUP BY 1
       ORDER BY 2 DESC`,
    );
    const top = coverage.slice(0, 5);
    if (top.length > 0) {
      console.log(`No canonical rows matched modelVersion='${modelVersion}' for season ${season}.`);
      console.log("Top model versions with canonical coverage for this season:");
      for (const row of top) {
        console.log(`  ${row.modelVersionName}: ${row.rows}`);
      }
      console.log("Tip: rerun with --model-version all or one of the versions above.\n");
    }
  }

  const patternIdSet = new Set(patternIds);
  const patternPickRows = new Map<string, PickRow[]>();
  const patternOutcomeCounts = new Map<string, Map<string, number>>();
  for (const pid of patternIds) patternPickRows.set(pid, []);
  let excludedNonConcreteMarket = 0;
  let excludedMissingPlayerTarget = 0;
  let retainedConcreteRows = 0;
  for (const row of canonicalRows) {
    const concreteMarket = row.market != null && row.priceAmerican != null;
    const playerOutcome = isPlayerOutcome(row.outcomeType);
    const concretePlayer = playerOutcome ? row.targetPlayerName != null : true;
    if (!concreteMarket) {
      excludedNonConcreteMarket += 1;
      continue;
    }
    if (!concretePlayer) {
      excludedMissingPlayerTarget += 1;
      continue;
    }
    retainedConcreteRows += 1;

    const pickId = `${row.date}|${row.gameId}|${row.outcomeType}`;
    const supporting = row.supportingPatterns ?? [];
    for (const pid of supporting) {
      if (!patternIdSet.has(pid)) continue;
      const arr = patternPickRows.get(pid) ?? [];
      arr.push({
        date: row.date,
        pickId,
        hit: Boolean(row.settledHit),
      });
      patternPickRows.set(pid, arr);
      const outcomeCounts = patternOutcomeCounts.get(pid) ?? new Map<string, number>();
      outcomeCounts.set(row.outcomeType, (outcomeCounts.get(row.outcomeType) ?? 0) + 1);
      patternOutcomeCounts.set(pid, outcomeCounts);
    }
  }
  console.log(
    `candidate rows=${canonicalRows.length}, retained concrete rows=${retainedConcreteRows}, excluded(non-concrete market)=${excludedNonConcreteMarket}, excluded(player target missing)=${excludedMissingPlayerTarget}`,
  );

  const outcomeKeyByPattern = new Map<string, string>();
  for (const p of patterns) {
    let outcomeKey = p.outcomeType;
    if (outcomeKey === "UNKNOWN") {
      const counts = patternOutcomeCounts.get(p.id);
      if (counts && counts.size > 0) {
        const top = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];
        if (top) outcomeKey = top[0];
      }
    }
    outcomeKeyByPattern.set(p.id, outcomeKey || "UNKNOWN");
  }

  const allResults: ComboResult[] = [];
  for (let size = 1; size <= Math.min(maxComboSize, patterns.length); size++) {
    const combos = combinations(patternIds, size);
    for (const combo of combos) {
      if (Number.isFinite(maxPerOutcomeType)) {
        const byOutcome = new Map<string, number>();
        let violates = false;
        for (const id of combo) {
          const key = outcomeKeyByPattern.get(id) ?? "UNKNOWN";
          const next = (byOutcome.get(key) ?? 0) + 1;
          if (next > maxPerOutcomeType) {
            violates = true;
            break;
          }
          byOutcome.set(key, next);
        }
        if (violates) continue;
      }
      const res = comboMetric(patternPickRows, combo, minLegs);
      if (res.qualifiedDays < requireQualifiedDays) continue;
      allResults.push(res);
    }
    console.log(`Evaluated size=${size} combos: ${combos.length}`);
  }

  if (allResults.length === 0) {
    console.log("No combinations satisfied constraints.");
    return;
  }

  allResults.sort((a, b) => {
    if (b.perfectDays !== a.perfectDays) return b.perfectDays - a.perfectDays;
    const aRate = a.perfectDayRate ?? -1;
    const bRate = b.perfectDayRate ?? -1;
    if (bRate !== aRate) return bRate - aRate;
    const aLegRate = a.legHitRate ?? -1;
    const bLegRate = b.legHitRate ?? -1;
    if (bLegRate !== aLegRate) return bLegRate - aLegRate;
    return b.totalLegs - a.totalLegs;
  });

  const top = allResults.slice(0, topResults);
  const patternById = new Map(patterns.map((p) => [p.id, p]));

  const printable = top.map((r, i) => ({
    rank: i + 1,
    comboSize: r.comboSize,
    perfectDays: r.perfectDays,
    qualifiedDays: r.qualifiedDays,
    perfectRate: r.perfectDayRate == null ? "n/a" : `${(r.perfectDayRate * 100).toFixed(1)}%`,
    legHitRate: r.legHitRate == null ? "n/a" : `${(r.legHitRate * 100).toFixed(1)}%`,
    totalLegs: r.totalLegs,
    patterns: r.patternIds
      .map((id) => {
        const p = patternById.get(id);
        if (!p) return id;
        const shortId = id.slice(0, 8);
        let outcomeType = p.outcomeType;
        if (outcomeType === "UNKNOWN") {
          const counts = patternOutcomeCounts.get(id);
          if (counts && counts.size > 0) {
            const top = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];
            if (top) outcomeType = top[0];
          }
        }
        return `${outcomeType}#${shortId}`;
      })
      .join(" | "),
  }));

  console.table(printable);

  if (json) {
    console.log(
      JSON.stringify(
        {
          season,
          minLegs,
          modelVersion,
          topPatterns,
          exhaustive,
          maxCombos,
          maxComboSize,
          minPatternN,
          minQualifiedDays: requireQualifiedDays,
          maxPerOutcomeType: Number.isFinite(maxPerOutcomeType) ? maxPerOutcomeType : null,
          patternPoolSelected: patternIds.length,
          patternPoolEligible: eligiblePatternCount || null,
          estimatedCombos: estimatedCombos.toString(),
          results: top.map((r) => ({
            ...r,
            patterns: r.patternIds.map((id) => ({
              id,
              outcomeType: patternById.get(id)?.outcomeType ?? id,
            })),
          })),
        },
        null,
        2,
      ),
    );
  }
}
