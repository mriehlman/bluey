import { prisma } from "../packages/db/src";

type SnapshotPattern = {
  outcomeType: string;
  conditions: string[];
  posteriorHitRate?: number;
  edge?: number;
  score?: number;
  n?: number;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const key = args[i];
    if (!key?.startsWith("--")) continue;
    const next = args[i + 1];
    if (next && !next.startsWith("--")) {
      flags[key.slice(2)] = next;
      i++;
    } else {
      flags[key.slice(2)] = "true";
    }
  }
  return flags;
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function outcomeBase(outcomeType: string): string {
  return outcomeType.split(":")[0] ?? outcomeType;
}

function isPlayerPointsOutcomeType(outcomeType: string): boolean {
  const base = outcomeBase(outcomeType);
  return (
    base === "PLAYER_30_PLUS" ||
    base === "PLAYER_40_PLUS" ||
    base.includes("SCORER")
  );
}

function familyFromOutcomeType(outcomeType: string): "TOTAL" | "PLAYER" | "SPREAD" | "MONEYLINE" | "OTHER" {
  const base = outcomeBase(outcomeType);
  if (base.startsWith("TOTAL_") || base.includes("OVER") || base.includes("UNDER")) return "TOTAL";
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "PLAYER";
  if (base.includes("COVER") || base.includes("SPREAD")) return "SPREAD";
  if (base.includes("WIN") || base.includes("MONEYLINE")) return "MONEYLINE";
  return "OTHER";
}

function patternKey(p: SnapshotPattern): string {
  return `${p.outcomeType}::${(p.conditions ?? []).join("&&")}`;
}

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const sourceModelName = flags["source-model"] ?? "v1.6-hybrid-with-player-strict-2026-03-28";
  const name = flags.name ?? `v1.7-player-points-filtered-${new Date().toISOString().slice(0, 10)}`;
  const activate = (flags.activate ?? "true") !== "false";
  const from = flags.from ?? "2025-10-01";
  const to = flags.to ?? "2026-03-28";
  const minPlayerPointsHitRate = Number(flags["min-player-points-hit-rate"] ?? "0.5");
  const minPlayerPointsGradedRows = Number(flags["min-player-points-graded"] ?? "50");

  const sourceModel = await prisma.modelVersion.findUnique({
    where: { name: sourceModelName },
  });
  if (!sourceModel) throw new Error(`Source model not found: ${sourceModelName}`);

  const sourcePatterns = (sourceModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];

  const pointsRows = await prisma.$queryRawUnsafe<
    Array<{ outcomeType: string; gradedRows: number; hitRate: number | null }>
  >(
    `SELECT
      l."outcomeType" AS "outcomeType",
      SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::int AS "gradedRows",
      CASE
        WHEN SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END) > 0
        THEN SUM(CASE WHEN l."settledHit" = TRUE THEN 1 ELSE 0 END)::float8
          / SUM(CASE WHEN l."settledHit" IS NOT NULL THEN 1 ELSE 0 END)::float8
        ELSE NULL
      END AS "hitRate"
    FROM "SuggestedPlayLedger" l
    WHERE COALESCE(l."modelVersionName",'live') = '${sqlEsc(sourceModelName)}'
      AND COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) = 'legacy'
      AND l."date" >= '${sqlEsc(from)}'::date
      AND l."date" <= '${sqlEsc(to)}'::date
      AND COALESCE(NULLIF(l."laneTag", ''), 'other') = 'player_points'
    GROUP BY 1`,
  );

  const allowedPlayerPointsOutcomeTypes = new Set(
    pointsRows
      .filter((r) => (r.hitRate ?? -1) >= minPlayerPointsHitRate && r.gradedRows >= minPlayerPointsGradedRows)
      .map((r) => r.outcomeType),
  );

  const filteredPatterns = sourcePatterns.filter((p) => {
    if (!isPlayerPointsOutcomeType(p.outcomeType)) return true;
    return allowedPlayerPointsOutcomeTypes.has(p.outcomeType);
  });

  const deduped: SnapshotPattern[] = [];
  const seen = new Set<string>();
  for (const p of filteredPatterns) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(p);
  }

  const featureBins = (sourceModel.featureBins as Record<string, unknown>) ?? {};
  const familyCounts = new Map<string, number>();
  for (const p of deduped) {
    const f = familyFromOutcomeType(p.outcomeType);
    familyCounts.set(f, (familyCounts.get(f) ?? 0) + 1);
  }

  const created = await prisma.modelVersion.create({
    data: {
      name,
      description: `Filtered from ${sourceModelName}: remove weak player_points outcomes`,
      isActive: false,
      deployedPatterns: deduped as unknown as object,
      featureBins: featureBins as unknown as object,
      metaModel: sourceModel.metaModel,
      tuningConfig: sourceModel.tuningConfig,
      stats: {
        patternCount: deduped.length,
        featureCount: Object.keys(featureBins).length,
        families: Object.fromEntries(familyCounts.entries()),
        playerPointsFilter: {
          sourceModel: sourceModelName,
          from,
          to,
          minPlayerPointsHitRate,
          minPlayerPointsGradedRows,
          evaluatedOutcomeTypes: pointsRows,
          allowedOutcomeTypes: [...allowedPlayerPointsOutcomeTypes],
        },
      } as unknown as object,
    },
  });

  if (activate) {
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
    await prisma.modelVersion.update({
      where: { id: created.id },
      data: { isActive: true },
    });
  }

  console.log(
    JSON.stringify(
      {
        created: created.name,
        isActive: activate,
        sourceModel: sourceModelName,
        filter: {
          from,
          to,
          minPlayerPointsHitRate,
          minPlayerPointsGradedRows,
        },
        pointsOutcomeEval: pointsRows,
        allowedPlayerPointsOutcomeTypes: [...allowedPlayerPointsOutcomeTypes],
        patternCounts: {
          before: sourcePatterns.length,
          after: deduped.length,
          removed: sourcePatterns.length - deduped.length,
        },
        familyCounts: Object.fromEntries(familyCounts.entries()),
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
