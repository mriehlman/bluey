import { prisma } from "../packages/db/src";

type SnapshotPattern = {
  id?: string;
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

function familyFromOutcomeType(outcomeType: string): "TOTAL" | "PLAYER" | "SPREAD" | "MONEYLINE" | "OTHER" {
  const base = outcomeType.split(":")[0] ?? outcomeType;
  if (base.startsWith("TOTAL_") || base.includes("OVER") || base.includes("UNDER")) return "TOTAL";
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "PLAYER";
  if (base.includes("COVER") || base.includes("SPREAD")) return "SPREAD";
  if (base.includes("WIN") || base.includes("MONEYLINE")) return "MONEYLINE";
  return "OTHER";
}

function patternKey(p: SnapshotPattern): string {
  return `${p.outcomeType}::${(p.conditions ?? []).join("&&")}`;
}

function asNum(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const baseModelName = flags["base-model"] ?? "v1.5-hybrid-totals-plus-game-2026-03-28";
  const playerSourceModelName = flags["player-source-model"] ?? "v1.2-two-season-2026-03-22";
  const name = flags.name ?? `v1.6-hybrid-with-player-strict-${new Date().toISOString().slice(0, 10)}`;
  const activate = (flags.activate ?? "true") !== "false";
  const minPlayerPosterior = Number(flags["min-player-posterior"] ?? "0.57");
  const minPlayerSamples = Number(flags["min-player-samples"] ?? "120");
  const minPlayerScore = Number(flags["min-player-score"] ?? "0.15");

  const [baseModel, playerSourceModel] = await Promise.all([
    prisma.modelVersion.findUnique({ where: { name: baseModelName } }),
    prisma.modelVersion.findUnique({ where: { name: playerSourceModelName } }),
  ]);

  if (!baseModel) throw new Error(`Base model not found: ${baseModelName}`);
  if (!playerSourceModel) throw new Error(`Player source model not found: ${playerSourceModelName}`);

  const basePatterns = (baseModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];
  const sourcePatterns = (playerSourceModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];

  const playerCandidates = sourcePatterns.filter((p) => familyFromOutcomeType(p.outcomeType) === "PLAYER");
  const strictPlayer = playerCandidates.filter((p) => {
    const posterior = asNum(p.posteriorHitRate) ?? 0;
    const n = asNum(p.n) ?? 0;
    const score = asNum(p.score) ?? 0;
    return posterior >= minPlayerPosterior && n >= minPlayerSamples && score >= minPlayerScore;
  });

  const merged: SnapshotPattern[] = [];
  const seen = new Set<string>();
  for (const p of basePatterns) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }
  for (const p of strictPlayer) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }

  const baseBins = (baseModel.featureBins as Record<string, unknown>) ?? {};
  const sourceBins = (playerSourceModel.featureBins as Record<string, unknown>) ?? {};
  const mergedBins: Record<string, unknown> = { ...sourceBins, ...baseBins };

  const familyCounts = new Map<string, number>();
  for (const p of merged) {
    const f = familyFromOutcomeType(p.outcomeType);
    familyCounts.set(f, (familyCounts.get(f) ?? 0) + 1);
  }

  if (strictPlayer.length === 0) {
    throw new Error(
      `No strict player patterns passed filters (posterior>=${minPlayerPosterior}, n>=${minPlayerSamples}, score>=${minPlayerScore}).`,
    );
  }

  const created = await prisma.modelVersion.create({
    data: {
      name,
      description: `Hybrid model: ${baseModelName} + strict PLAYER subset from ${playerSourceModelName}`,
      isActive: false,
      deployedPatterns: merged as unknown as object,
      featureBins: mergedBins as unknown as object,
      metaModel: baseModel.metaModel,
      tuningConfig: baseModel.tuningConfig,
      stats: {
        patternCount: merged.length,
        featureCount: Object.keys(mergedBins).length,
        families: Object.fromEntries(familyCounts.entries()),
        mergedFrom: {
          baseModel: baseModelName,
          playerSourceModel: playerSourceModelName,
        },
        playerStrictFilter: {
          minPlayerPosterior,
          minPlayerSamples,
          minPlayerScore,
        },
        counts: {
          basePatterns: basePatterns.length,
          playerCandidates: playerCandidates.length,
          strictPlayerAdded: strictPlayer.length,
          mergedPatterns: merged.length,
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
        baseModel: baseModelName,
        playerSourceModel: playerSourceModelName,
        strictPlayerFilter: {
          minPlayerPosterior,
          minPlayerSamples,
          minPlayerScore,
        },
        counts: {
          basePatterns: basePatterns.length,
          playerCandidates: playerCandidates.length,
          strictPlayerAdded: strictPlayer.length,
          mergedPatternCount: merged.length,
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
