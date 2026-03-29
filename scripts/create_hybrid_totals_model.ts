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

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const totalsModelName = flags["totals-model"] ?? "v1.4-totals-strict-2026-03-28";
  const secondaryModelName = flags["secondary-model"] ?? "v1.2-two-season-2026-03-22";
  const name = flags.name ?? `v1.5-hybrid-totals-plus-game-${new Date().toISOString().slice(0, 10)}`;
  const activate = (flags.activate ?? "true") !== "false";

  const [totalsModel, secondaryModel] = await Promise.all([
    prisma.modelVersion.findUnique({ where: { name: totalsModelName } }),
    prisma.modelVersion.findUnique({ where: { name: secondaryModelName } }),
  ]);

  if (!totalsModel) throw new Error(`Totals model not found: ${totalsModelName}`);
  if (!secondaryModel) throw new Error(`Secondary model not found: ${secondaryModelName}`);

  const totalsPatterns = (totalsModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];
  const secondaryPatterns = (secondaryModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];

  const secondaryGamePatterns = secondaryPatterns.filter((p) => {
    const family = familyFromOutcomeType(p.outcomeType);
    return family !== "TOTAL" && family !== "PLAYER";
  });

  const merged: SnapshotPattern[] = [];
  const seen = new Set<string>();
  for (const p of totalsPatterns) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }
  for (const p of secondaryGamePatterns) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }

  const totalsBins = (totalsModel.featureBins as Record<string, unknown>) ?? {};
  const secondaryBins = (secondaryModel.featureBins as Record<string, unknown>) ?? {};
  const mergedBins: Record<string, unknown> = { ...secondaryBins, ...totalsBins };

  const familyCounts = new Map<string, number>();
  for (const p of merged) {
    const f = familyFromOutcomeType(p.outcomeType);
    familyCounts.set(f, (familyCounts.get(f) ?? 0) + 1);
  }

  const created = await prisma.modelVersion.create({
    data: {
      name,
      description: `Hybrid model: ${totalsModelName} totals + non-player game patterns from ${secondaryModelName}`,
      isActive: false,
      deployedPatterns: merged as unknown as object,
      featureBins: mergedBins as unknown as object,
      metaModel: totalsModel.metaModel,
      tuningConfig: totalsModel.tuningConfig,
      stats: {
        patternCount: merged.length,
        featureCount: Object.keys(mergedBins).length,
        families: Object.fromEntries(familyCounts.entries()),
        mergedFrom: {
          totalsModel: totalsModelName,
          secondaryModel: secondaryModelName,
        },
        counts: {
          totalsPatterns: totalsPatterns.length,
          secondaryPatterns: secondaryPatterns.length,
          secondaryGamePatterns: secondaryGamePatterns.length,
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
        totalsModel: totalsModelName,
        secondaryModel: secondaryModelName,
        totalsPatternCount: totalsPatterns.length,
        secondaryGamePatternCount: secondaryGamePatterns.length,
        mergedPatternCount: merged.length,
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
