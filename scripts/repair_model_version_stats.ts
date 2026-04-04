import { prisma } from "../packages/db/src";

type SnapshotPattern = { outcomeType: string };

function familyFromOutcomeType(outcomeType: string): string {
  const base = outcomeType.split(":")[0] ?? outcomeType;
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "PLAYER";
  if (base.includes("COVER") || base.includes("SPREAD")) return "SPREAD";
  if (base.includes("WIN") || base.includes("MONEYLINE")) return "MONEYLINE";
  if (base.startsWith("TOTAL_") || base.includes("OVER") || base.includes("UNDER")) return "TOTAL";
  return "OTHER";
}

async function main() {
  const name = process.argv[2];
  if (!name) throw new Error("Usage: bun scripts/repair_model_version_stats.ts <model-name>");

  const model = await prisma.modelVersion.findUnique({
    where: { name },
    select: { id: true, name: true, deployedPatterns: true, featureBins: true },
  });
  if (!model) throw new Error(`Model not found: ${name}`);

  const patterns = (model.deployedPatterns as unknown as SnapshotPattern[]) ?? [];
  const featureBins = (model.featureBins as Record<string, unknown>) ?? {};
  const families = new Map<string, number>();
  for (const p of patterns) {
    const f = familyFromOutcomeType(p.outcomeType);
    families.set(f, (families.get(f) ?? 0) + 1);
  }

  const stats = {
    patternCount: patterns.length,
    featureCount: Object.keys(featureBins).length,
    families: Object.fromEntries(families.entries()),
  };

  await prisma.modelVersion.update({
    where: { id: model.id },
    data: { stats: stats as unknown as object },
  });

  console.log(JSON.stringify({ name: model.name, stats }, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
