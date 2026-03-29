import { prisma } from "../packages/db/src";
import { parsePlayerOutcomeRequirement } from "../packages/core/src/features/predictionEngine";

type PatternRow = { outcomeType: string };

function isAssistsOutcome(outcomeType: string): boolean {
  const base = (outcomeType.split(":")[0] ?? outcomeType).toUpperCase();
  return base.includes("ASSIST") || base.includes("PLAYMAKER");
}

async function main() {
  const model = process.argv[2] ?? "v1.8-hybrid-with-rebounds-2026-03-29";
  const row = await prisma.modelVersion.findUnique({
    where: { name: model },
    select: { deployedPatterns: true },
  });
  if (!row) throw new Error(`Model not found: ${model}`);

  const patterns = (row.deployedPatterns as unknown as PatternRow[]) ?? [];
  const assistsOutcomes = [...new Set(patterns.map((p) => p.outcomeType).filter(isAssistsOutcome))].sort();
  const mapped = assistsOutcomes.filter((o) => parsePlayerOutcomeRequirement(o) != null);
  const unmapped = assistsOutcomes.filter((o) => parsePlayerOutcomeRequirement(o) == null);

  console.log(
    JSON.stringify(
      {
        model,
        assistsOutcomeCount: assistsOutcomes.length,
        mappedForPlayerPropMarket: mapped,
        unmappedForPlayerPropMarket: unmapped,
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
