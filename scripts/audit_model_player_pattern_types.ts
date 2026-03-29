import { prisma } from "../packages/db/src";

type P = { outcomeType: string };

function classify(outcomeType: string): string {
  const base = (outcomeType.split(":")[0] ?? outcomeType).toUpperCase();
  if (base.includes("REBOUND")) return "player_rebounds";
  if (base.includes("ASSIST") || base.includes("PLAYMAKER")) return "player_assists";
  if (base.includes("SCORER") || base.includes("30_PLUS") || base.includes("40_PLUS") || base.includes("POINT")) {
    return "player_points";
  }
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "other_player";
  return "non_player";
}

async function main() {
  const model = process.argv[2] ?? "v1.2-two-season-2026-03-22";
  const row = await prisma.modelVersion.findUnique({
    where: { name: model },
    select: { deployedPatterns: true },
  });
  if (!row) throw new Error(`Model not found: ${model}`);
  const patterns = (row.deployedPatterns as unknown as P[]) ?? [];

  const counts = new Map<string, number>();
  for (const p of patterns) {
    const t = classify(p.outcomeType);
    counts.set(t, (counts.get(t) ?? 0) + 1);
  }

  console.log(
    JSON.stringify(
      {
        model,
        totalPatterns: patterns.length,
        counts: Object.fromEntries([...counts.entries()].sort((a, b) => b[1] - a[1])),
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
