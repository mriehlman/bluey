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

function asNum(v: unknown): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

function patternKey(p: SnapshotPattern): string {
  return `${p.outcomeType}::${(p.conditions ?? []).join("&&")}`;
}

function familyFromOutcomeType(outcomeType: string): "TOTAL" | "PLAYER" | "SPREAD" | "MONEYLINE" | "OTHER" {
  const base = (outcomeType.split(":")[0] ?? outcomeType).toUpperCase();
  if (base.startsWith("TOTAL_") || base.includes("OVER") || base.includes("UNDER")) return "TOTAL";
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "PLAYER";
  if (base.includes("COVER") || base.includes("SPREAD")) return "SPREAD";
  if (base.includes("WIN") || base.includes("MONEYLINE")) return "MONEYLINE";
  return "OTHER";
}

function isReboundOutcome(outcomeType: string): boolean {
  const base = (outcomeType.split(":")[0] ?? outcomeType).toUpperCase();
  return base.includes("REBOUND") || base.includes("REBOUNDER");
}

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const baseModelName = flags["base-model"] ?? "v1.7-player-points-filtered-2026-03-28";
  const name = flags.name ?? `v1.8-hybrid-with-rebounds-${new Date().toISOString().slice(0, 10)}`;
  const activate = (flags.activate ?? "true") !== "false";

  const minPosterior = Number(flags["min-rebound-posterior"] ?? "0.89");
  const minScore = Number(flags["min-rebound-score"] ?? "1.20");
  const minEdge = Number(flags["min-rebound-edge"] ?? "0.003");
  const minN = Number(flags["min-rebound-n"] ?? "500");
  const maxAdd = Number(flags["max-rebound-add"] ?? "40");

  const baseModel = await prisma.modelVersion.findUnique({ where: { name: baseModelName } });
  if (!baseModel) throw new Error(`Base model not found: ${baseModelName}`);

  const basePatterns = (baseModel.deployedPatterns as unknown as SnapshotPattern[]) ?? [];

  const reboundRows = await prisma.patternV2.findMany({
    where: {
      status: "deployed",
      OR: [{ outcomeType: { contains: "REBOUND" } }, { outcomeType: { contains: "REBOUNDER" } }],
    },
    select: {
      outcomeType: true,
      conditions: true,
      posteriorHitRate: true,
      edge: true,
      score: true,
      n: true,
    },
    orderBy: [{ score: "desc" }],
  });

  const reboundCandidates = (reboundRows as unknown as SnapshotPattern[]).filter((p) => {
    if (!isReboundOutcome(p.outcomeType)) return false;
    return (
      asNum(p.posteriorHitRate) >= minPosterior &&
      asNum(p.score) >= minScore &&
      asNum(p.edge) >= minEdge &&
      asNum(p.n) >= minN
    );
  });

  const reboundSelected = reboundCandidates.slice(0, Math.max(1, maxAdd));

  const merged: SnapshotPattern[] = [];
  const seen = new Set<string>();
  for (const p of basePatterns) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }
  for (const p of reboundSelected) {
    const key = patternKey(p);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(p);
  }

  const featureBins = (baseModel.featureBins as Record<string, unknown>) ?? {};
  const familyCounts = new Map<string, number>();
  for (const p of merged) {
    const f = familyFromOutcomeType(p.outcomeType);
    familyCounts.set(f, (familyCounts.get(f) ?? 0) + 1);
  }

  const created = await prisma.modelVersion.create({
    data: {
      name,
      description: `Hybrid model: ${baseModelName} + strict rebounds subset from deployed PatternV2`,
      isActive: false,
      deployedPatterns: merged as unknown as object,
      featureBins: featureBins as unknown as object,
      metaModel: baseModel.metaModel,
      tuningConfig: baseModel.tuningConfig,
      stats: {
        patternCount: merged.length,
        featureCount: Object.keys(featureBins).length,
        families: Object.fromEntries(familyCounts.entries()),
        reboundsFilter: {
          minPosterior,
          minScore,
          minEdge,
          minN,
          maxAdd,
          candidates: reboundRows.length,
          selected: reboundSelected.length,
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
        reboundsFilter: {
          minPosterior,
          minScore,
          minEdge,
          minN,
          maxAdd,
          candidates: reboundRows.length,
          selected: reboundSelected.length,
        },
        counts: {
          before: basePatterns.length,
          after: merged.length,
          added: merged.length - basePatterns.length,
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
