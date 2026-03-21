import { prisma } from "@bluey/db";
import { loadMetaModel, type MetaModel } from "./metaModelCore";
import { PREDICTION_TUNING } from "../config/tuning";
import type { FeatureBinDef } from "../features/v2PregameMatching";
import type { DeployedPatternV2 } from "../features/v2PregameMatching";

export type ModelVersionSnapshot = {
  deployedPatterns: DeployedPatternV2[];
  featureBins: Record<string, FeatureBinDef>;
  metaModel: MetaModel;
  tuningConfig: typeof PREDICTION_TUNING;
};

export type ModelVersionRow = {
  id: string;
  name: string;
  description: string | null;
  isActive: boolean;
  stats: {
    patternCount: number;
    featureCount: number;
    families: Record<string, number>;
  } | null;
  createdAt: Date;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = "true";
      }
    }
  }
  return flags;
}

async function collectCurrentSnapshot(): Promise<ModelVersionSnapshot> {
  const patterns = await prisma.patternV2.findMany({
    where: { status: "deployed" },
    select: {
      id: true,
      outcomeType: true,
      conditions: true,
      posteriorHitRate: true,
      edge: true,
      score: true,
      n: true,
    },
    orderBy: { score: "desc" },
  }) as DeployedPatternV2[];

  if (patterns.length === 0) {
    throw new Error("No deployed patterns found. Run discovery + validation first.");
  }

  const binRows = await prisma.$queryRawUnsafe<
    Array<{ featureName: string; binEdges: unknown }>
  >(
    `SELECT DISTINCT ON ("featureName") "featureName", "binEdges"
     FROM "FeatureBin"
     ORDER BY "featureName", "createdAt" DESC`,
  );
  const featureBins: Record<string, FeatureBinDef> = {};
  for (const row of binRows) {
    const parsed = row.binEdges as FeatureBinDef;
    if (parsed && Array.isArray(parsed.labels) && Array.isArray(parsed.edges)) {
      featureBins[row.featureName] = parsed;
    }
  }
  if (Object.keys(featureBins).length === 0) {
    throw new Error("No feature bins found. Run build:feature-bins first.");
  }

  const metaModel = await loadMetaModel();
  if (!metaModel) {
    throw new Error("No meta model found. Run train:meta-model first.");
  }

  return {
    deployedPatterns: patterns,
    featureBins,
    metaModel,
    tuningConfig: structuredClone(PREDICTION_TUNING) as typeof PREDICTION_TUNING,
  };
}

function computeSnapshotStats(snapshot: ModelVersionSnapshot) {
  const families: Record<string, number> = {};
  for (const p of snapshot.deployedPatterns) {
    const base = p.outcomeType.replace(/:.*$/, "");
    let fam = "OTHER";
    if (base.startsWith("PLAYER_") || base.includes("TOP_")) fam = "PLAYER";
    else if (base.endsWith("_WIN")) fam = "MONEYLINE";
    else if (base.includes("COVERED")) fam = "SPREAD";
    else if (base.startsWith("TOTAL_") || base.startsWith("OVER_") || base.startsWith("UNDER_")) fam = "TOTAL";
    families[fam] = (families[fam] ?? 0) + 1;
  }
  return {
    patternCount: snapshot.deployedPatterns.length,
    featureCount: Object.keys(snapshot.featureBins).length,
    families,
  };
}

export async function snapshotModelVersion(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const name = flags.name;
  if (!name) {
    throw new Error("--name is required. Example: --name v1.0-baseline");
  }
  const description = flags.description ?? null;
  const activate = flags.activate !== "false";

  console.log(`\n=== Snapshot Model Version: ${name} ===\n`);

  const existing = await prisma.modelVersion.findUnique({ where: { name } });
  if (existing) {
    throw new Error(`Model version "${name}" already exists. Choose a different name.`);
  }

  const snapshot = await collectCurrentSnapshot();
  const stats = computeSnapshotStats(snapshot);

  console.log(`Patterns: ${stats.patternCount}`);
  console.log(`Features: ${stats.featureCount}`);
  console.log(`Families: ${Object.entries(stats.families).map(([k, v]) => `${k}=${v}`).join(", ")}`);
  console.log(`Meta model: ${snapshot.metaModel.featureNames.length} features, ${snapshot.metaModel.sampleCount} samples`);

  if (activate) {
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
  }

  await prisma.modelVersion.create({
    data: {
      name,
      description,
      isActive: activate,
      deployedPatterns: snapshot.deployedPatterns as any,
      featureBins: snapshot.featureBins as any,
      metaModel: snapshot.metaModel as any,
      tuningConfig: snapshot.tuningConfig as any,
      stats: stats as any,
    },
  });

  console.log(`\nModel version "${name}" created${activate ? " and activated" : ""}.`);
}

export async function listModelVersions(_args: string[] = []): Promise<void> {
  const versions = await prisma.modelVersion.findMany({
    select: {
      id: true,
      name: true,
      description: true,
      isActive: true,
      stats: true,
      createdAt: true,
    },
    orderBy: { createdAt: "desc" },
  });

  if (versions.length === 0) {
    console.log("No model versions found. Use snapshot:model-version --name <name> to create one.");
    return;
  }

  console.log(`\n=== Model Versions (${versions.length}) ===\n`);
  for (const v of versions) {
    const stats = v.stats as ModelVersionRow["stats"];
    const marker = v.isActive ? " ★ ACTIVE" : "";
    console.log(`  ${v.name}${marker}`);
    console.log(`    ID: ${v.id}`);
    if (v.description) console.log(`    Description: ${v.description}`);
    if (stats) {
      console.log(`    Patterns: ${stats.patternCount} | Features: ${stats.featureCount}`);
      console.log(`    Families: ${Object.entries(stats.families).map(([k, v]) => `${k}=${v}`).join(", ")}`);
    }
    console.log(`    Created: ${v.createdAt.toISOString()}`);
    console.log();
  }
}

export async function activateModelVersion(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const name = flags.name;
  if (!name) {
    throw new Error("--name is required. Example: --name v1.0-baseline");
  }

  const version = await prisma.modelVersion.findUnique({ where: { name } });
  if (!version) {
    throw new Error(`Model version "${name}" not found.`);
  }

  await prisma.modelVersion.updateMany({
    where: { isActive: true },
    data: { isActive: false },
  });

  await prisma.modelVersion.update({
    where: { id: version.id },
    data: { isActive: true },
  });

  console.log(`Model version "${name}" is now active. Predictions will use this snapshot.`);
}

export async function deactivateModelVersion(_args: string[] = []): Promise<void> {
  const result = await prisma.modelVersion.updateMany({
    where: { isActive: true },
    data: { isActive: false },
  });

  if (result.count === 0) {
    console.log("No active model version to deactivate. Predictions already use live data.");
  } else {
    console.log("Model version deactivated. Predictions will now use live patterns/bins/model.");
  }
}

export async function deleteModelVersion(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const name = flags.name;
  if (!name) {
    throw new Error("--name is required.");
  }

  const version = await prisma.modelVersion.findUnique({ where: { name } });
  if (!version) {
    throw new Error(`Model version "${name}" not found.`);
  }
  if (version.isActive) {
    throw new Error(`Cannot delete active model version "${name}". Deactivate it first.`);
  }

  await prisma.modelVersion.delete({ where: { id: version.id } });
  console.log(`Model version "${name}" deleted.`);
}

export async function loadActiveModelVersion(): Promise<ModelVersionSnapshot | null> {
  const active = await prisma.modelVersion.findFirst({
    where: { isActive: true },
  });

  if (!active) return null;

  const patterns = active.deployedPatterns as unknown as DeployedPatternV2[];
  const bins = active.featureBins as unknown as Record<string, FeatureBinDef>;
  const meta = active.metaModel as unknown as MetaModel;
  const tuning = active.tuningConfig as unknown as typeof PREDICTION_TUNING;

  return {
    deployedPatterns: patterns,
    featureBins: bins,
    metaModel: meta,
    tuningConfig: tuning,
  };
}

export async function autoSnapshotBeforeDiscovery(): Promise<void> {
  const deployedCount = await prisma.patternV2.count({ where: { status: "deployed" } });
  if (deployedCount === 0) {
    console.log("No deployed patterns to snapshot. Skipping auto-snapshot.\n");
    return;
  }

  const metaModel = await loadMetaModel();
  if (!metaModel) {
    console.log("No meta model found. Skipping auto-snapshot.\n");
    return;
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const name = `auto-pre-discovery-${timestamp}`;

  const existing = await prisma.modelVersion.findUnique({ where: { name } });
  if (existing) {
    console.log(`Auto-snapshot "${name}" already exists. Skipping.\n`);
    return;
  }

  try {
    const snapshot = await collectCurrentSnapshot();
    const stats = computeSnapshotStats(snapshot);

    await prisma.modelVersion.create({
      data: {
        name,
        description: `Auto-snapshot before discovery run (${deployedCount} deployed patterns)`,
        isActive: false,
        deployedPatterns: snapshot.deployedPatterns as any,
        featureBins: snapshot.featureBins as any,
        metaModel: snapshot.metaModel as any,
        tuningConfig: snapshot.tuningConfig as any,
        stats: stats as any,
      },
    });

    console.log(`Auto-snapshot saved: "${name}" (${stats.patternCount} patterns, ${stats.featureCount} features)\n`);
  } catch (err) {
    console.warn(`Auto-snapshot failed (non-fatal): ${err instanceof Error ? err.message : err}\n`);
  }
}
