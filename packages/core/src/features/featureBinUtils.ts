export type FeatureBinDef = {
  kind: "quantile" | "fixed" | "hybrid";
  labels: string[];
  edges: number[];
  hybridLabels?: string[];
};

export function bucketValue(value: number | string, def: FeatureBinDef): string {
  if (typeof value === "string") {
    if (def.kind === "hybrid") {
      if ((def.hybridLabels ?? []).includes(value)) return value;
      if (def.labels.includes(value)) return value;
    }
    if (def.kind === "fixed" && def.labels.includes(value)) return value;
    return value;
  }
  if (!Number.isFinite(value)) return def.labels[0] ?? "UNKNOWN";
  if (def.edges.length === 0) return def.labels[0] ?? "UNKNOWN";
  for (let i = 0; i < def.edges.length; i++) {
    if (value <= def.edges[i]) return def.labels[i] ?? `B${i + 1}`;
  }
  return def.labels[def.labels.length - 1] ?? `B${def.labels.length}`;
}

export async function loadLatestFeatureBins(
  prisma: {
    $queryRawUnsafe: <T>(query: string) => Promise<T>;
  },
): Promise<Map<string, FeatureBinDef>> {
  const rows = await prisma.$queryRawUnsafe<Array<{ featureName: string; binEdges: unknown }>>(
    `SELECT DISTINCT ON ("featureName") "featureName", "binEdges"
     FROM "FeatureBin"
     ORDER BY "featureName", "createdAt" DESC`,
  );
  const out = new Map<string, FeatureBinDef>();
  for (const row of rows) {
    const parsed = row.binEdges as FeatureBinDef;
    if (!parsed || !Array.isArray(parsed.labels) || !Array.isArray(parsed.edges)) continue;
    out.set(row.featureName, parsed);
  }
  return out;
}
