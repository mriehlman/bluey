import { prisma } from "../packages/db/src";

type PatternRow = {
  id: string;
  outcomeType: string;
  conditions: string[];
  createdAt: Date;
  trainStats: unknown;
  valStats: unknown;
  forwardStats: unknown;
};

function parseArg(name: string, fallback?: string): string | undefined {
  const idx = process.argv.indexOf(name);
  if (idx >= 0 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  return fallback;
}

function num(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function extractPosterior(stats: unknown): number | null {
  if (!stats || typeof stats !== "object") return null;
  const s = stats as Record<string, unknown>;
  return (
    num(s.posteriorHitRate) ??
    num(s.posterior) ??
    num(s.hitRate) ??
    null
  );
}

function extractSamples(stats: unknown): number | null {
  if (!stats || typeof stats !== "object") return null;
  const s = stats as Record<string, unknown>;
  return num(s.n) ?? num(s.samples) ?? num(s.total) ?? null;
}

function avg(values: Array<number | null>): number | null {
  const v = values.filter((x): x is number => x != null);
  if (v.length === 0) return null;
  return v.reduce((a, b) => a + b, 0) / v.length;
}

function p10(values: Array<number | null>): number | null {
  const v = values.filter((x): x is number => x != null).sort((a, b) => a - b);
  if (v.length === 0) return null;
  const idx = Math.max(0, Math.floor(0.1 * (v.length - 1)));
  return v[idx] ?? null;
}

function tokenFamily(token: string): string {
  const core = token.replace(/^!/, "");
  const feature = core.split(":")[0] ?? core;
  return feature.replace(/^(home|away)_/, "");
}

async function main() {
  const label = parseArg("--label", "run")!;
  const family = (parseArg("--family", "TOTAL") ?? "TOTAL").toUpperCase();
  const windowSec = Number(parseArg("--window-sec", "5") ?? "5");
  const fromIso = parseArg("--from-iso");

  const latest = await prisma.patternV2.findFirst({
    orderBy: { createdAt: "desc" },
    select: { createdAt: true },
  });
  if (!latest) {
    console.log(JSON.stringify({ label, error: "no PatternV2 rows" }, null, 2));
    return;
  }

  const latestTs = latest.createdAt;
  const fromTs = fromIso ? new Date(fromIso) : new Date(latestTs.getTime() - windowSec * 1000);

  const rows = (await prisma.patternV2.findMany({
    where: {
      createdAt: { gte: fromTs, lte: latestTs },
      outcomeType: { startsWith: `${family}_` },
    },
    select: {
      id: true,
      outcomeType: true,
      conditions: true,
      createdAt: true,
      trainStats: true,
      valStats: true,
      forwardStats: true,
    },
    orderBy: { createdAt: "asc" },
  })) as PatternRow[];

  const trainP = rows.map((r) => extractPosterior(r.trainStats));
  const valP = rows.map((r) => extractPosterior(r.valStats));
  const fwdP = rows.map((r) => extractPosterior(r.forwardStats));
  const fwdN = rows.map((r) => extractSamples(r.forwardStats));

  const famCounts = new Map<string, number>();
  for (const r of rows) {
    for (const c of r.conditions ?? []) {
      const f = tokenFamily(c);
      famCounts.set(f, (famCounts.get(f) ?? 0) + 1);
    }
  }
  const topFamilies = [...famCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([featureFamily, count]) => ({ featureFamily, count }));

  const summary = {
    label,
    family,
    fromTs: fromTs.toISOString(),
    latestTs: latestTs.toISOString(),
    acceptedStoredCount: rows.length,
    avgTrainPosterior: avg(trainP),
    avgValPosterior: avg(valP),
    avgForwardPosterior: avg(fwdP),
    bottomDecileForwardPosterior: p10(fwdP),
    expectedPickVolumeProxyForwardSamplesSum: fwdN
      .filter((x): x is number => x != null)
      .reduce((a, b) => a + b, 0),
    expectedPickVolumeProxyForwardSamplesAvg: avg(fwdN),
    topAcceptedFeatureFamilies: topFamilies,
  };

  console.log(JSON.stringify(summary, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

