import { prisma } from "../src/db/prisma.js";

const rows = await prisma.patternV2.findMany({
  where: { status: "deployed" },
  select: { id: true, outcomeType: true, conditions: true, score: true, n: true, edge: true },
});

console.log("deployed", rows.length);
const byLen: Record<string, number> = {};
for (const r of rows) {
  const l = (r.conditions?.length ?? 0).toString();
  byLen[l] = (byLen[l] ?? 0) + 1;
}
console.log("byLen", byLen);

const byOutcome: Record<string, number> = {};
for (const r of rows) {
  const k = (r.outcomeType ?? "").split(":")[0] ?? "unknown";
  byOutcome[k] = (byOutcome[k] ?? 0) + 1;
}
console.log(
  "byOutcomeTop",
  Object.entries(byOutcome)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15),
);

console.log(
  "sample",
  JSON.stringify(
    rows.slice(0, 10).map((r) => ({ out: r.outcomeType, n: r.n, edge: r.edge, conds: r.conditions })),
    null,
    2,
  ),
);

await prisma.$disconnect();

