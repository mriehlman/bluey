import { prisma } from "../src/db/prisma.js";

const dateStr = process.argv[2] ?? "2026-03-16";
const home = process.argv[3]; // team code or name substring
const away = process.argv[4];

const date = new Date(dateStr + "T00:00:00Z");
const games = await prisma.game.findMany({
  where: { date },
  include: { homeTeam: true, awayTeam: true, featureTokens: true, context: true },
});

const filtered = games.filter((g) => {
  const h = (g.homeTeam?.code ?? g.homeTeam?.name ?? "").toLowerCase();
  const a = (g.awayTeam?.code ?? g.awayTeam?.name ?? "").toLowerCase();
  if (home && !h.includes(home.toLowerCase())) return false;
  if (away && !a.includes(away.toLowerCase())) return false;
  return true;
});

console.log(`games on ${dateStr}: ${games.length}, filtered: ${filtered.length}`);
for (const g of filtered) {
  const tokens = (g.featureTokens?.[0]?.tokens ?? []) as string[];
  console.log(`\n${g.homeTeam?.code ?? g.homeTeam?.name} vs ${g.awayTeam?.code ?? g.awayTeam?.name} (${g.id})`);
  console.log(`hasContext=${g.context != null}`);
  console.log(`tokenCount=${tokens.length}`);
  console.log(tokens.slice(0, 60).join(", "));
}

const deployed = await prisma.patternV2.findMany({
  where: { status: "deployed" },
  select: { id: true, outcomeType: true, conditions: true, score: true },
  orderBy: { score: "desc" },
});
console.log(`\ndeployed patterns: ${deployed.length}`);
console.log(
  deployed
    .slice(0, 10)
    .map((p) => `${p.outcomeType} :: ${(p.conditions ?? []).join(" + ")}`)
    .join("\n"),
);

await prisma.$disconnect();

