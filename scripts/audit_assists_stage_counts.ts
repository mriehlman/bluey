import { prisma } from "../packages/db/src";

function isAssistOutcome(outcomeType: string): boolean {
  const base = outcomeType.replace(/:.*$/, "").toUpperCase();
  return base.includes("ASSIST") || base.includes("PLAYMAKER");
}

async function main() {
  const from = process.argv[2] ?? "2026-03-01";
  const to = process.argv[3] ?? "2026-03-28";
  const gateMode = (process.argv[4] ?? "legacy").toLowerCase() === "strict" ? "strict" : "legacy";
  const baseUrl = process.argv[5] ?? "http://localhost:3001";

  const rows = await prisma.$queryRawUnsafe<Array<{ d: string }>>(
    `SELECT DISTINCT TO_CHAR("date", 'YYYY-MM-DD') as d
     FROM "Game"
     WHERE "date" >= '${from.replaceAll("'", "''")}'::date
       AND "date" <= '${to.replaceAll("'", "''")}'::date
     ORDER BY d ASC`,
  );
  const dates = rows.map((r) => r.d).filter(Boolean);

  const out: Array<{
    date: string;
    modelVersion: string | null;
    games: number;
    discoveryAssistMatches: number;
    modelAssistPicks: number;
    suggestedAssistPlays: number;
    suggestedAssistBetPicks: number;
    uniqueAssistOutcomesInDiscovery: string[];
  }> = [];

  for (const date of dates) {
    const url = `${baseUrl}/api/predictions?date=${encodeURIComponent(date)}&gateMode=${gateMode}`;
    const res = await fetch(url);
    if (!res.ok) {
      out.push({
        date,
        modelVersion: null,
        games: 0,
        discoveryAssistMatches: 0,
        modelAssistPicks: 0,
        suggestedAssistPlays: 0,
        suggestedAssistBetPicks: 0,
        uniqueAssistOutcomesInDiscovery: [`HTTP_${res.status}`],
      });
      continue;
    }
    const json = await res.json() as {
      modelVersion?: string;
      games?: Array<{
        discoveryV2Matches?: Array<{ outcomeType: string }>;
        modelPicks?: Array<{ outcomeType: string }>;
        suggestedPlays?: Array<{ outcomeType: string }>;
        suggestedBetPicks?: Array<{ outcomeType: string }>;
      }>;
    };
    const games = json.games ?? [];
    let discoveryAssistMatches = 0;
    let modelAssistPicks = 0;
    let suggestedAssistPlays = 0;
    let suggestedAssistBetPicks = 0;
    const discoveryOutcomes = new Set<string>();
    for (const g of games) {
      for (const p of g.discoveryV2Matches ?? []) {
        if (!isAssistOutcome(p.outcomeType)) continue;
        discoveryAssistMatches += 1;
        discoveryOutcomes.add(p.outcomeType);
      }
      for (const p of g.modelPicks ?? []) {
        if (isAssistOutcome(p.outcomeType)) modelAssistPicks += 1;
      }
      for (const p of g.suggestedPlays ?? []) {
        if (isAssistOutcome(p.outcomeType)) suggestedAssistPlays += 1;
      }
      for (const p of g.suggestedBetPicks ?? []) {
        if (isAssistOutcome(p.outcomeType)) suggestedAssistBetPicks += 1;
      }
    }
    out.push({
      date,
      modelVersion: json.modelVersion ?? null,
      games: games.length,
      discoveryAssistMatches,
      modelAssistPicks,
      suggestedAssistPlays,
      suggestedAssistBetPicks,
      uniqueAssistOutcomesInDiscovery: [...discoveryOutcomes].sort(),
    });
  }

  const totals = out.reduce(
    (acc, r) => {
      acc.discoveryAssistMatches += r.discoveryAssistMatches;
      acc.modelAssistPicks += r.modelAssistPicks;
      acc.suggestedAssistPlays += r.suggestedAssistPlays;
      acc.suggestedAssistBetPicks += r.suggestedAssistBetPicks;
      return acc;
    },
    {
      discoveryAssistMatches: 0,
      modelAssistPicks: 0,
      suggestedAssistPlays: 0,
      suggestedAssistBetPicks: 0,
    },
  );

  console.log(
    JSON.stringify(
      {
        from,
        to,
        gateMode,
        dates: out,
        totals,
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
