import { prisma } from "../db/prisma.js";

export async function aggregateCoverageReport(): Promise<void> {
  console.log("=== Aggregate Coverage Report ===\n");

  const seasons = await prisma.game.findMany({
    select: { season: true },
    distinct: ["season"],
    orderBy: { season: "asc" },
  });

  for (const { season } of seasons) {
    const gameDates = await prisma.game.findMany({
      where: { season },
      select: { date: true, homeTeamId: true, awayTeamId: true },
    });

    const teamsByDate = new Map<string, Set<number>>();
    for (const g of gameDates) {
      const key = g.date.toISOString().slice(0, 10);
      let set = teamsByDate.get(key);
      if (!set) {
        set = new Set();
        teamsByDate.set(key, set);
      }
      set.add(g.homeTeamId);
      set.add(g.awayTeamId);
    }

    const distinctDates = [...teamsByDate.keys()].sort();
    let expectedPairs = 0;
    for (const teams of teamsByDate.values()) {
      expectedPairs += teams.size;
    }

    const aggRows = await prisma.nightTeamAggregate.findMany({
      where: { season },
      select: { date: true, teamId: true },
    });

    const aggSet = new Set(
      aggRows.map((r) => `${r.date.toISOString().slice(0, 10)}|${r.teamId}`),
    );

    let coveredPairs = 0;
    const missingDates: string[] = [];

    for (const [dateStr, teams] of teamsByDate) {
      let dateCovered = true;
      for (const teamId of teams) {
        if (aggSet.has(`${dateStr}|${teamId}`)) {
          coveredPairs++;
        } else {
          dateCovered = false;
        }
      }
      if (!dateCovered) {
        missingDates.push(dateStr);
      }
    }

    const pct = expectedPairs > 0 ? ((coveredPairs / expectedPairs) * 100).toFixed(1) : "0.0";

    console.log(`  Season ${season}:`);
    console.log(`    Game dates:         ${distinctDates.length}`);
    console.log(`    Expected team-date: ${expectedPairs}`);
    console.log(`    Covered:            ${coveredPairs} (${pct}%)`);

    if (missingDates.length > 0) {
      console.log(`    Missing dates:      ${missingDates.length}`);
      const show = missingDates.slice(0, 10);
      for (const d of show) {
        const missingTeams: number[] = [];
        const teams = teamsByDate.get(d)!;
        for (const t of teams) {
          if (!aggSet.has(`${d}|${t}`)) missingTeams.push(t);
        }
        console.log(`      ${d}: missing ${missingTeams.length} team(s) [${missingTeams.slice(0, 5).join(", ")}${missingTeams.length > 5 ? "..." : ""}]`);
      }
      if (missingDates.length > 10) {
        console.log(`      ... and ${missingDates.length - 10} more dates`);
      }
    } else {
      console.log(`    Missing dates:      0 (full coverage)`);
    }

    console.log();
  }

  const statsCheck = await prisma.$queryRaw<
    { season: number; dates_with_games: bigint; dates_with_stats: bigint }[]
  >`
    SELECT
      g.season,
      COUNT(DISTINCT g.date)::bigint AS dates_with_games,
      COUNT(DISTINCT CASE WHEN ps.id IS NOT NULL THEN g.date END)::bigint AS dates_with_stats
    FROM "Game" g
    LEFT JOIN "PlayerGameStat" ps ON ps."gameId" = g.id
    GROUP BY g.season
    ORDER BY g.season
  `;

  console.log("  Stats Ingestion Health:");
  console.log("    " + "Season".padEnd(10) + "GameDates".padStart(10) + "  WithStats".padStart(10) + "  Coverage".padStart(10));
  console.log("    " + "-".repeat(42));
  for (const row of statsCheck) {
    const gd = Number(row.dates_with_games);
    const sd = Number(row.dates_with_stats);
    const pct = gd > 0 ? ((sd / gd) * 100).toFixed(1) : "0.0";
    console.log(
      "    " +
      String(row.season).padEnd(10) +
      String(gd).padStart(10) +
      String(sd).padStart(10) +
      `${pct}%`.padStart(10),
    );
  }

  console.log("\n=== End Aggregate Coverage Report ===");
}
