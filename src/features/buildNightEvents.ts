import { prisma } from "../db/prisma.js";
import { CATALOG } from "./eventCatalog.js";
import type { NightContext, TeamAgg } from "./eventCatalog.js";

export async function buildNightEvents(): Promise<void> {
  const dates = await prisma.game.findMany({
    select: { date: true },
    distinct: ["date"],
    orderBy: { date: "asc" },
  });

  console.log(`Found ${dates.length} distinct game dates`);

  const aggCount = await prisma.nightTeamAggregate.count();
  const useAggregates = aggCount > 0;
  if (useAggregates) {
    console.log(`Using precomputed NightTeamAggregate (${aggCount} rows)`);
  }

  let totalEvents = 0;
  let nightsWithEvents = 0;

  for (const { date } of dates) {
    const games = await prisma.game.findMany({
      where: { date },
      include: { homeTeam: true, awayTeam: true },
    });

    const gameIds = games.map((g) => g.id);

    const stats = await prisma.playerGameStat.findMany({
      where: { gameId: { in: gameIds } },
      include: { player: true },
    });

    const seasons = [...new Set(games.map((g) => g.season))];
    if (seasons.length > 1) {
      console.warn(`  Warning: multiple seasons on ${date.toISOString().slice(0, 10)}: ${seasons.join(", ")}`);
    }
    const season = seasons[0];

    const dateStr = date.toISOString().slice(0, 10);

    let teamAggregates: TeamAgg[] | undefined;
    if (useAggregates) {
      const rows = await prisma.nightTeamAggregate.findMany({ where: { date } });
      teamAggregates = rows.map((r) => ({
        teamId: r.teamId,
        points: r.points,
        rebounds: r.rebounds,
        assists: r.assists,
        steals: r.steals,
        blocks: r.blocks,
        turnovers: r.turnovers,
        minutes: r.minutes,
      }));
    }

    const ctx: NightContext = { date: dateStr, season, games, stats, teamAggregates };

    const hits: { date: Date; season: number; eventKey: string; meta: unknown }[] = [];

    for (const def of CATALOG) {
      const result = def.compute(ctx);
      if (result.hit) {
        hits.push({
          date,
          season,
          eventKey: def.key,
          meta: result.meta ?? null,
        });
      }
    }

    hits.push({
      date,
      season,
      eventKey: "NIGHT_PROCESSED",
      meta: { gameCount: games.length, statCount: stats.length, eventHits: hits.length },
    });

    if (stats.length > 0) {
      hits.push({
        date,
        season,
        eventKey: "STATS_PRESENT",
        meta: { statCount: stats.length },
      });
    }

    if (hits.length > 0) {
      nightsWithEvents++;
      const result = await prisma.nightEvent.createMany({
        data: hits.map((h) => ({
          date: h.date,
          season: h.season,
          eventKey: h.eventKey,
          value: true,
          meta: h.meta as any,
        })),
        skipDuplicates: true,
      });
      totalEvents += result.count;
    }

    if (dates.indexOf({ date }) % 50 === 0) {
      process.stdout.write(".");
    }
  }

  console.log(`\nProcessed ${dates.length} dates, created ${totalEvents} events across ${nightsWithEvents} nights`);
}
