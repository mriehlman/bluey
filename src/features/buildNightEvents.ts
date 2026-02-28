import { prisma } from "../db/prisma.js";
import { CATALOG } from "./eventCatalog.js";
import type { NightContext, TeamAgg } from "./eventCatalog.js";
import type { Prisma } from "@prisma/client";

interface EventBuildFlags {
  season?: number;
  dateFrom?: string;
  dateTo?: string;
}

export async function buildNightEvents(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const opts: EventBuildFlags = {
    season: flags.season ? Number(flags.season) : undefined,
    dateFrom: flags.dateFrom,
    dateTo: flags.dateTo,
  };

  const isIncremental = opts.season != null || opts.dateFrom != null || opts.dateTo != null;

  if (isIncremental) {
    console.log("Building NightEvents (incremental)...\n");
  } else {
    console.log("Building NightEvents (full rebuild)...\n");
  }

  const gameWhere: Prisma.GameWhereInput = {};
  if (opts.season != null) gameWhere.season = opts.season;
  if (opts.dateFrom || opts.dateTo) {
    gameWhere.date = {};
    if (opts.dateFrom) gameWhere.date.gte = new Date(opts.dateFrom);
    if (opts.dateTo) gameWhere.date.lte = new Date(opts.dateTo);
  }

  if (isIncremental) {
    const deleteWhere: Prisma.NightEventWhereInput = {};
    if (opts.season != null) deleteWhere.season = opts.season;
    if (opts.dateFrom || opts.dateTo) {
      deleteWhere.date = {};
      if (opts.dateFrom) deleteWhere.date.gte = new Date(opts.dateFrom);
      if (opts.dateTo) deleteWhere.date.lte = new Date(opts.dateTo);
    }
    const deleted = await prisma.nightEvent.deleteMany({ where: deleteWhere });
    console.log(`  Cleared ${deleted.count} existing events in range`);
  }

  const dates = await prisma.game.findMany({
    where: gameWhere,
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

  for (let i = 0; i < dates.length; i++) {
    const { date } = dates[i];

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
      if (rows.length > 0) {
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
    }

    const gameOdds = await prisma.gameOdds.findMany({
      where: { gameId: { in: gameIds } },
    });

    const ctx: NightContext = { date: dateStr, season, games, stats, teamAggregates, gameOdds: gameOdds.length > 0 ? gameOdds : undefined };

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

    const catalogHitCount = hits.length;

    // Infra: aggregate completeness
    if (useAggregates) {
      if (teamAggregates) {
        hits.push({
          date,
          season,
          eventKey: "AGGREGATES_PRESENT",
          meta: { teamCount: teamAggregates.length },
        });
      } else {
        hits.push({
          date,
          season,
          eventKey: "AGGREGATES_MISSING",
          meta: { reason: "no_rows_for_date" },
        });
      }
    }

    // Infra: score completeness
    const scoredGameCount = games.filter(
      (g) => g.homeScore != null && g.awayScore != null,
    ).length;
    hits.push({
      date,
      season,
      eventKey: "SCORES_PRESENT",
      meta: { scoredGameCount, gameCount: games.length },
    });

    hits.push({
      date,
      season,
      eventKey: "NIGHT_PROCESSED",
      meta: { gameCount: games.length, statCount: stats.length, eventHits: catalogHitCount },
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

    if (i % 50 === 0) {
      process.stdout.write(".");
    }
  }

  console.log(`\nProcessed ${dates.length} dates, created ${totalEvents} events across ${nightsWithEvents} nights`);
}
