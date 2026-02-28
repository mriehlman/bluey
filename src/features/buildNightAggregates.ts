import { prisma } from "../db/prisma.js";
import { Prisma } from "@prisma/client";

interface AggregateFlags {
  season?: number;
  dateFrom?: string;
  dateTo?: string;
  missingOnly?: boolean;
  teamId?: number;
}

const BOOL_FLAGS = new Set(["missingOnly"]);

export async function buildNightAggregates(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (!args[i].startsWith("--")) continue;
    const key = args[i].slice(2);
    if (BOOL_FLAGS.has(key)) {
      flags[key] = "true";
    } else if (i + 1 < args.length) {
      flags[key] = args[i + 1];
      i++;
    }
  }

  const opts: AggregateFlags = {
    season: flags.season ? Number(flags.season) : undefined,
    dateFrom: flags.dateFrom,
    dateTo: flags.dateTo,
    missingOnly: flags.missingOnly === "true",
    teamId: flags.teamId ? Number(flags.teamId) : undefined,
  };

  const isIncremental = opts.season != null || opts.dateFrom != null || opts.dateTo != null;

  if (opts.missingOnly) {
    console.log("Building NightTeamAggregate (missing-only fill)...\n");
  } else if (isIncremental) {
    console.log("Building NightTeamAggregate (incremental)...\n");
  } else {
    console.log("Building NightTeamAggregate (full rebuild)...\n");
  }

  if (opts.teamId) {
    console.log(`  Debug filter: teamId=${opts.teamId}`);
  }

  const gameWhere: Prisma.GameWhereInput = {};
  if (opts.season != null) gameWhere.season = opts.season;
  if (opts.dateFrom || opts.dateTo) {
    gameWhere.date = {};
    if (opts.dateFrom) gameWhere.date.gte = new Date(opts.dateFrom);
    if (opts.dateTo) gameWhere.date.lte = new Date(opts.dateTo);
  }

  if (opts.missingOnly) {
    await fillMissingAggregates(gameWhere, opts);
    return;
  }

  if (isIncremental) {
    const deleteWhere: Prisma.NightTeamAggregateWhereInput = {};
    if (opts.season != null) deleteWhere.season = opts.season;
    if (opts.dateFrom || opts.dateTo) {
      deleteWhere.date = {};
      if (opts.dateFrom) deleteWhere.date.gte = new Date(opts.dateFrom);
      if (opts.dateTo) deleteWhere.date.lte = new Date(opts.dateTo);
    }
    if (opts.teamId) deleteWhere.teamId = opts.teamId;
    const deleted = await prisma.nightTeamAggregate.deleteMany({ where: deleteWhere });
    console.log(`  Cleared ${deleted.count} existing rows in range`);
  } else {
    const deleted = await prisma.nightTeamAggregate.deleteMany();
    console.log(`  Cleared ${deleted.count} existing rows`);
  }

  const dates = await prisma.game.findMany({
    where: gameWhere,
    select: { date: true },
    distinct: ["date"],
    orderBy: { date: "asc" },
  });

  console.log(`  Processing ${dates.length} game dates...`);

  let totalRows = 0;

  for (const { date } of dates) {
    const games = await prisma.game.findMany({
      where: { date },
      select: { id: true, season: true },
    });

    const season = games[0].season;
    const gameIds = games.map((g) => g.id);

    const statGroupWhere: Prisma.PlayerGameStatWhereInput = { gameId: { in: gameIds } };
    if (opts.teamId) statGroupWhere.teamId = opts.teamId;

    const teamAgg = await prisma.playerGameStat.groupBy({
      by: ["teamId"],
      where: statGroupWhere,
      _sum: {
        points: true,
        rebounds: true,
        assists: true,
        steals: true,
        blocks: true,
        turnovers: true,
        minutes: true,
      },
    });

    const rowsForDate = teamAgg.map((row) => ({
      date,
      season,
      teamId: row.teamId,
      points: row._sum.points ?? 0,
      rebounds: row._sum.rebounds ?? 0,
      assists: row._sum.assists ?? 0,
      steals: row._sum.steals ?? 0,
      blocks: row._sum.blocks ?? 0,
      turnovers: row._sum.turnovers ?? 0,
      minutes: row._sum.minutes ?? 0,
    }));

    if (rowsForDate.length) {
      await prisma.nightTeamAggregate.createMany({
        data: rowsForDate,
        skipDuplicates: true,
      });
      totalRows += rowsForDate.length;
    }
  }

  console.log(`\nDone. Wrote ${totalRows} NightTeamAggregate rows across ${dates.length} dates.`);
}

async function fillMissingAggregates(
  gameWhere: Prisma.GameWhereInput,
  opts: AggregateFlags
): Promise<void> {
  const allTeamDates = await prisma.$queryRaw<{ date: Date; team_id: number; season: number }[]>`
    SELECT DISTINCT g.date, t.id AS team_id, g.season
    FROM "Game" g
    JOIN (
      SELECT DISTINCT "teamId" AS id FROM "PlayerGameStat"
      WHERE "gameId" IN (SELECT id FROM "Game" WHERE 1=1
        ${opts.season != null ? Prisma.sql`AND season = ${opts.season}` : Prisma.empty}
        ${opts.dateFrom ? Prisma.sql`AND date >= ${new Date(opts.dateFrom)}` : Prisma.empty}
        ${opts.dateTo ? Prisma.sql`AND date <= ${new Date(opts.dateTo)}` : Prisma.empty}
      )
    ) t ON TRUE
    JOIN "PlayerGameStat" ps ON ps."gameId" = g.id AND ps."teamId" = t.id
    WHERE 1=1
      ${opts.season != null ? Prisma.sql`AND g.season = ${opts.season}` : Prisma.empty}
      ${opts.dateFrom ? Prisma.sql`AND g.date >= ${new Date(opts.dateFrom)}` : Prisma.empty}
      ${opts.dateTo ? Prisma.sql`AND g.date <= ${new Date(opts.dateTo)}` : Prisma.empty}
      ${opts.teamId ? Prisma.sql`AND t.id = ${opts.teamId}` : Prisma.empty}
  `;

  const existingAggs = await prisma.nightTeamAggregate.findMany({
    where: {
      ...(opts.season != null ? { season: opts.season } : {}),
      ...(opts.dateFrom || opts.dateTo
        ? {
            date: {
              ...(opts.dateFrom ? { gte: new Date(opts.dateFrom) } : {}),
              ...(opts.dateTo ? { lte: new Date(opts.dateTo) } : {}),
            },
          }
        : {}),
      ...(opts.teamId ? { teamId: opts.teamId } : {}),
    },
    select: { date: true, teamId: true },
  });

  const existingKeys = new Set(
    existingAggs.map((r) => `${r.date.toISOString().slice(0, 10)}:${r.teamId}`)
  );

  const missing = allTeamDates.filter(
    (r) => !existingKeys.has(`${r.date.toISOString().slice(0, 10)}:${r.team_id}`)
  );

  if (missing.length === 0) {
    console.log("  No missing aggregates found — everything is up to date.");
    return;
  }

  console.log(`  Found ${missing.length} missing (date, teamId) pairs to fill`);

  const missingByDate = new Map<string, { date: Date; season: number; teamIds: number[] }>();
  for (const row of missing) {
    const key = row.date.toISOString().slice(0, 10);
    if (!missingByDate.has(key)) {
      missingByDate.set(key, { date: row.date, season: row.season, teamIds: [] });
    }
    missingByDate.get(key)!.teamIds.push(row.team_id);
  }

  let totalRows = 0;

  for (const [, { date, season, teamIds }] of missingByDate) {
    const games = await prisma.game.findMany({
      where: { date },
      select: { id: true },
    });
    const gameIds = games.map((g) => g.id);

    const teamAgg = await prisma.playerGameStat.groupBy({
      by: ["teamId"],
      where: { gameId: { in: gameIds }, teamId: { in: teamIds } },
      _sum: {
        points: true,
        rebounds: true,
        assists: true,
        steals: true,
        blocks: true,
        turnovers: true,
        minutes: true,
      },
    });

    const rowsForDate = teamAgg.map((row) => ({
      date,
      season,
      teamId: row.teamId,
      points: row._sum.points ?? 0,
      rebounds: row._sum.rebounds ?? 0,
      assists: row._sum.assists ?? 0,
      steals: row._sum.steals ?? 0,
      blocks: row._sum.blocks ?? 0,
      turnovers: row._sum.turnovers ?? 0,
      minutes: row._sum.minutes ?? 0,
    }));

    if (rowsForDate.length) {
      await prisma.nightTeamAggregate.createMany({
        data: rowsForDate,
        skipDuplicates: true,
      });
      totalRows += rowsForDate.length;
    }
  }

  console.log(`\nDone. Filled ${totalRows} missing NightTeamAggregate rows across ${missingByDate.size} dates.`);
}
