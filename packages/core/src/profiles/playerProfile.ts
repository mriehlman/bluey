import { prisma } from "@bluey/db";
import type { RollupFilters } from "../stats/filters";
import { buildGameWhere, buildStatWhere } from "../stats/filters";

export async function playerProfile(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const playerId = Number(flags.playerId);
  if (!playerId) {
    console.error("Usage: profile:player --playerId <id> [--season N] [--dateFrom YYYY-MM-DD] ...");
    process.exit(1);
  }

  const filters: RollupFilters = {};
  if (flags.season) filters.season = Number(flags.season);
  if (flags.dateFrom) filters.dateFrom = flags.dateFrom;
  if (flags.dateTo) filters.dateTo = flags.dateTo;
  if (flags.opponentTeamId) filters.opponentTeamId = Number(flags.opponentTeamId);
  if (flags.homeAway && ["home", "away", "either"].includes(flags.homeAway)) {
    filters.homeAway = flags.homeAway as "home" | "away" | "either";
  }
  if (flags.minMinutes) filters.minMinutes = Number(flags.minMinutes);
  if (flags.stage) filters.stage = Number(flags.stage);

  const player = await prisma.player.findUnique({ where: { id: playerId } });
  if (!player) {
    console.error(`Player ${playerId} not found`);
    process.exit(1);
  }

  const playerName = [player.firstname, player.lastname].filter(Boolean).join(" ") || `#${playerId}`;

  const teamIds = await prisma.playerGameStat.findMany({
    where: { playerId },
    select: { teamId: true },
    distinct: ["teamId"],
  });

  const gameWhereConditions = teamIds.map((t) => buildGameWhere(filters, t.teamId));
  const statWhere = buildStatWhere(filters);

  const baseWhere = {
    playerId,
    ...statWhere,
    game: gameWhereConditions.length > 0
      ? { OR: gameWhereConditions }
      : buildGameWhere(filters),
  };

  const allStats = await prisma.playerGameStat.findMany({
    where: baseWhere,
    include: { game: true },
    orderBy: { game: { date: "asc" } },
  });

  if (allStats.length === 0) {
    console.log(`\nNo games found for ${playerName} with given filters.`);
    return;
  }

  const gp = allStats.length;
  const totals = {
    minutes: 0,
    points: 0,
    assists: 0,
    rebounds: 0,
    steals: 0,
    blocks: 0,
    turnovers: 0,
  };

  for (const s of allStats) {
    totals.minutes += s.minutes;
    totals.points += s.points;
    totals.assists += s.assists;
    totals.rebounds += s.rebounds;
    totals.steals += s.steals;
    totals.blocks += s.blocks;
    totals.turnovers += s.turnovers;
  }

  console.log(`\n=== Player Profile: ${playerName} ===\n`);

  console.log("  Totals:");
  console.log(`    Games Played: ${gp}`);
  console.log(`    Points:       ${totals.points}`);
  console.log(`    Assists:      ${totals.assists}`);
  console.log(`    Rebounds:     ${totals.rebounds}`);
  console.log(`    Steals:       ${totals.steals}`);
  console.log(`    Blocks:       ${totals.blocks}`);
  console.log(`    Turnovers:    ${totals.turnovers}`);
  console.log(`    Minutes:      ${Math.floor(totals.minutes / 60)}:${String(totals.minutes % 60).padStart(2, "0")}`);

  console.log("\n  Per-Game Averages:");
  console.log(`    PPG: ${(totals.points / gp).toFixed(1)}  APG: ${(totals.assists / gp).toFixed(1)}  RPG: ${(totals.rebounds / gp).toFixed(1)}`);
  console.log(`    SPG: ${(totals.steals / gp).toFixed(1)}  BPG: ${(totals.blocks / gp).toFixed(1)}  TPG: ${(totals.turnovers / gp).toFixed(1)}`);
  console.log(`    MPG: ${(totals.minutes / gp / 60).toFixed(1)}`);

  // Best games
  const byPoints = [...allStats].sort((a, b) => b.points - a.points).slice(0, 5);
  const byAssists = [...allStats].sort((a, b) => b.assists - a.assists).slice(0, 5);
  const byRebounds = [...allStats].sort((a, b) => b.rebounds - a.rebounds).slice(0, 5);

  console.log("\n  Best Games (Points):");
  for (const s of byPoints) {
    const d = s.game.date.toISOString().slice(0, 10);
    console.log(`    ${d}  ${s.points} pts, ${s.assists} ast, ${s.rebounds} reb`);
  }

  console.log("\n  Best Games (Assists):");
  for (const s of byAssists) {
    const d = s.game.date.toISOString().slice(0, 10);
    console.log(`    ${d}  ${s.assists} ast, ${s.points} pts, ${s.rebounds} reb`);
  }

  console.log("\n  Best Games (Rebounds):");
  for (const s of byRebounds) {
    const d = s.game.date.toISOString().slice(0, 10);
    console.log(`    ${d}  ${s.rebounds} reb, ${s.points} pts, ${s.assists} ast`);
  }

  // Distributions
  const pts30 = allStats.filter((s) => s.points >= 30).length;
  const pts40 = allStats.filter((s) => s.points >= 40).length;
  const pts50 = allStats.filter((s) => s.points >= 50).length;

  const tripleDoubles = allStats.filter((s) => {
    const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
    return cats.filter((v) => v >= 10).length >= 3;
  }).length;

  const doubleDoubles = allStats.filter((s) => {
    const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
    return cats.filter((v) => v >= 10).length >= 2;
  }).length;

  const min30 = allStats.filter((s) => s.minutes >= 30 * 60).length;
  const min35 = allStats.filter((s) => s.minutes >= 35 * 60).length;
  const min40 = allStats.filter((s) => s.minutes >= 40 * 60).length;

  console.log("\n  Distributions:");
  console.log(`    30+ points: ${pts30} games (${((pts30 / gp) * 100).toFixed(1)}%)`);
  console.log(`    40+ points: ${pts40} games (${((pts40 / gp) * 100).toFixed(1)}%)`);
  console.log(`    50+ points: ${pts50} games (${((pts50 / gp) * 100).toFixed(1)}%)`);
  console.log(`    Double-doubles:  ${doubleDoubles}`);
  console.log(`    Triple-doubles:  ${tripleDoubles}`);

  console.log("\n  Minutes Buckets:");
  console.log(`    30+ min: ${min30} games (${((min30 / gp) * 100).toFixed(1)}%)`);
  console.log(`    35+ min: ${min35} games (${((min35 / gp) * 100).toFixed(1)}%)`);
  console.log(`    40+ min: ${min40} games (${((min40 / gp) * 100).toFixed(1)}%)`);

  console.log("\n=== End Player Profile ===");
}
