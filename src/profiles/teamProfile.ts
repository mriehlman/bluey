import { prisma } from "../db/prisma.js";
import type { RollupFilters } from "../stats/filters.js";
import { buildGameWhere, buildStatWhere } from "../stats/filters.js";

export async function teamProfile(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const teamId = Number(flags.teamId);
  if (!teamId) {
    console.error("Usage: profile:team --teamId <id> [--season N] [--dateFrom YYYY-MM-DD] ...");
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

  const team = await prisma.team.findUnique({ where: { id: teamId } });
  if (!team) {
    console.error(`Team ${teamId} not found`);
    process.exit(1);
  }

  const teamName = [team.city, team.name].filter(Boolean).join(" ") || `Team #${teamId}`;

  const gameWhere = buildGameWhere(filters, teamId);
  const statWhere = buildStatWhere(filters);

  const games = await prisma.game.findMany({
    where: gameWhere,
    orderBy: { date: "asc" },
  });

  if (games.length === 0) {
    console.log(`\nNo games found for ${teamName} with given filters.`);
    return;
  }

  const gameIds = games.map((g) => g.id);

  const allStats = await prisma.playerGameStat.findMany({
    where: {
      teamId,
      gameId: { in: gameIds },
      ...statWhere,
    },
    include: { player: true, game: true },
  });

  // Aggregate team totals per game
  const perGame = new Map<string, {
    gameId: string;
    date: Date;
    points: number;
    assists: number;
    rebounds: number;
    steals: number;
    blocks: number;
    turnovers: number;
    minutes: number;
  }>();

  for (const s of allStats) {
    let entry = perGame.get(s.gameId);
    if (!entry) {
      entry = {
        gameId: s.gameId,
        date: s.game.date,
        points: 0,
        assists: 0,
        rebounds: 0,
        steals: 0,
        blocks: 0,
        turnovers: 0,
        minutes: 0,
      };
      perGame.set(s.gameId, entry);
    }
    entry.points += s.points;
    entry.assists += s.assists;
    entry.rebounds += s.rebounds;
    entry.steals += s.steals;
    entry.blocks += s.blocks;
    entry.turnovers += s.turnovers;
    entry.minutes += s.minutes;
  }

  const gp = perGame.size;
  const teamGames = [...perGame.values()];

  const totals = {
    points: 0,
    assists: 0,
    rebounds: 0,
    steals: 0,
    blocks: 0,
    turnovers: 0,
    minutes: 0,
  };

  for (const g of teamGames) {
    totals.points += g.points;
    totals.assists += g.assists;
    totals.rebounds += g.rebounds;
    totals.steals += g.steals;
    totals.blocks += g.blocks;
    totals.turnovers += g.turnovers;
    totals.minutes += g.minutes;
  }

  console.log(`\n=== Team Profile: ${teamName} ===\n`);

  console.log("  Totals:");
  console.log(`    Games Played: ${gp}`);
  console.log(`    Points:       ${totals.points}`);
  console.log(`    Assists:      ${totals.assists}`);
  console.log(`    Rebounds:     ${totals.rebounds}`);
  console.log(`    Steals:       ${totals.steals}`);
  console.log(`    Blocks:       ${totals.blocks}`);
  console.log(`    Turnovers:    ${totals.turnovers}`);

  console.log("\n  Per-Game Averages:");
  console.log(`    PPG: ${(totals.points / gp).toFixed(1)}  APG: ${(totals.assists / gp).toFixed(1)}  RPG: ${(totals.rebounds / gp).toFixed(1)}`);
  console.log(`    SPG: ${(totals.steals / gp).toFixed(1)}  BPG: ${(totals.blocks / gp).toFixed(1)}  TPG: ${(totals.turnovers / gp).toFixed(1)}`);

  // Blowout wins/losses
  let blowoutWins = 0;
  let blowoutLosses = 0;

  for (const game of games) {
    const margin = game.homeTeamId === teamId
      ? game.homeScore - game.awayScore
      : game.awayScore - game.homeScore;

    if (margin >= 20) blowoutWins++;
    else if (margin <= -20) blowoutLosses++;
  }

  const wins = games.filter((g) => {
    const teamScore = g.homeTeamId === teamId ? g.homeScore : g.awayScore;
    const oppScore = g.homeTeamId === teamId ? g.awayScore : g.homeScore;
    return teamScore > oppScore;
  }).length;

  console.log(`\n  Record: ${wins}-${gp - wins}`);
  console.log(`  Blowout wins (20+ margin):   ${blowoutWins}`);
  console.log(`  Blowout losses (20+ margin): ${blowoutLosses}`);

  // Threshold nights
  const pts140 = teamGames.filter((g) => g.points >= 140).length;
  const ast35 = teamGames.filter((g) => g.assists >= 35).length;
  const blk12 = teamGames.filter((g) => g.blocks >= 12).length;
  const pts120 = teamGames.filter((g) => g.points >= 120).length;
  const reb50 = teamGames.filter((g) => g.rebounds >= 50).length;

  console.log("\n  Threshold Nights:");
  console.log(`    Team points >= 140: ${pts140} games`);
  console.log(`    Team points >= 120: ${pts120} games`);
  console.log(`    Team assists >= 35: ${ast35} games`);
  console.log(`    Team blocks >= 12:  ${blk12} games`);
  console.log(`    Team rebounds >= 50: ${reb50} games`);

  // Top scorers for the team in filter window
  const playerPoints = new Map<number, { name: string; points: number; games: number }>();
  for (const s of allStats) {
    let entry = playerPoints.get(s.playerId);
    if (!entry) {
      const name = [s.player.firstname, s.player.lastname].filter(Boolean).join(" ") || `#${s.playerId}`;
      entry = { name, points: 0, games: 0 };
      playerPoints.set(s.playerId, entry);
    }
    entry.points += s.points;
    entry.games++;
  }

  const topScorers = [...playerPoints.entries()]
    .sort((a, b) => b[1].points - a[1].points)
    .slice(0, 10);

  console.log("\n  Top Scorers:");
  console.log("    " + "Player".padEnd(28) + "  Pts".padStart(6) + "  GP".padStart(5) + "  PPG".padStart(6));
  console.log("    " + "-".repeat(48));

  for (const [, p] of topScorers) {
    console.log(
      `    ${p.name.padEnd(28)}  ${String(p.points).padStart(6)}  ${String(p.games).padStart(5)}  ${(p.points / p.games).toFixed(1).padStart(6)}`
    );
  }

  console.log("\n=== End Team Profile ===");
}
