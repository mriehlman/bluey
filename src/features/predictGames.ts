import { prisma } from "../db/prisma.js";
import { computeContextForDate } from "./buildGameContext.js";
import { GAME_EVENT_CATALOG } from "./gameEventCatalog.js";
import type { GameEventContext } from "./gameEventCatalog.js";
import type { GameContext, PlayerGameContext } from "@prisma/client";
import { getDiscoveryV2MatchesForGameIds } from "../patterns/discoveryV2.js";

function getCurrentSeason(): number {
  const now = new Date();
  // NBA season starts in October, so Oct-Dec uses current year, Jan-Sep uses previous year
  return now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
}

function getSeasonForDate(date: Date): number {
  // NBA season starts in October, so Oct-Dec uses that year, Jan-Sep uses previous year
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

export async function predictGames(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date;
  if (!dateStr) {
    console.error("Usage: predict:games --date YYYY-MM-DD");
    process.exit(1);
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);

  console.log(`\n=== Predictions for ${dateStr} (season ${season}) ===\n`);

  // Load games on this date
  const games = await prisma.game.findMany({
    where: {
      date: targetDate,
    },
    include: {
      homeTeam: true,
      awayTeam: true,
      playerStats: true,
      odds: true,
    },
  });

  if (games.length === 0) {
    console.log("No games found for this date.");
    return;
  }

  console.log(`Found ${games.length} games\n`);

  // Compute live context
  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  // Load all stored patterns
  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
  });
  const deployedV2Count = await prisma.patternV2.count({
    where: { status: "deployed" },
  });
  const tokenizedTodayCount = await prisma.gameFeatureToken.count({
    where: { gameId: { in: games.map((g) => g.id) } },
  });
  const discoveryV2ByGame = await getDiscoveryV2MatchesForGameIds(games.map((g) => g.id));
  console.log(`Loaded ${patterns.length} legacy stored patterns`);
  console.log(`Loaded ${deployedV2Count} deployed PatternV2 rows`);
  console.log(`Found ${tokenizedTodayCount}/${games.length} GameFeatureToken rows for target games\n`);
  if (deployedV2Count > 0 && tokenizedTodayCount === 0) {
    console.log(
      `  Note: deployed v2 patterns exist, but none of today's games are tokenized yet.`,
    );
    console.log(
      `  Run: bun run src/cli/index.ts build:quantized-game-features --from-season ${season} --to-season ${season}\n`,
    );
  }

  // Load player data for player contexts
  const playerIds = [...new Set(games.flatMap((g) => g.playerStats.map((s) => s.playerId)))];
  const playerContextsFromDB = await prisma.playerGameContext.findMany({
    where: {
      playerId: { in: playerIds },
      game: { season, date: { lt: targetDate } },
    },
    orderBy: { game: { date: "desc" } },
    distinct: ["playerId"],
  });

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);

    if (!homeSnap || !awaySnap) {
      console.log(`${game.homeTeam.code ?? game.homeTeam.name} vs ${game.awayTeam.code ?? game.awayTeam.name}  |  Insufficient context (< 10 games)\n`);
      continue;
    }

    // Build virtual GameContext
    const virtualContext: GameContext = {
      id: "",
      gameId: game.id,
      homeWins: homeSnap.wins,
      homeLosses: homeSnap.losses,
      homePpg: homeSnap.ppg,
      homeOppg: homeSnap.oppg,
      homePace: homeSnap.pace,
      homeRebPg: homeSnap.rebPg,
      homeAstPg: homeSnap.astPg,
      homeFg3Pct: homeSnap.fg3Pct,
      homeFtPct: homeSnap.ftPct,
      homeRankOff: homeSnap.rankOff,
      homeRankDef: homeSnap.rankDef,
      homeRankPace: homeSnap.rankPace,
      homeStreak: homeSnap.streak,
      awayWins: awaySnap.wins,
      awayLosses: awaySnap.losses,
      awayPpg: awaySnap.ppg,
      awayOppg: awaySnap.oppg,
      awayPace: awaySnap.pace,
      awayRebPg: awaySnap.rebPg,
      awayAstPg: awaySnap.astPg,
      awayFg3Pct: awaySnap.fg3Pct,
      awayFtPct: awaySnap.ftPct,
      awayRankOff: awaySnap.rankOff,
      awayRankDef: awaySnap.rankDef,
      awayRankPace: awaySnap.rankPace,
      awayStreak: awaySnap.streak,
      homeRestDays: homeSnap.lastGameDate
        ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1
        : null,
      awayRestDays: awaySnap.lastGameDate
        ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1
        : null,
      homeIsB2b: false,
      awayIsB2b: false,
      h2hHomeWins: 0,
      h2hAwayWins: 0,
    };

    virtualContext.homeIsB2b = virtualContext.homeRestDays === 0;
    virtualContext.awayIsB2b = virtualContext.awayRestDays === 0;

    // Build player contexts for this game from snapshots
    const virtualPlayerContexts: PlayerGameContext[] = [];
    for (const stat of game.playerStats) {
      const pSnap = playerSnapshots.get(stat.playerId);
      if (!pSnap) continue;

      const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
      const oppSnap = teamSnapshots.get(oppTeamId);

      virtualPlayerContexts.push({
        id: "",
        gameId: game.id,
        playerId: pSnap.playerId,
        teamId: pSnap.teamId,
        gamesPlayed: pSnap.gamesPlayed,
        ppg: pSnap.ppg,
        rpg: pSnap.rpg,
        apg: pSnap.apg,
        mpg: pSnap.mpg,
        fg3Pct: pSnap.fg3Pct,
        ftPct: pSnap.ftPct,
        last5Ppg: pSnap.last5Ppg,
        rankPpg: pSnap.rankPpg,
        rankRpg: pSnap.rankRpg,
        rankApg: pSnap.rankApg,
        oppRankDef: oppSnap?.rankDef ?? null,
      });
    }

    // If no player stats yet (future game), use most recent DB-stored player contexts
    if (virtualPlayerContexts.length === 0) {
      const relevantPCtx = playerContextsFromDB.filter(
        (p) => p.teamId === game.homeTeamId || p.teamId === game.awayTeamId,
      );
      virtualPlayerContexts.push(...relevantPCtx);
    }

    const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;

    const ctx: GameEventContext = {
      game: { ...game, homeTeam: game.homeTeam, awayTeam: game.awayTeam },
      context: virtualContext,
      playerContexts: virtualPlayerContexts,
      stats: game.playerStats,
      odds: consensusOdds,
    };

    // Compute active conditions
    const activeConditions = new Set<string>();
    for (const def of GAME_EVENT_CATALOG) {
      if (def.type !== "condition") continue;
      for (const side of def.sides) {
        const result = def.compute(ctx, side);
        if (result.hit) {
          activeConditions.add(`${def.key}:${side}`);
        }
      }
    }

    // Match against stored patterns
    const matchingPredictions: {
      conditions: string[];
      outcome: string;
      hitRate: number;
      hitCount: number;
      sampleSize: number;
      seasons: number;
      confidenceScore: number;
      valueScore: number;
    }[] = [];

    for (const pattern of patterns) {
      const allConditionsMet = pattern.conditions.every((c) => activeConditions.has(c));
      if (allConditionsMet) {
        matchingPredictions.push({
          conditions: pattern.conditions,
          outcome: pattern.outcome,
          hitRate: pattern.hitRate,
          hitCount: pattern.hitCount,
          sampleSize: pattern.sampleSize,
          seasons: pattern.seasons,
          confidenceScore: pattern.confidenceScore ?? 0,
          valueScore: pattern.valueScore ?? 0,
        });
      }
    }

    matchingPredictions.sort((a, b) => b.confidenceScore - a.confidenceScore || b.valueScore - a.valueScore);

    // Print output
    const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeamId}`;
    const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeamId}`;
    const spreadStr = consensusOdds?.spreadHome != null ? `Spread: ${homeLabel} ${consensusOdds.spreadHome > 0 ? "+" : ""}${consensusOdds.spreadHome}` : "";
    const totalStr = consensusOdds?.totalOver != null ? `O/U: ${consensusOdds.totalOver}` : "";
    const lineInfo = [spreadStr, totalStr].filter(Boolean).join("  |  ");

    console.log(`${homeLabel} vs ${awayLabel}${lineInfo ? `  |  ${lineInfo}` : ""}`);
    console.log(`  Record: ${homeLabel} ${homeSnap.wins}-${homeSnap.losses} (Off #${homeSnap.rankOff}, Def #${homeSnap.rankDef})  |  ${awayLabel} ${awaySnap.wins}-${awaySnap.losses} (Off #${awaySnap.rankOff}, Def #${awaySnap.rankDef})`);
    console.log(`  Active conditions: ${activeConditions.size > 0 ? [...activeConditions].join(", ") : "(none)"}`);

    if (matchingPredictions.length === 0) {
      console.log("  No matching patterns found.\n");
    } else {
      console.log(`  ${matchingPredictions.length} matching predictions:\n`);
      const top = matchingPredictions.slice(0, 10);
      for (let i = 0; i < top.length; i++) {
        const p = top[i];
        const edge = ((p.hitRate - 0.524) * 100).toFixed(1);
        console.log(`  [${i + 1}] ${p.conditions.join(" + ")}`);
        console.log(`      -> ${p.outcome}  |  ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  ${p.seasons} seasons  |  edge ${Number(edge) >= 0 ? "+" : ""}${edge}%`);
      }
      console.log("");
    }

    const v2Matches = discoveryV2ByGame.get(game.id) ?? [];
    if (v2Matches.length > 0) {
      console.log(`  Discovery v2 deployed matches (${v2Matches.length}):`);
      for (const m of v2Matches.slice(0, 5)) {
        console.log(
          `    ${m.conditions.join(" + ")} -> ${m.outcomeType} | posterior ${(m.posteriorHitRate * 100).toFixed(1)}% | edge ${(m.edge * 100).toFixed(1)}% | n=${m.n}`,
        );
      }
      console.log("");
    }
  }
}

export async function predictPlayers(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date;
  if (!dateStr) {
    console.error("Usage: predict:players --date YYYY-MM-DD");
    process.exit(1);
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);

  console.log(`\n=== Player Predictions for ${dateStr} (season ${season}) ===\n`);

  const games = await prisma.game.findMany({
    where: { date: targetDate },
    include: {
      homeTeam: true,
      awayTeam: true,
      playerStats: true,
      odds: true,
    },
  });

  if (games.length === 0) {
    console.log("No games found for this date.");
    return;
  }

  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  // Load player patterns (those with player-related outcomes)
  const playerOutcomeKeys = [
    "PLAYER_30_PLUS", "PLAYER_40_PLUS", "PLAYER_DOUBLE_DOUBLE",
    "PLAYER_TRIPLE_DOUBLE", "PLAYER_10_PLUS_ASSISTS",
    "PLAYER_10_PLUS_REBOUNDS", "PLAYER_5_PLUS_THREES",
  ].map((k) => `${k}:game`);

  const patterns = await prisma.gamePattern.findMany({
    where: { outcome: { in: playerOutcomeKeys } },
    orderBy: { confidenceScore: "desc" },
  });

  console.log(`Loaded ${patterns.length} player-outcome patterns\n`);

  // Load players
  const playerIds = [...playerSnapshots.keys()];
  const players = await prisma.player.findMany({
    where: { id: { in: playerIds } },
  });
  const playerMap = new Map(players.map((p) => [p.id, p]));

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    if (!homeSnap || !awaySnap) continue;

    const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeamId}`;
    const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeamId}`;

    console.log(`${homeLabel} vs ${awayLabel}`);

    // Build virtual context and run condition events (same as predictGames)
    const virtualContext = buildVirtualContext(game, homeSnap, awaySnap, targetDate);
    const virtualPlayerContexts = buildVirtualPlayerContexts(game, playerSnapshots, teamSnapshots);
    const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;

    const ctx: GameEventContext = {
      game: { ...game, homeTeam: game.homeTeam, awayTeam: game.awayTeam },
      context: virtualContext,
      playerContexts: virtualPlayerContexts,
      stats: game.playerStats,
      odds: consensusOdds,
    };

    const activeConditions = new Set<string>();
    for (const def of GAME_EVENT_CATALOG) {
      if (def.type !== "condition") continue;
      for (const side of def.sides) {
        if (def.compute(ctx, side).hit) {
          activeConditions.add(`${def.key}:${side}`);
        }
      }
    }

    const matchingPredictions: {
      conditions: string[];
      outcome: string;
      hitRate: number;
      hitCount: number;
      sampleSize: number;
      seasons: number;
      confidenceScore: number;
    }[] = [];

    for (const pattern of patterns) {
      if (pattern.conditions.every((c) => activeConditions.has(c))) {
        matchingPredictions.push({
          conditions: pattern.conditions,
          outcome: pattern.outcome,
          hitRate: pattern.hitRate,
          hitCount: pattern.hitCount,
          sampleSize: pattern.sampleSize,
          seasons: pattern.seasons,
          confidenceScore: pattern.confidenceScore ?? 0,
        });
      }
    }

    matchingPredictions.sort((a, b) => b.confidenceScore - a.confidenceScore);

    // Show notable players
    const teamPlayers = [...playerSnapshots.values()].filter(
      (p) => p.teamId === game.homeTeamId || p.teamId === game.awayTeamId,
    );

    const notablePlayers = teamPlayers.filter(
      (p) => (p.rankPpg != null && p.rankPpg <= 20) || (p.rankRpg != null && p.rankRpg <= 20) || (p.rankApg != null && p.rankApg <= 20),
    );

    if (notablePlayers.length > 0) {
      console.log("  Notable players:");
      for (const p of notablePlayers.sort((a, b) => a.ppg > b.ppg ? -1 : 1).slice(0, 6)) {
        const info = playerMap.get(p.playerId);
        const name = info ? `${info.firstname ?? ""} ${info.lastname ?? ""}`.trim() : `Player ${p.playerId}`;
        const side = p.teamId === game.homeTeamId ? homeLabel : awayLabel;
        const ranks = [
          p.rankPpg != null ? `PPG #${p.rankPpg}` : null,
          p.rankRpg != null ? `RPG #${p.rankRpg}` : null,
          p.rankApg != null ? `APG #${p.rankApg}` : null,
        ].filter(Boolean).join(", ");
        console.log(`    ${name} (${side}): ${p.ppg.toFixed(1)}/${p.rpg.toFixed(1)}/${p.apg.toFixed(1)}  [${ranks}]`);
      }
    }

    if (matchingPredictions.length > 0) {
      console.log(`  ${matchingPredictions.length} player-outcome predictions:`);
      for (const p of matchingPredictions.slice(0, 5)) {
        const edge = ((p.hitRate - 0.524) * 100).toFixed(1);
        console.log(`    ${p.conditions.join(" + ")}  ->  ${p.outcome}  |  ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  edge ${Number(edge) >= 0 ? "+" : ""}${edge}%`);
      }
    } else {
      console.log("  No player-outcome patterns matched.");
    }
    console.log("");
  }
}

function buildVirtualContext(
  game: { id: string; homeTeamId: number; awayTeamId: number },
  homeSnap: { wins: number; losses: number; ppg: number; oppg: number; pace: number | null; rebPg: number | null; astPg: number | null; fg3Pct: number | null; ftPct: number | null; rankOff: number | null; rankDef: number | null; rankPace: number | null; streak: number; lastGameDate: Date | null },
  awaySnap: typeof homeSnap,
  targetDate: Date,
): GameContext {
  const homeRestDays = homeSnap.lastGameDate
    ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;
  const awayRestDays = awaySnap.lastGameDate
    ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;

  return {
    id: "",
    gameId: game.id,
    homeWins: homeSnap.wins,
    homeLosses: homeSnap.losses,
    homePpg: homeSnap.ppg,
    homeOppg: homeSnap.oppg,
    homePace: homeSnap.pace,
    homeRebPg: homeSnap.rebPg,
    homeAstPg: homeSnap.astPg,
    homeFg3Pct: homeSnap.fg3Pct,
    homeFtPct: homeSnap.ftPct,
    homeRankOff: homeSnap.rankOff,
    homeRankDef: homeSnap.rankDef,
    homeRankPace: homeSnap.rankPace,
    homeStreak: homeSnap.streak,
    awayWins: awaySnap.wins,
    awayLosses: awaySnap.losses,
    awayPpg: awaySnap.ppg,
    awayOppg: awaySnap.oppg,
    awayPace: awaySnap.pace,
    awayRebPg: awaySnap.rebPg,
    awayAstPg: awaySnap.astPg,
    awayFg3Pct: awaySnap.fg3Pct,
    awayFtPct: awaySnap.ftPct,
    awayRankOff: awaySnap.rankOff,
    awayRankDef: awaySnap.rankDef,
    awayRankPace: awaySnap.rankPace,
    awayStreak: awaySnap.streak,
    homeRestDays,
    awayRestDays,
    homeIsB2b: homeRestDays === 0,
    awayIsB2b: awayRestDays === 0,
    h2hHomeWins: 0,
    h2hAwayWins: 0,
  };
}

function buildVirtualPlayerContexts(
  game: { id: string; homeTeamId: number; awayTeamId: number; playerStats: { playerId: number; teamId: number }[] },
  playerSnapshots: Map<number, { playerId: number; teamId: number; gamesPlayed: number; ppg: number; rpg: number; apg: number; mpg: number; fg3Pct: number | null; ftPct: number | null; last5Ppg: number | null; rankPpg: number | null; rankRpg: number | null; rankApg: number | null }>,
  teamSnapshots: Map<number, { rankDef: number | null }>,
): import("@prisma/client").PlayerGameContext[] {
  const result: import("@prisma/client").PlayerGameContext[] = [];

  for (const stat of game.playerStats) {
    const pSnap = playerSnapshots.get(stat.playerId);
    if (!pSnap) continue;

    const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
    const oppSnap = teamSnapshots.get(oppTeamId);

    result.push({
      id: "",
      gameId: game.id,
      playerId: pSnap.playerId,
      teamId: pSnap.teamId,
      gamesPlayed: pSnap.gamesPlayed,
      ppg: pSnap.ppg,
      rpg: pSnap.rpg,
      apg: pSnap.apg,
      mpg: pSnap.mpg,
      fg3Pct: pSnap.fg3Pct,
      ftPct: pSnap.ftPct,
      last5Ppg: pSnap.last5Ppg,
      rankPpg: pSnap.rankPpg,
      rankRpg: pSnap.rankRpg,
      rankApg: pSnap.rankApg,
      oppRankDef: oppSnap?.rankDef ?? null,
    });
  }

  return result;
}
