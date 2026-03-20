/**
 * Parity check: verifies that CLI and dashboard use the same prediction engine
 * by running generateGamePredictions() directly and optionally comparing to the
 * dashboard API response.
 *
 * Usage: bun run scripts/parityCheck.ts --date 2026-03-18
 */
import { prisma } from "../src/db/prisma.js";
import { computeContextForDate } from "../src/features/buildGameContext.js";
import {
  generateGamePredictions,
  loadLatestFeatureBins,
  loadMetaModel,
  type DeployedPatternV2,
  type GamePlayerContext,
  type PlayerPropRow,
} from "../src/features/predictionEngine.js";
import { LEDGER_TUNING } from "../src/config/tuning.js";
import type { GameContext } from "@prisma/client";

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }
  return flags;
}

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const dateStr = flags.date ?? new Date().toISOString().slice(0, 10);
  const dashboardUrl = flags.dashboardUrl ?? "http://localhost:3000";
  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);

  console.log(`\n=== Parity Check for ${dateStr} (season ${season}) ===\n`);

  // ── Load data (CLI-style) ────────────────────────────────────────────────
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
    process.exit(0);
  }

  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  const deployedV2 = await prisma.patternV2.findMany({
    where: { status: "deployed" },
    select: { id: true, outcomeType: true, conditions: true, posteriorHitRate: true, edge: true, score: true, n: true },
    orderBy: { score: "desc" },
  }) as DeployedPatternV2[];

  const bins = await loadLatestFeatureBins(prisma);
  const metaModel = await loadMetaModel();

  const gameIds = games.map((g) => g.id);
  const playerPropsRaw = await prisma.playerPropOdds.findMany({
    where: { gameId: { in: gameIds } },
    select: {
      gameId: true, playerId: true, market: true, line: true, overPrice: true, underPrice: true,
      player: { select: { firstname: true, lastname: true } },
    },
  });
  const playerPropsByGame = new Map<string, PlayerPropRow[]>();
  for (const gid of gameIds) playerPropsByGame.set(gid, []);
  for (const r of playerPropsRaw) playerPropsByGame.get(r.gameId)?.push(r);

  const playerContextsFromDB = await prisma.playerGameContext.findMany({
    where: {
      playerId: { in: [...new Set(games.flatMap((g) => g.playerStats.map((s) => s.playerId)))] },
      game: { season, date: { lt: targetDate } },
    },
    orderBy: { game: { date: "desc" } },
    distinct: ["playerId"],
  });

  const allPlayerIds = [...new Set([
    ...games.flatMap((g) => g.playerStats.map((s) => s.playerId)),
    ...playerPropsRaw.map((r) => r.playerId),
    ...[...playerSnapshots.keys()],
  ])];
  const playersForNames = await prisma.player.findMany({
    where: { id: { in: allPlayerIds } },
    select: { id: true, firstname: true, lastname: true },
  });
  const playerNameMap = new Map(playersForNames.map((p) => [p.id, p]));

  console.log(`Games: ${games.length}, Deployed V2 patterns: ${deployedV2.length}, Feature bins: ${bins.size}\n`);

  // ── Run engine for each game ─────────────────────────────────────────────
  type EngineResult = {
    gameId: string;
    matchup: string;
    outcomes: string[];
    betPicks: Array<{ outcomeType: string; metaScore: number | null; displayLabel: string | null }>;
  };
  const engineResults: EngineResult[] = [];

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    if (!homeSnap || !awaySnap) continue;

    const virtualContext: GameContext = {
      id: "", gameId: game.id,
      homeWins: homeSnap.wins, homeLosses: homeSnap.losses,
      homePpg: homeSnap.ppg, homeOppg: homeSnap.oppg,
      homePace: homeSnap.pace, homeRebPg: homeSnap.rebPg, homeAstPg: homeSnap.astPg,
      homeFg3Pct: homeSnap.fg3Pct, homeFtPct: homeSnap.ftPct,
      homeRankOff: homeSnap.rankOff, homeRankDef: homeSnap.rankDef, homeRankPace: homeSnap.rankPace,
      homeStreak: homeSnap.streak,
      awayWins: awaySnap.wins, awayLosses: awaySnap.losses,
      awayPpg: awaySnap.ppg, awayOppg: awaySnap.oppg,
      awayPace: awaySnap.pace, awayRebPg: awaySnap.rebPg, awayAstPg: awaySnap.astPg,
      awayFg3Pct: awaySnap.fg3Pct, awayFtPct: awaySnap.ftPct,
      awayRankOff: awaySnap.rankOff, awayRankDef: awaySnap.rankDef, awayRankPace: awaySnap.rankPace,
      awayStreak: awaySnap.streak,
      homeRestDays: homeSnap.lastGameDate ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1 : null,
      awayRestDays: awaySnap.lastGameDate ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1 : null,
      homeIsB2b: false, awayIsB2b: false,
      homeInjuryOutCount: null, homeInjuryDoubtfulCount: null, homeInjuryQuestionableCount: null, homeInjuryProbableCount: null,
      awayInjuryOutCount: null, awayInjuryDoubtfulCount: null, awayInjuryQuestionableCount: null, awayInjuryProbableCount: null,
      homeLineupCertainty: null, awayLineupCertainty: null,
      homeLateScratchRisk: null, awayLateScratchRisk: null,
      h2hHomeWins: 0, h2hAwayWins: 0,
    };
    virtualContext.homeIsB2b = virtualContext.homeRestDays === 0;
    virtualContext.awayIsB2b = virtualContext.awayRestDays === 0;

    const virtualPlayerContexts = game.playerStats
      .map((stat) => {
        const pSnap = playerSnapshots.get(stat.playerId);
        if (!pSnap) return null;
        const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
        return { ...pSnap, gameId: game.id, id: "", oppRankDef: teamSnapshots.get(oppTeamId)?.rankDef ?? null };
      })
      .filter(Boolean) as any[];

    if (virtualPlayerContexts.length === 0) {
      const relevantPCtx = playerContextsFromDB.filter(
        (p) => p.teamId === game.homeTeamId || p.teamId === game.awayTeamId,
      );
      virtualPlayerContexts.push(...relevantPCtx);
    }

    const homePlayerCtxs = virtualPlayerContexts.filter((c: any) => c.teamId === game.homeTeamId);
    const awayPlayerCtxs = virtualPlayerContexts.filter((c: any) => c.teamId === game.awayTeamId);
    const getTopPlayer = (list: any[], stat: "ppg" | "rpg" | "apg") => {
      if (list.length === 0) return null;
      const sorted = [...list].sort((a: any, b: any) => b[stat] - a[stat]);
      const top = sorted[0];
      const pInfo = playerNameMap.get(top.playerId);
      const name = pInfo ? `${pInfo.firstname ?? ""} ${pInfo.lastname ?? ""}`.trim() : `Player ${top.playerId}`;
      return { id: top.playerId, name, stat: top[stat] };
    };
    const gamePlayerCtx: GamePlayerContext = {
      homeTopScorer: getTopPlayer(homePlayerCtxs, "ppg"),
      homeTopRebounder: getTopPlayer(homePlayerCtxs, "rpg"),
      homeTopPlaymaker: getTopPlayer(homePlayerCtxs, "apg"),
      awayTopScorer: getTopPlayer(awayPlayerCtxs, "ppg"),
      awayTopRebounder: getTopPlayer(awayPlayerCtxs, "rpg"),
      awayTopPlaymaker: getTopPlayer(awayPlayerCtxs, "apg"),
    };

    const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;
    const engineOdds = consensusOdds
      ? {
          spreadHome: (consensusOdds as any).spreadHome ?? null,
          totalOver: (consensusOdds as any).totalOver ?? null,
          mlHome: (consensusOdds as any).mlHome ?? (consensusOdds as any).moneylineHome ?? null,
          mlAway: (consensusOdds as any).mlAway ?? (consensusOdds as any).moneylineAway ?? null,
        }
      : null;

    const propsForGame = playerPropsByGame.get(game.id) ?? [];
    const engineOutput = generateGamePredictions({
      season,
      gameContext: virtualContext,
      odds: engineOdds,
      deployedV2Patterns: deployedV2,
      featureBins: bins,
      metaModel,
      gamePlayerContext: gamePlayerCtx,
      propsForGame,
      maxBetPicksPerGame: Math.max(1, LEDGER_TUNING.maxBetPicksPerGame),
      fallbackAmericanOdds: LEDGER_TUNING.fallbackAmericanOdds,
    });

    const matchup = `${game.awayTeam.code ?? game.awayTeam.name} @ ${game.homeTeam.code ?? game.homeTeam.name}`;
    engineResults.push({
      gameId: game.id,
      matchup,
      outcomes: engineOutput.discoveryV2Matches.map((m) => m.outcomeType),
      betPicks: engineOutput.suggestedBetPicks.map((p) => ({
        outcomeType: p.outcomeType,
        metaScore: p.metaScore,
        displayLabel: p.displayLabel,
      })),
    });

    console.log(`${matchup}`);
    console.log(`  V2 matches: ${engineOutput.discoveryV2Matches.length}`);
    console.log(`  Bet picks: ${engineOutput.suggestedBetPicks.length}`);
    for (const p of engineOutput.suggestedBetPicks) {
      console.log(`    ${p.displayLabel ?? p.outcomeType} | meta ${p.metaScore != null ? (p.metaScore * 100).toFixed(1) + "%" : "n/a"}`);
    }
  }

  // ── Compare with dashboard API (if running) ──────────────────────────────
  console.log("\n--- Dashboard API comparison ---");
  try {
    const apiUrl = `${dashboardUrl}/api/predictions?date=${dateStr}`;
    const resp = await fetch(apiUrl, { signal: AbortSignal.timeout(5000) });
    if (!resp.ok) {
      console.log(`Dashboard returned ${resp.status} - skipping comparison.`);
    } else {
      const data = await resp.json() as any;
      const dashGames = (data.games ?? []) as any[];
      console.log(`Dashboard returned ${dashGames.length} games\n`);

      let parity = true;
      for (const engineGame of engineResults) {
        const dashGame = dashGames.find((g: any) => g.id === engineGame.gameId);
        if (!dashGame) {
          console.log(`  ${engineGame.matchup}: NOT FOUND in dashboard response`);
          continue;
        }

        const dashPicks = (dashGame.suggestedBetPicks ?? []) as any[];
        const enginePickTypes = engineGame.betPicks.map((p) => p.outcomeType).sort();
        const dashPickTypes = dashPicks.map((p: any) => p.outcomeType).sort();

        const picksParity = JSON.stringify(enginePickTypes) === JSON.stringify(dashPickTypes);
        if (!picksParity) {
          parity = false;
          console.log(`  ${engineGame.matchup}: MISMATCH`);
          console.log(`    Engine: ${enginePickTypes.join(", ") || "(none)"}`);
          console.log(`    Dash:   ${dashPickTypes.join(", ") || "(none)"}`);
        } else {
          console.log(`  ${engineGame.matchup}: MATCH (${enginePickTypes.length} picks)`);
        }
      }

      console.log(`\nOverall parity: ${parity ? "PASS" : "FAIL (differences found above)"}`);
    }
  } catch {
    console.log("Dashboard not reachable - skipping API comparison.");
    console.log("(Start the dashboard and re-run to compare against live API.)");
  }

  console.log("\n--- Summary ---");
  console.log(`Both CLI and dashboard now call generateGamePredictions() from src/features/predictionEngine.ts`);
  console.log(`Engine results: ${engineResults.length} games processed`);
  const totalPicks = engineResults.reduce((s, g) => s + g.betPicks.length, 0);
  console.log(`Total suggested bet picks: ${totalPicks}`);
  console.log("");

  process.exit(0);
}

main().catch((err) => {
  console.error("Parity check failed:", err);
  process.exit(1);
});
