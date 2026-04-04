import { prisma } from "@bluey/db";
import { computeContextForDate } from "./buildGameContext";
import { GAME_EVENT_CATALOG } from "./gameEventCatalog";
import type { GameEventContext } from "./gameEventCatalog";
import type { GameContext, PlayerGameContext, GamePattern } from "@bluey/db";
import * as fs from "fs";
import * as path from "path";

const IMPLIED_PROB = 0.524;
const MIN_CONFIDENCE = 70;
const MIN_EDGE = 0.05;

interface Pick {
  game: string;
  betType: "spread" | "total" | "moneyline" | "player_prop" | "other";
  description: string;
  conditions: string[];
  outcome: string;
  hitRate: number;
  edge: number;
  sampleSize: number;
  seasons: number;
  grade: string;
  kellyFraction: number;
  suggestedUnit: number;
  playerLine?: { player: string; prop: string; line: number; odds: number };
}

interface PlayerPropLine {
  player: string;
  market: string;
  line: number;
  overOdds: number;
  underOdds: number;
}

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function calculateKelly(hitRate: number, odds: number = -110): number {
  const decimalOdds = odds > 0 ? (odds / 100) + 1 : (100 / Math.abs(odds)) + 1;
  const b = decimalOdds - 1;
  const p = hitRate;
  const q = 1 - p;
  const kelly = (b * p - q) / b;
  return Math.max(0, Math.min(kelly, 0.25)); // Cap at 25% of bankroll
}

function gradePattern(pattern: GamePattern, testHitRate?: number): string {
  const trainRate = pattern.hitRate;
  const edge = trainRate - IMPLIED_PROB;
  
  if (edge >= 0.15 && pattern.sampleSize >= 200 && pattern.seasons >= 5) return "A";
  if (edge >= 0.10 && pattern.sampleSize >= 150 && pattern.seasons >= 4) return "A";
  if (edge >= 0.08 && pattern.sampleSize >= 100 && pattern.seasons >= 3) return "B";
  if (edge >= 0.05 && pattern.sampleSize >= 75) return "C";
  return "D";
}

function categorizeBet(outcome: string): Pick["betType"] {
  if (outcome.includes("COVERED") || outcome.includes("SPREAD")) return "spread";
  if (outcome.includes("OVER_HIT") || outcome.includes("UNDER_HIT") || outcome.includes("TOTAL_")) return "total";
  if (outcome.includes("_WIN")) return "moneyline";
  if (outcome.includes("SCORER") || outcome.includes("REBOUNDER") || outcome.includes("ASSIST") || 
      outcome.includes("DOUBLE") || outcome.includes("TRIPLE") || outcome.includes("PLAYER_")) return "player_prop";
  return "other";
}

async function loadPlayerPropLines(gameDate: string): Promise<Map<string, PlayerPropLine[]>> {
  const propsDir = path.join(process.cwd(), "data", "raw", "odds", "player-props", gameDate);
  const result = new Map<string, PlayerPropLine[]>();
  
  if (!fs.existsSync(propsDir)) return result;
  
  const files = fs.readdirSync(propsDir).filter(f => f.endsWith(".json"));
  
  for (const file of files) {
    try {
      const content = JSON.parse(fs.readFileSync(path.join(propsDir, file), "utf-8"));
      // Create multiple key variations for matching
      const keys = [
        `${content.home_team} vs ${content.away_team}`,
        `${content.away_team} @ ${content.home_team}`,
        content.home_team,
        content.away_team,
      ];
      
      const lines: PlayerPropLine[] = [];
      const seenPlayers = new Set<string>();
      
      for (const bookmaker of content.bookmakers || []) {
        for (const market of bookmaker.markets || []) {
          const outcomes = market.outcomes || [];
          
          for (let i = 0; i < outcomes.length; i++) {
            const outcome = outcomes[i];
            if (outcome.name !== "Over") continue;
            
            const playerKey = `${outcome.description}:${market.key}`;
            if (seenPlayers.has(playerKey)) continue;
            seenPlayers.add(playerKey);
            
            const under = outcomes.find((o: any) => 
              o.name === "Under" && 
              o.description === outcome.description && 
              o.point === outcome.point
            );
            
            if (under) {
              lines.push({
                player: outcome.description,
                market: market.key,
                line: outcome.point,
                overOdds: outcome.price,
                underOdds: under.price,
              });
            }
          }
        }
      }
      
      for (const key of keys) {
        const existing = result.get(key) || [];
        result.set(key, [...existing, ...lines]);
      }
    } catch (e) {
      // Skip malformed files
    }
  }
  
  return result;
}

interface PlayerInfo {
  name: string;
  ppg: number;
  rpg: number;
  apg: number;
}

function matchOutcomeToLine(
  outcome: string, 
  lines: PlayerPropLine[], 
  homeTopScorer?: PlayerInfo, 
  awayTopScorer?: PlayerInfo,
  homeTopRebounder?: PlayerInfo,
  awayTopRebounder?: PlayerInfo,
): PlayerPropLine | null {
  const findPlayer = (target: PlayerInfo | undefined, market: string): PlayerPropLine | null => {
    if (!target) return null;
    const marketLines = lines.filter(l => l.market === market);
    const lastName = target.name.split(" ").slice(-1)[0].toLowerCase();
    return marketLines.find(l => l.player.toLowerCase().includes(lastName)) || null;
  };

  // Points outcomes
  if (outcome.includes("TOP_SCORER_25_PLUS") || outcome.includes("TOP_SCORER_30_PLUS")) {
    const target = outcome.includes("HOME") ? homeTopScorer : awayTopScorer;
    const match = findPlayer(target, "player_points");
    if (match) {
      const threshold = outcome.includes("30") ? 29.5 : 24.5;
      if (match.line >= threshold - 1) return match;
    }
  }

  // Rebounds outcomes
  if (outcome.includes("REBOUNDER_10_PLUS") || outcome.includes("REBOUNDER_12_PLUS") || outcome.includes("10_PLUS_REBOUNDS")) {
    const homeMatch = findPlayer(homeTopRebounder, "player_rebounds");
    const awayMatch = findPlayer(awayTopRebounder, "player_rebounds");
    const threshold = outcome.includes("12") ? 11.5 : 9.5;
    
    if (outcome.includes("HOME") && homeMatch && homeMatch.line >= threshold - 1) return homeMatch;
    if (outcome.includes("AWAY") && awayMatch && awayMatch.line >= threshold - 1) return awayMatch;
    
    // For generic PLAYER_10_PLUS_REBOUNDS, return the best match
    const allMatches = [homeMatch, awayMatch].filter(m => m && m.line >= threshold - 1);
    if (allMatches.length > 0) return allMatches[0]!;
  }

  // Double-double - suggest the top rebounder with points line
  if (outcome.includes("DOUBLE_DOUBLE")) {
    const homeMatch = findPlayer(homeTopRebounder, "player_points");
    const awayMatch = findPlayer(awayTopRebounder, "player_points");
    if (homeMatch) return homeMatch;
    if (awayMatch) return awayMatch;
  }

  return null;
}

export async function dailyPicks(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date || new Date().toISOString().slice(0, 10);
  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);
  const minGrade = flags.grade || "B";

  console.log(`\n${"═".repeat(60)}`);
  console.log(`  DAILY PICKS - ${dateStr}`);
  console.log(`${"═".repeat(60)}\n`);

  // Load games
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
    console.log("No games found for this date.\n");
    return;
  }

  console.log(`Found ${games.length} games\n`);

  // Load high-confidence patterns
  const patterns = await prisma.gamePattern.findMany({
    where: {
      confidenceScore: { gte: MIN_CONFIDENCE },
      hitRate: { gte: IMPLIED_PROB + MIN_EDGE },
    },
    orderBy: { confidenceScore: "desc" },
  });

  console.log(`Loaded ${patterns.length} high-confidence patterns\n`);

  // Compute context
  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  // Load player prop lines
  const playerPropLines = await loadPlayerPropLines(dateStr);

  // Load players for name matching
  const playerIds = [...playerSnapshots.keys()];
  const players = await prisma.player.findMany({ where: { id: { in: playerIds } } });
  const playerMap = new Map(players.map(p => [p.id, p]));

  const allPicks: Pick[] = [];

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);

    if (!homeSnap || !awaySnap) continue;

    const homeLabel = game.homeTeam.code ?? game.homeTeam.name;
    const awayLabel = game.awayTeam.code ?? game.awayTeam.name;
    const gameKey = `${game.homeTeam.name} vs ${game.awayTeam.name}`;

    // Build virtual context
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

    // Build player contexts
    const virtualPlayerContexts: PlayerGameContext[] = [];
    for (const [playerId, pSnap] of playerSnapshots) {
      if (pSnap.teamId !== game.homeTeamId && pSnap.teamId !== game.awayTeamId) continue;
      const oppTeamId = pSnap.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
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
        if (def.compute(ctx, side).hit) {
          activeConditions.add(`${def.key}:${side}`);
        }
      }
    }

    // Find top players for this game
    const homePlayers = virtualPlayerContexts.filter(p => p.teamId === game.homeTeamId);
    const awayPlayers = virtualPlayerContexts.filter(p => p.teamId === game.awayTeamId);
    const homeTopScorerCtx = homePlayers.sort((a, b) => b.ppg - a.ppg)[0];
    const awayTopScorerCtx = awayPlayers.sort((a, b) => b.ppg - a.ppg)[0];
    const homeTopRebounderCtx = homePlayers.sort((a, b) => b.rpg - a.rpg)[0];
    const awayTopRebounderCtx = awayPlayers.sort((a, b) => b.rpg - a.rpg)[0];

    const toPlayerInfo = (ctx: typeof homeTopScorerCtx): PlayerInfo | undefined => {
      if (!ctx) return undefined;
      const info = playerMap.get(ctx.playerId);
      return {
        name: `${info?.firstname ?? ""} ${info?.lastname ?? ""}`.trim(),
        ppg: ctx.ppg,
        rpg: ctx.rpg,
        apg: ctx.apg,
      };
    };

    const homeTopScorer = toPlayerInfo(homeTopScorerCtx);
    const awayTopScorer = toPlayerInfo(awayTopScorerCtx);
    const homeTopRebounder = toPlayerInfo(homeTopRebounderCtx);
    const awayTopRebounder = toPlayerInfo(awayTopRebounderCtx);

    // Match patterns
    for (const pattern of patterns) {
      const allConditionsMet = pattern.conditions.every((c) => activeConditions.has(c));
      if (!allConditionsMet) continue;

      const grade = gradePattern(pattern);
      if (minGrade === "A" && grade !== "A") continue;
      if (minGrade === "B" && grade !== "A" && grade !== "B") continue;

      const edge = pattern.hitRate - IMPLIED_PROB;
      const betType = categorizeBet(pattern.outcome);
      const kelly = calculateKelly(pattern.hitRate);
      
      // Build description
      let description = pattern.outcome.replace(/:game$/, "").replace(/_/g, " ");
      
      // Try to match player prop lines
      const gameLines = playerPropLines.get(gameKey) || playerPropLines.get(game.homeTeam.name ?? "") || [];
      const matchedLine = matchOutcomeToLine(
        pattern.outcome, 
        gameLines, 
        homeTopScorer, 
        awayTopScorer,
        homeTopRebounder,
        awayTopRebounder,
      );

      allPicks.push({
        game: `${awayLabel} @ ${homeLabel}`,
        betType,
        description,
        conditions: pattern.conditions,
        outcome: pattern.outcome,
        hitRate: pattern.hitRate,
        edge,
        sampleSize: pattern.sampleSize,
        seasons: pattern.seasons,
        grade,
        kellyFraction: kelly,
        suggestedUnit: Math.round(kelly * 100 * 10) / 10,
        playerLine: matchedLine ? {
          player: matchedLine.player,
          prop: matchedLine.market.replace("player_", ""),
          line: matchedLine.line,
          odds: matchedLine.overOdds,
        } : undefined,
      });
    }
  }

  // Sort by grade then edge
  allPicks.sort((a, b) => {
    const gradeOrder = { A: 0, B: 1, C: 2, D: 3 };
    const gradeDiff = (gradeOrder[a.grade as keyof typeof gradeOrder] ?? 4) - (gradeOrder[b.grade as keyof typeof gradeOrder] ?? 4);
    if (gradeDiff !== 0) return gradeDiff;
    return b.edge - a.edge;
  });

  // Group and display picks
  const spreadPicks = allPicks.filter(p => p.betType === "spread");
  const totalPicks = allPicks.filter(p => p.betType === "total");
  const mlPicks = allPicks.filter(p => p.betType === "moneyline");
  const playerPicks = allPicks.filter(p => p.betType === "player_prop");
  const otherPicks = allPicks.filter(p => p.betType === "other");

  function printPicks(picks: Pick[], title: string) {
    if (picks.length === 0) return;
    
    console.log(`\n${"─".repeat(50)}`);
    console.log(`  ${title} (${picks.length} picks)`);
    console.log(`${"─".repeat(50)}\n`);
    
    for (const pick of picks.slice(0, 10)) {
      const gradeColor = pick.grade === "A" ? "\x1b[32m" : pick.grade === "B" ? "\x1b[36m" : "\x1b[33m";
      console.log(`${gradeColor}[${pick.grade}]\x1b[0m ${pick.game}`);
      console.log(`    ${pick.description}`);
      console.log(`    Hit: ${(pick.hitRate * 100).toFixed(1)}% | Edge: +${(pick.edge * 100).toFixed(1)}% | n=${pick.sampleSize} | ${pick.seasons}szn`);
      console.log(`    Kelly: ${(pick.kellyFraction * 100).toFixed(1)}% → ${pick.suggestedUnit}u`);
      
      if (pick.playerLine) {
        console.log(`    LINE: ${pick.playerLine.player} ${pick.playerLine.prop} O${pick.playerLine.line} (${pick.playerLine.odds > 0 ? "+" : ""}${pick.playerLine.odds})`);
      }
      
      console.log(`    Conditions: ${pick.conditions.join(" + ")}`);
      console.log("");
    }
  }

  printPicks(spreadPicks, "SPREAD PICKS");
  printPicks(totalPicks, "TOTAL (O/U) PICKS");
  printPicks(mlPicks, "MONEYLINE PICKS");
  printPicks(playerPicks, "PLAYER PROP PICKS");
  printPicks(otherPicks, "OTHER PICKS");

  // Summary
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  SUMMARY`);
  console.log(`${"═".repeat(60)}\n`);
  
  const totalUnits = allPicks.reduce((sum, p) => sum + p.suggestedUnit, 0);
  const aGradeCount = allPicks.filter(p => p.grade === "A").length;
  const bGradeCount = allPicks.filter(p => p.grade === "B").length;
  
  console.log(`  Total picks: ${allPicks.length}`);
  console.log(`  A-grade: ${aGradeCount} | B-grade: ${bGradeCount}`);
  console.log(`  Total suggested units: ${totalUnits.toFixed(1)}u`);
  console.log(`  Avg edge: +${(allPicks.reduce((sum, p) => sum + p.edge, 0) / allPicks.length * 100).toFixed(1)}%`);
  console.log("");
}
