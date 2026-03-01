import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

interface TeamSnapshot {
  wins: number;
  losses: number;
  ppg: number;
  oppg: number;
  pace: number | null;
  rebPg: number | null;
  astPg: number | null;
  fg3Pct: number | null;
  ftPct: number | null;
  rankOff: number | null;
  rankDef: number | null;
  rankPace: number | null;
  streak: number;
  lastGameDate: Date | null;
}

function getCurrentSeason(): number {
  const now = new Date();
  return now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date");

  if (!dateStr) {
    const today = new Date().toISOString().slice(0, 10);
    return NextResponse.redirect(new URL(`/api/predictions?date=${today}`, req.url));
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = getCurrentSeason();

  const games = await prisma.game.findMany({
    where: { date: targetDate },
    include: {
      homeTeam: true,
      awayTeam: true,
      odds: true,
      context: true,
    },
    orderBy: { tipoffTimeUtc: "asc" },
  });

  if (games.length === 0) {
    return NextResponse.json({ date: dateStr, games: [], message: "No games found" });
  }

  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
  });

  const teamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const teamSnapshots = await computeTeamSnapshots(season, targetDate, teamIds);

  const result = games.map((game) => {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    const consensus = game.odds.find((o) => o.source === "consensus") ?? game.odds[0];

    const context = {
      home: homeSnap
        ? {
            record: `${homeSnap.wins}-${homeSnap.losses}`,
            ppg: homeSnap.ppg,
            oppg: homeSnap.oppg,
            rankOff: homeSnap.rankOff,
            rankDef: homeSnap.rankDef,
            streak: homeSnap.streak,
          }
        : null,
      away: awaySnap
        ? {
            record: `${awaySnap.wins}-${awaySnap.losses}`,
            ppg: awaySnap.ppg,
            oppg: awaySnap.oppg,
            rankOff: awaySnap.rankOff,
            rankDef: awaySnap.rankDef,
            streak: awaySnap.streak,
          }
        : null,
    };

    const matchingPatterns = findMatchingPatterns(game, homeSnap, awaySnap, patterns, targetDate);

    return {
      id: game.id,
      homeTeam: { id: game.homeTeamId, code: game.homeTeam.code, name: game.homeTeam.name },
      awayTeam: { id: game.awayTeamId, code: game.awayTeam.code, name: game.awayTeam.name },
      tipoff: game.tipoffTimeUtc,
      status: game.status,
      homeScore: game.homeScore,
      awayScore: game.awayScore,
      odds: consensus
        ? {
            spreadHome: consensus.spreadHome,
            totalOver: consensus.totalOver,
            mlHome: consensus.mlHome,
            mlAway: consensus.mlAway,
          }
        : null,
      context,
      predictions: matchingPatterns.slice(0, 10),
      predictionCount: matchingPatterns.length,
    };
  });

  return NextResponse.json({ date: dateStr, games: result });
}

async function computeTeamSnapshots(
  season: number,
  beforeDate: Date,
  teamIds: number[],
): Promise<Map<number, TeamSnapshot>> {
  const result = new Map<number, TeamSnapshot>();

  for (const teamId of teamIds) {
    const games = await prisma.game.findMany({
      where: {
        season,
        date: { lt: beforeDate },
        OR: [{ homeTeamId: teamId }, { awayTeamId: teamId }],
        homeScore: { gt: 0 },
      },
      orderBy: { date: "desc" },
      take: 82,
      select: {
        id: true,
        date: true,
        homeTeamId: true,
        awayTeamId: true,
        homeScore: true,
        awayScore: true,
      },
    });

    if (games.length < 5) continue;

    let wins = 0, losses = 0, pointsFor = 0, pointsAgainst = 0;
    let streak = 0, lastResult: "W" | "L" | null = null;

    for (let i = 0; i < games.length; i++) {
      const g = games[i];
      const isHome = g.homeTeamId === teamId;
      const won = isHome ? g.homeScore > g.awayScore : g.awayScore > g.homeScore;
      const pf = isHome ? g.homeScore : g.awayScore;
      const pa = isHome ? g.awayScore : g.homeScore;

      if (won) wins++;
      else losses++;
      pointsFor += pf;
      pointsAgainst += pa;

      if (i === 0) {
        lastResult = won ? "W" : "L";
        streak = 1;
      } else if (i < 10 && lastResult && ((won && lastResult === "W") || (!won && lastResult === "L"))) {
        streak++;
      }
    }

    const gp = games.length;
    result.set(teamId, {
      wins,
      losses,
      ppg: pointsFor / gp,
      oppg: pointsAgainst / gp,
      pace: null,
      rebPg: null,
      astPg: null,
      fg3Pct: null,
      ftPct: null,
      rankOff: null,
      rankDef: null,
      rankPace: null,
      streak: lastResult === "W" ? streak : -streak,
      lastGameDate: games[0]?.date ?? null,
    });
  }

  const allTeamStats = [...result.entries()].map(([id, snap]) => ({
    id,
    ppg: snap.ppg,
    oppg: snap.oppg,
  }));

  allTeamStats.sort((a, b) => b.ppg - a.ppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankOff = i + 1;
  });

  allTeamStats.sort((a, b) => a.oppg - b.oppg);
  allTeamStats.forEach((t, i) => {
    const snap = result.get(t.id);
    if (snap) snap.rankDef = i + 1;
  });

  return result;
}

function findMatchingPatterns(
  game: { id: string; homeTeamId: number; awayTeamId: number },
  homeSnap: TeamSnapshot | undefined,
  awaySnap: TeamSnapshot | undefined,
  patterns: { conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; confidenceScore: number | null }[],
  targetDate: Date,
): { conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; edge: number }[] {
  if (!homeSnap || !awaySnap) return [];

  const activeConditions = new Set<string>();

  if (homeSnap.rankOff != null && homeSnap.rankOff <= 10) activeConditions.add("HOME_TOP_10_OFF:home");
  if (homeSnap.rankDef != null && homeSnap.rankDef <= 10) activeConditions.add("HOME_TOP_10_DEF:home");
  if (awaySnap.rankOff != null && awaySnap.rankOff <= 10) activeConditions.add("AWAY_TOP_10_OFF:away");
  if (awaySnap.rankDef != null && awaySnap.rankDef <= 10) activeConditions.add("AWAY_TOP_10_DEF:away");

  if (homeSnap.rankOff != null && homeSnap.rankOff >= 21) activeConditions.add("HOME_BOTTOM_10_OFF:home");
  if (homeSnap.rankDef != null && homeSnap.rankDef >= 21) activeConditions.add("HOME_BOTTOM_10_DEF:home");
  if (awaySnap.rankOff != null && awaySnap.rankOff >= 21) activeConditions.add("AWAY_BOTTOM_10_OFF:away");
  if (awaySnap.rankDef != null && awaySnap.rankDef >= 21) activeConditions.add("AWAY_BOTTOM_10_DEF:away");

  const homeWinPct = homeSnap.wins / (homeSnap.wins + homeSnap.losses);
  const awayWinPct = awaySnap.wins / (awaySnap.wins + awaySnap.losses);
  if (homeWinPct >= 0.6) activeConditions.add("HOME_WINNING_RECORD:home");
  if (awayWinPct >= 0.6) activeConditions.add("AWAY_WINNING_RECORD:away");
  if (homeWinPct < 0.4) activeConditions.add("HOME_LOSING_RECORD:home");
  if (awayWinPct < 0.4) activeConditions.add("AWAY_LOSING_RECORD:away");

  if (homeSnap.streak >= 3) activeConditions.add("HOME_HOT_STREAK:home");
  if (homeSnap.streak <= -3) activeConditions.add("HOME_COLD_STREAK:home");
  if (awaySnap.streak >= 3) activeConditions.add("AWAY_HOT_STREAK:away");
  if (awaySnap.streak <= -3) activeConditions.add("AWAY_COLD_STREAK:away");

  const homeRestDays = homeSnap.lastGameDate
    ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;
  const awayRestDays = awaySnap.lastGameDate
    ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;

  if (homeRestDays === 0) activeConditions.add("HOME_B2B:home");
  if (awayRestDays === 0) activeConditions.add("AWAY_B2B:away");
  if (homeRestDays != null && homeRestDays >= 2) activeConditions.add("HOME_RESTED:home");
  if (awayRestDays != null && awayRestDays >= 2) activeConditions.add("AWAY_RESTED:away");

  const matched: { conditions: string[]; outcome: string; hitRate: number; hitCount: number; sampleSize: number; seasons: number; edge: number }[] = [];

  for (const pattern of patterns) {
    if (pattern.conditions.every((c) => activeConditions.has(c))) {
      matched.push({
        conditions: pattern.conditions,
        outcome: pattern.outcome,
        hitRate: pattern.hitRate,
        hitCount: pattern.hitCount,
        sampleSize: pattern.sampleSize,
        seasons: pattern.seasons,
        edge: (pattern.hitRate - 0.524) * 100,
      });
    }
  }

  matched.sort((a, b) => (b.hitRate - a.hitRate) || (b.sampleSize - a.sampleSize));
  return matched;
}
