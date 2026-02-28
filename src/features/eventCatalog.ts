import type { Game, Team, Player, PlayerGameStat, GameOdds } from "@prisma/client";
import {
  anyPlayer,
  countPlayers,
  anyGame,
  countGames,
  anyTeam,
  countTeams,
  teamsScoring,
  perTeamPlayerCount,
  withScoredGames,
} from "./eventDsl.js";

export interface TeamAgg {
  teamId: number;
  points: number;
  rebounds: number;
  assists: number;
  steals: number;
  blocks: number;
  turnovers: number;
  minutes: number;
}

export interface NightContext {
  date: string;
  season: number;
  games: (Game & { homeTeam: Team; awayTeam: Team })[];
  stats: (PlayerGameStat & { player: Player })[];
  teamAggregates?: TeamAgg[];
  gameOdds?: GameOdds[];
}

export interface EventResult {
  hit: boolean;
  meta?: Record<string, unknown>;
}

export interface EventDef {
  key: string;
  compute: (ctx: NightContext) => EventResult;
}

export const CATALOG: EventDef[] = [
  // ── Slate structure ──

  {
    key: "SLATE_GAMES_GE_7",
    compute(ctx) {
      const count = ctx.games.length;
      return { hit: count >= 7, meta: { gameCount: count } };
    },
  },
  {
    key: "SLATE_GAMES_BETWEEN_5_6",
    compute(ctx) {
      const count = ctx.games.length;
      return { hit: count === 5 || count === 6, meta: { gameCount: count } };
    },
  },
  {
    key: "SLATE_GAMES_LE_4",
    compute(ctx) {
      const count = ctx.games.length;
      return { hit: count <= 4 && count > 0, meta: { gameCount: count } };
    },
  },

  // ── Home / road ──

  {
    key: "HOME_TEAMS_WIN_GE_5",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count } = countGames(sCtx, (g) => g.homeScore > g.awayScore);
      return { hit: count >= 5, meta: { homeWins: count } };
    },
  },
  {
    key: "ROAD_TEAMS_WIN_GE_5",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count } = countGames(sCtx, (g) => g.awayScore > g.homeScore);
      return { hit: count >= 5, meta: { roadWins: count } };
    },
  },

  // ── Close games ──

  {
    key: "GAMES_DECIDED_BY_5_GE_3",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(
        sCtx,
        (g) => Math.abs(g.homeScore - g.awayScore) <= 5,
      );
      return { hit: count >= 3, meta: { count, gameIds: matching.map((g) => g.id) } };
    },
  },
  {
    key: "GAMES_DECIDED_BY_3_GE_2",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(
        sCtx,
        (g) => Math.abs(g.homeScore - g.awayScore) <= 3,
      );
      return { hit: count >= 2, meta: { count, gameIds: matching.map((g) => g.id) } };
    },
  },

  // ── Team scoring tiers (score-based, uses Game rows) ──

  {
    key: "ANY_TEAM_130_PLUS",
    compute(ctx) {
      const { teamIds, maxScore } = teamsScoring(ctx, 130);
      return { hit: teamIds.length > 0, meta: { teamIds, maxScore } };
    },
  },
  {
    key: "TWO_TEAMS_130_PLUS",
    compute(ctx) {
      const { teamIds } = teamsScoring(ctx, 130);
      return { hit: teamIds.length >= 2, meta: { teamIds, count: teamIds.length } };
    },
  },
  {
    key: "TEAM_POINTS_GE_140",
    compute(ctx) {
      const { teamIds, maxScore } = teamsScoring(ctx, 140);
      return { hit: teamIds.length > 0, meta: { teamIds, maxScore } };
    },
  },
  {
    key: "AT_LEAST_4_TEAMS_120_PLUS",
    compute(ctx) {
      const { teamIds } = teamsScoring(ctx, 120);
      return { hit: teamIds.length >= 4, meta: { teamIds, count: teamIds.length } };
    },
  },
  {
    key: "AT_LEAST_4_TEAMS_UNDER_100",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const under100: number[] = [];
      for (const g of sCtx.games) {
        if (g.homeScore < 100) under100.push(g.homeTeamId);
        if (g.awayScore < 100) under100.push(g.awayTeamId);
      }
      const unique = [...new Set(under100)];
      return { hit: unique.length >= 4, meta: { teamIds: unique, count: unique.length } };
    },
  },
  {
    key: "NO_TEAM_120_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      if (sCtx.games.length === 0) return { hit: false };
      const { teamIds, maxScore } = teamsScoring(ctx, 120);
      return { hit: teamIds.length === 0, meta: { maxScore } };
    },
  },

  // ── Game total tiers (score-based) ──

  {
    key: "ANY_GAME_TOTAL_250_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore + g.awayScore >= 250);
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), count } };
    },
  },
  {
    key: "ANY_GAME_TOTAL_260_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore + g.awayScore >= 260);
      const maxTotal = Math.max(...sCtx.games.map((g) => g.homeScore + g.awayScore), 0);
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), maxTotal } };
    },
  },
  {
    key: "ANY_GAME_TOTAL_270_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore + g.awayScore >= 270);
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), count } };
    },
  },
  {
    key: "THREE_GAMES_TOTAL_240_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore + g.awayScore >= 240);
      return { hit: count >= 3, meta: { gameIds: matching.map((g) => g.id), count } };
    },
  },

  // ── Blowout tiers (score-based) ──

  {
    key: "BLOWOUT_25_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { matching } = anyGame(sCtx, (g) => Math.abs(g.homeScore - g.awayScore) >= 25);
      const blowouts = matching.map((g) => ({
        gameId: g.id,
        margin: Math.abs(g.homeScore - g.awayScore),
      }));
      return {
        hit: blowouts.length > 0,
        meta: { games: blowouts, count: blowouts.length, maxMargin: Math.max(...blowouts.map((b) => b.margin), 0) },
      };
    },
  },
  {
    key: "BLOWOUTS_20_PLUS_GE_3",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => Math.abs(g.homeScore - g.awayScore) >= 20);
      return { hit: count >= 3, meta: { count, gameIds: matching.map((g) => g.id) } };
    },
  },
  {
    key: "BLOWOUTS_25_PLUS_GE_2",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => Math.abs(g.homeScore - g.awayScore) >= 25);
      return { hit: count >= 2, meta: { count, gameIds: matching.map((g) => g.id) } };
    },
  },

  // ── Player scoring ──

  {
    key: "TWO_30PT_SCORERS",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => s.points >= 30);
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },
  {
    key: "PLAYER_40_PLUS_POINTS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.points >= 40);
      const maxPoints = Math.max(...matching.map((s) => s.points), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxPoints } };
    },
  },
  {
    key: "TWO_PLAYERS_40_PLUS",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => s.points >= 40);
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },
  {
    key: "THREE_TEAMS_HAVE_3x20PT_SCORERS",
    compute(ctx) {
      const teamMap = perTeamPlayerCount(ctx, (s) => s.points >= 20);
      const qualifying: number[] = [];
      for (const [teamId, players] of teamMap) {
        if (players.size >= 3) qualifying.push(teamId);
      }
      return {
        hit: qualifying.length >= 3,
        meta: { teamIds: qualifying, count: qualifying.length },
      };
    },
  },

  // ── Triple-double ──

  {
    key: "TRIPLE_DOUBLE_EXISTS",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => {
        const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
        return cats.filter((v) => v >= 10).length >= 3;
      });
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  // ── Player assists / rebounds ──

  {
    key: "ANY_PLAYER_15_PLUS_ASSISTS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.assists >= 15);
      const maxAssists = Math.max(...matching.map((s) => s.assists), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxAssists } };
    },
  },
  {
    key: "ANY_PLAYER_20_PLUS_REBOUNDS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.rebounds >= 20);
      const maxRebounds = Math.max(...matching.map((s) => s.rebounds), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxRebounds } };
    },
  },

  // ── Turnover pressure ──

  {
    key: "ANY_PLAYER_10_PLUS_TURNOVERS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.turnovers >= 10);
      const maxTurnovers = Math.max(...matching.map((s) => s.turnovers), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxTurnovers } };
    },
  },
  {
    key: "TWO_PLAYERS_8_PLUS_TURNOVERS",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => s.turnovers >= 8);
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },
  {
    key: "ANY_TEAM_TURNOVERS_GE_22",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.turnovers >= 22);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, turnovers: a.turnovers })) },
      };
    },
  },

  // ── Team rebounds tiers (aggregate-based) ──

  {
    key: "TEAM_WITH_58_REBOUNDS",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.rebounds >= 58);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, rebounds: a.rebounds })) },
      };
    },
  },
  {
    key: "TEAM_WITH_60_REBOUNDS",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.rebounds >= 60);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, rebounds: a.rebounds })) },
      };
    },
  },
  {
    key: "TEAM_WITH_62_REBOUNDS",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.rebounds >= 62);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, rebounds: a.rebounds })) },
      };
    },
  },

  // ── Team assists / blocks (aggregate-based) ──

  {
    key: "TEAM_ASSISTS_GE_35",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.assists >= 35);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, assists: a.assists })) },
      };
    },
  },
  {
    key: "TEAM_BLOCKS_GE_12",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.blocks >= 12);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, blocks: a.blocks })) },
      };
    },
  },

  // ── Defense / disruption (aggregate-based) ──

  {
    key: "TEAM_STEALS_GE_15",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.steals >= 15);
      return {
        hit: matching.length > 0,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, steals: a.steals })) },
      };
    },
  },
  {
    key: "TWO_TEAMS_BLOCKS_GE_10",
    compute(ctx) {
      const { count, matching } = countTeams(ctx, (a) => a.blocks >= 10);
      return {
        hit: count >= 2,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, blocks: a.blocks })), count },
      };
    },
  },

  // ── Slate pace ──

  {
    key: "SLATE_AVG_TOTAL_GE_225",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      if (sCtx.games.length === 0) return { hit: false };
      const totals = sCtx.games.map((g) => g.homeScore + g.awayScore);
      const avg = totals.reduce((a, b) => a + b, 0) / totals.length;
      return { hit: avg >= 225, meta: { avgTotal: Math.round(avg * 10) / 10, gameCount: sCtx.games.length } };
    },
  },
  {
    key: "SLATE_AVG_TOTAL_LE_205",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      if (sCtx.games.length === 0) return { hit: false };
      const totals = sCtx.games.map((g) => g.homeScore + g.awayScore);
      const avg = totals.reduce((a, b) => a + b, 0) / totals.length;
      return { hit: avg <= 205, meta: { avgTotal: Math.round(avg * 10) / 10, gameCount: sCtx.games.length } };
    },
  },

  // ── Double-doubles ──

  {
    key: "DOUBLE_DOUBLE_GE_5",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => {
        const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
        return cats.filter((v) => v >= 10).length >= 2;
      });
      return { hit: uniqueIds.length >= 5, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  // ── Player dominance ──

  {
    key: "ANY_PLAYER_25_10_GAME",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) =>
        s.points >= 25 && (s.rebounds >= 10 || s.assists >= 10),
      );
      const lines = matching.map((s) => ({
        playerId: s.playerId,
        points: s.points,
        rebounds: s.rebounds,
        assists: s.assists,
      }));
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, lines } };
    },
  },

  // ── Scoring extremes / absence ──

  {
    key: "PLAYER_50_PLUS_POINTS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.points >= 50);
      const maxPoints = Math.max(...matching.map((s) => s.points), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxPoints } };
    },
  },
  {
    key: "NO_30PT_SCORER",
    compute(ctx) {
      if (ctx.stats.length === 0) return { hit: false };
      const { uniqueIds } = countPlayers(ctx, (s) => s.points >= 30);
      const maxPoints = Math.max(...ctx.stats.map((s) => s.points), 0);
      return { hit: uniqueIds.length === 0, meta: { maxPoints } };
    },
  },
  {
    key: "FIVE_25PT_SCORERS",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => s.points >= 25);
      return { hit: uniqueIds.length >= 5, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  // ── Competitive high-scoring / low-scoring ──

  {
    key: "BOTH_TEAMS_110_PLUS_IN_GAME",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore >= 110 && g.awayScore >= 110);
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), count } };
    },
  },
  {
    key: "ANY_GAME_TOTAL_UNDER_190",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      const { count, matching } = countGames(sCtx, (g) => g.homeScore + g.awayScore < 190);
      const minTotal = sCtx.games.length > 0
        ? Math.min(...sCtx.games.map((g) => g.homeScore + g.awayScore))
        : 0;
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), count, minTotal } };
    },
  },

  // ── Game competitiveness distribution ──

  {
    key: "ALL_GAMES_DECIDED_BY_10_PLUS",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      if (sCtx.games.length === 0) return { hit: false };
      const allBlowouts = sCtx.games.every((g) => Math.abs(g.homeScore - g.awayScore) >= 10);
      const minMargin = Math.min(...sCtx.games.map((g) => Math.abs(g.homeScore - g.awayScore)));
      return { hit: allBlowouts, meta: { gameCount: sCtx.games.length, minMargin } };
    },
  },
  {
    key: "NO_BLOWOUT_15",
    compute(ctx) {
      const sCtx = withScoredGames(ctx);
      if (sCtx.games.length === 0) return { hit: false };
      const allClose = sCtx.games.every((g) => Math.abs(g.homeScore - g.awayScore) <= 15);
      const maxMargin = Math.max(...sCtx.games.map((g) => Math.abs(g.homeScore - g.awayScore)), 0);
      return { hit: allClose, meta: { gameCount: sCtx.games.length, maxMargin } };
    },
  },

  // ── Balanced / deep offense ──

  {
    key: "TEAM_6_PLAYERS_DOUBLE_FIGURES",
    compute(ctx) {
      const teamMap = perTeamPlayerCount(ctx, (s) => s.points >= 10);
      const qualifying: { teamId: number; count: number }[] = [];
      for (const [teamId, players] of teamMap) {
        if (players.size >= 6) qualifying.push({ teamId, count: players.size });
      }
      return {
        hit: qualifying.length > 0,
        meta: { teams: qualifying, teamCount: qualifying.length },
      };
    },
  },

  // ── Multiple triple-doubles ──

  {
    key: "MULTIPLE_TRIPLE_DOUBLES",
    compute(ctx) {
      const { uniqueIds } = countPlayers(ctx, (s) => {
        const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
        return cats.filter((v) => v >= 10).length >= 3;
      });
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  // ── Individual defense ──

  {
    key: "ANY_PLAYER_5_PLUS_STEALS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.steals >= 5);
      const maxSteals = Math.max(...matching.map((s) => s.steals), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxSteals } };
    },
  },
  {
    key: "ANY_PLAYER_5_PLUS_BLOCKS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.blocks >= 5);
      const maxBlocks = Math.max(...matching.map((s) => s.blocks), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxBlocks } };
    },
  },

  // ── Ball movement breadth ──

  {
    key: "TWO_TEAMS_ASSISTS_GE_30",
    compute(ctx) {
      const { count, matching } = countTeams(ctx, (a) => a.assists >= 30);
      return {
        hit: count >= 2,
        meta: { teams: matching.map((a) => ({ teamId: a.teamId, assists: a.assists })), count },
      };
    },
  },

  // ── Minutes extremes ──

  {
    key: "ANY_PLAYER_45_PLUS_MINUTES",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.minutes >= 45);
      const maxMinutes = Math.max(...matching.map((s) => s.minutes), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxMinutes } };
    },
  },

  // ── Shooting splits (requires expanded stat fields) ──

  {
    key: "ANY_PLAYER_10_PLUS_THREES",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => (s.fg3m ?? 0) >= 10);
      const max = Math.max(...matching.map((s) => s.fg3m ?? 0), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxFg3m: max } };
    },
  },
  {
    key: "ANY_PLAYER_PERFECT_FT_10_PLUS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(
        ctx,
        (s) => (s.ftm ?? 0) >= 10 && s.ftm != null && s.fta != null && s.ftm === s.fta,
      );
      const lines = matching.map((s) => ({ playerId: s.playerId, ftm: s.ftm, fta: s.fta }));
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, lines } };
    },
  },
  {
    key: "TEAM_FT_PCT_UNDER_60",
    compute(ctx) {
      const teamFt = new Map<number, { ftm: number; fta: number }>();
      for (const s of ctx.stats) {
        if (s.ftm == null || s.fta == null) continue;
        let t = teamFt.get(s.teamId);
        if (!t) { t = { ftm: 0, fta: 0 }; teamFt.set(s.teamId, t); }
        t.ftm += s.ftm;
        t.fta += s.fta;
      }
      const qualifying: { teamId: number; ftPct: number }[] = [];
      for (const [teamId, t] of teamFt) {
        if (t.fta >= 10) {
          const pct = t.ftm / t.fta;
          if (pct < 0.6) qualifying.push({ teamId, ftPct: Math.round(pct * 1000) / 10 });
        }
      }
      return { hit: qualifying.length > 0, meta: { teams: qualifying } };
    },
  },
  {
    key: "TEAM_3PT_PCT_GE_45",
    compute(ctx) {
      const team3pt = new Map<number, { fg3m: number; fg3a: number }>();
      for (const s of ctx.stats) {
        if (s.fg3m == null || s.fg3a == null) continue;
        let t = team3pt.get(s.teamId);
        if (!t) { t = { fg3m: 0, fg3a: 0 }; team3pt.set(s.teamId, t); }
        t.fg3m += s.fg3m;
        t.fg3a += s.fg3a;
      }
      const qualifying: { teamId: number; fg3Pct: number }[] = [];
      for (const [teamId, t] of team3pt) {
        if (t.fg3a >= 10) {
          const pct = t.fg3m / t.fg3a;
          if (pct >= 0.45) qualifying.push({ teamId, fg3Pct: Math.round(pct * 1000) / 10 });
        }
      }
      return { hit: qualifying.length > 0, meta: { teams: qualifying } };
    },
  },

  // ── Odds-based events (requires GameOdds data) ──

  {
    key: "GAME_SPREAD_COVERED",
    compute(ctx) {
      if (!ctx.gameOdds || ctx.gameOdds.length === 0) return { hit: false };
      const oddsMap = new Map<string, typeof ctx.gameOdds[number]>();
      for (const o of ctx.gameOdds) {
        if (o.source === "consensus") oddsMap.set(o.gameId, o);
      }
      if (oddsMap.size === 0) {
        for (const o of ctx.gameOdds) oddsMap.set(o.gameId, o);
      }

      const covered: { gameId: string; margin: number; spread: number }[] = [];
      for (const g of ctx.games) {
        const odds = oddsMap.get(g.id);
        if (!odds || odds.spreadHome == null) continue;
        const actualMargin = g.homeScore - g.awayScore;
        if (actualMargin > odds.spreadHome) {
          covered.push({ gameId: g.id, margin: actualMargin, spread: odds.spreadHome });
        }
      }
      return { hit: covered.length > 0, meta: { covered, count: covered.length } };
    },
  },
  {
    key: "GAME_WENT_OVER",
    compute(ctx) {
      if (!ctx.gameOdds || ctx.gameOdds.length === 0) return { hit: false };
      const oddsMap = new Map<string, typeof ctx.gameOdds[number]>();
      for (const o of ctx.gameOdds) {
        if (o.source === "consensus") oddsMap.set(o.gameId, o);
      }
      if (oddsMap.size === 0) {
        for (const o of ctx.gameOdds) oddsMap.set(o.gameId, o);
      }

      const overs: { gameId: string; total: number; line: number }[] = [];
      for (const g of ctx.games) {
        const odds = oddsMap.get(g.id);
        if (!odds || odds.totalOver == null) continue;
        const actualTotal = g.homeScore + g.awayScore;
        if (actualTotal > odds.totalOver) {
          overs.push({ gameId: g.id, total: actualTotal, line: odds.totalOver });
        }
      }
      return { hit: overs.length > 0, meta: { overs, count: overs.length } };
    },
  },
];
