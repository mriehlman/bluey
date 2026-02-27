import type { Game, Team, Player, PlayerGameStat } from "@prisma/client";
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
];
