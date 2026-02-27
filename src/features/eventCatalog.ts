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
  {
    key: "SLATE_GAMES_GE_7",
    compute(ctx) {
      const count = ctx.games.length;
      return { hit: count >= 7, meta: { gameCount: count } };
    },
  },

  {
    key: "HOME_TEAMS_WIN_GE_5",
    compute(ctx) {
      const { count } = countGames(ctx, (g) => g.homeScore > g.awayScore);
      return { hit: count >= 5, meta: { homeWins: count } };
    },
  },

  {
    key: "ANY_TEAM_130_PLUS",
    compute(ctx) {
      const { teamIds, maxScore } = teamsScoring(ctx, 130);
      return { hit: teamIds.length > 0, meta: { teamIds, maxScore } };
    },
  },

  {
    key: "ANY_GAME_TOTAL_260_PLUS",
    compute(ctx) {
      const { count, matching } = countGames(ctx, (g) => g.homeScore + g.awayScore >= 260);
      const maxTotal = Math.max(...ctx.games.map((g) => g.homeScore + g.awayScore), 0);
      return { hit: count > 0, meta: { gameIds: matching.map((g) => g.id), maxTotal } };
    },
  },

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

  {
    key: "TEAM_WITH_60_REBOUNDS",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.rebounds >= 60);
      const allAggs = ctx.teamAggregates ?? matching;
      return {
        hit: matching.length > 0,
        meta: {
          teams: matching.map((a) => ({ teamId: a.teamId, rebounds: a.rebounds })),
          maxRebounds: Math.max(...allAggs.map((a) => a.rebounds), 0),
        },
      };
    },
  },

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

  {
    key: "SLATE_GAMES_LE_4",
    compute(ctx) {
      const count = ctx.games.length;
      return { hit: count <= 4 && count > 0, meta: { gameCount: count } };
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
    key: "TWO_TEAMS_130_PLUS",
    compute(ctx) {
      const { teamIds } = teamsScoring(ctx, 130);
      return { hit: teamIds.length >= 2, meta: { teamIds, count: teamIds.length } };
    },
  },

  {
    key: "THREE_GAMES_TOTAL_240_PLUS",
    compute(ctx) {
      const { count, matching } = countGames(ctx, (g) => g.homeScore + g.awayScore >= 240);
      return { hit: count >= 3, meta: { gameIds: matching.map((g) => g.id), count } };
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
    key: "ANY_PLAYER_10_PLUS_TURNOVERS",
    compute(ctx) {
      const { uniqueIds, matching } = countPlayers(ctx, (s) => s.turnovers >= 10);
      const maxTurnovers = Math.max(...matching.map((s) => s.turnovers), 0);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxTurnovers } };
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
    key: "TEAM_ASSISTS_GE_35",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.assists >= 35);
      const allAggs = ctx.teamAggregates ?? matching;
      return {
        hit: matching.length > 0,
        meta: {
          teams: matching.map((a) => ({ teamId: a.teamId, assists: a.assists })),
          maxAssists: Math.max(...allAggs.map((a) => a.assists), 0),
        },
      };
    },
  },

  {
    key: "TEAM_BLOCKS_GE_12",
    compute(ctx) {
      const { matching } = anyTeam(ctx, (a) => a.blocks >= 12);
      const allAggs = ctx.teamAggregates ?? matching;
      return {
        hit: matching.length > 0,
        meta: {
          teams: matching.map((a) => ({ teamId: a.teamId, blocks: a.blocks })),
          maxBlocks: Math.max(...allAggs.map((a) => a.blocks), 0),
        },
      };
    },
  },

  {
    key: "BLOWOUT_25_PLUS",
    compute(ctx) {
      const { matching } = anyGame(ctx, (g) => Math.abs(g.homeScore - g.awayScore) >= 25);
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
];
