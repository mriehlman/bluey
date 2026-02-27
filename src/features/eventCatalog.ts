import type { Game, Team, Player, PlayerGameStat } from "@prisma/client";

export interface NightContext {
  date: string;
  season: number;
  games: (Game & { homeTeam: Team; awayTeam: Team })[];
  stats: (PlayerGameStat & { player: Player })[];
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
      const homeWins = ctx.games.filter((g) => g.homeScore > g.awayScore).length;
      return { hit: homeWins >= 5, meta: { homeWins } };
    },
  },

  {
    key: "ANY_TEAM_130_PLUS",
    compute(ctx) {
      const teams: number[] = [];
      let max = 0;
      for (const g of ctx.games) {
        if (g.homeScore >= 130) teams.push(g.homeTeamId);
        if (g.awayScore >= 130) teams.push(g.awayTeamId);
        max = Math.max(max, g.homeScore, g.awayScore);
      }
      return { hit: teams.length > 0, meta: { teamIds: [...new Set(teams)], maxScore: max } };
    },
  },

  {
    key: "ANY_GAME_TOTAL_260_PLUS",
    compute(ctx) {
      let maxTotal = 0;
      const gameIds: string[] = [];
      for (const g of ctx.games) {
        const total = g.homeScore + g.awayScore;
        if (total >= 260) gameIds.push(g.id);
        maxTotal = Math.max(maxTotal, total);
      }
      return { hit: gameIds.length > 0, meta: { gameIds, maxTotal } };
    },
  },

  {
    key: "TRIPLE_DOUBLE_EXISTS",
    compute(ctx) {
      const playerIds: number[] = [];
      for (const s of ctx.stats) {
        const cats = [s.points, s.rebounds, s.assists, s.steals, s.blocks];
        const over10 = cats.filter((v) => v >= 10).length;
        if (over10 >= 3) playerIds.push(s.playerId);
      }
      const unique = [...new Set(playerIds)];
      return { hit: unique.length > 0, meta: { playerIds: unique, count: unique.length } };
    },
  },

  {
    key: "TWO_30PT_SCORERS",
    compute(ctx) {
      const scorers = ctx.stats.filter((s) => s.points >= 30);
      const uniqueIds = [...new Set(scorers.map((s) => s.playerId))];
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  {
    key: "PLAYER_40_PLUS_POINTS",
    compute(ctx) {
      const scorers = ctx.stats.filter((s) => s.points >= 40);
      const uniqueIds = [...new Set(scorers.map((s) => s.playerId))];
      let maxPts = 0;
      for (const s of scorers) maxPts = Math.max(maxPts, s.points);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxPoints: maxPts } };
    },
  },

  {
    key: "THREE_TEAMS_HAVE_3x20PT_SCORERS",
    compute(ctx) {
      const teamScorers = new Map<number, Set<number>>();
      for (const s of ctx.stats) {
        if (s.points >= 20) {
          let set = teamScorers.get(s.teamId);
          if (!set) {
            set = new Set();
            teamScorers.set(s.teamId, set);
          }
          set.add(s.playerId);
        }
      }
      const qualifying: number[] = [];
      for (const [teamId, players] of teamScorers) {
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
      const teamReb = new Map<number, number>();
      for (const s of ctx.stats) {
        teamReb.set(s.teamId, (teamReb.get(s.teamId) ?? 0) + s.rebounds);
      }
      const qualifying: { teamId: number; rebounds: number }[] = [];
      for (const [teamId, reb] of teamReb) {
        if (reb >= 60) qualifying.push({ teamId, rebounds: reb });
      }
      return {
        hit: qualifying.length > 0,
        meta: { teams: qualifying, maxRebounds: Math.max(...[...teamReb.values()], 0) },
      };
    },
  },

  {
    key: "ANY_PLAYER_15_PLUS_ASSISTS",
    compute(ctx) {
      const players = ctx.stats.filter((s) => s.assists >= 15);
      const uniqueIds = [...new Set(players.map((s) => s.playerId))];
      let maxAst = 0;
      for (const s of players) maxAst = Math.max(maxAst, s.assists);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxAssists: maxAst } };
    },
  },

  {
    key: "ANY_PLAYER_20_PLUS_REBOUNDS",
    compute(ctx) {
      const players = ctx.stats.filter((s) => s.rebounds >= 20);
      const uniqueIds = [...new Set(players.map((s) => s.playerId))];
      let maxReb = 0;
      for (const s of players) maxReb = Math.max(maxReb, s.rebounds);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxRebounds: maxReb } };
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
      const teams: number[] = [];
      for (const g of ctx.games) {
        if (g.homeScore >= 130) teams.push(g.homeTeamId);
        if (g.awayScore >= 130) teams.push(g.awayTeamId);
      }
      const unique = [...new Set(teams)];
      return { hit: unique.length >= 2, meta: { teamIds: unique, count: unique.length } };
    },
  },

  {
    key: "THREE_GAMES_TOTAL_240_PLUS",
    compute(ctx) {
      const qualifying: string[] = [];
      for (const g of ctx.games) {
        if (g.homeScore + g.awayScore >= 240) qualifying.push(g.id);
      }
      return { hit: qualifying.length >= 3, meta: { gameIds: qualifying, count: qualifying.length } };
    },
  },

  {
    key: "TWO_PLAYERS_40_PLUS",
    compute(ctx) {
      const scorers = ctx.stats.filter((s) => s.points >= 40);
      const uniqueIds = [...new Set(scorers.map((s) => s.playerId))];
      return { hit: uniqueIds.length >= 2, meta: { playerIds: uniqueIds, count: uniqueIds.length } };
    },
  },

  {
    key: "ANY_PLAYER_10_PLUS_TURNOVERS",
    compute(ctx) {
      const players = ctx.stats.filter((s) => s.turnovers >= 10);
      const uniqueIds = [...new Set(players.map((s) => s.playerId))];
      let maxTov = 0;
      for (const s of players) maxTov = Math.max(maxTov, s.turnovers);
      return { hit: uniqueIds.length > 0, meta: { playerIds: uniqueIds, maxTurnovers: maxTov } };
    },
  },

  {
    key: "TEAM_POINTS_GE_140",
    compute(ctx) {
      const teams: number[] = [];
      let maxScore = 0;
      for (const g of ctx.games) {
        if (g.homeScore >= 140) teams.push(g.homeTeamId);
        if (g.awayScore >= 140) teams.push(g.awayTeamId);
        maxScore = Math.max(maxScore, g.homeScore, g.awayScore);
      }
      const unique = [...new Set(teams)];
      return { hit: unique.length > 0, meta: { teamIds: unique, maxScore } };
    },
  },

  {
    key: "TEAM_ASSISTS_GE_35",
    compute(ctx) {
      const teamAst = new Map<number, number>();
      for (const s of ctx.stats) {
        teamAst.set(s.teamId, (teamAst.get(s.teamId) ?? 0) + s.assists);
      }
      const qualifying: { teamId: number; assists: number }[] = [];
      for (const [teamId, ast] of teamAst) {
        if (ast >= 35) qualifying.push({ teamId, assists: ast });
      }
      return {
        hit: qualifying.length > 0,
        meta: { teams: qualifying, maxAssists: Math.max(...[...teamAst.values()], 0) },
      };
    },
  },

  {
    key: "TEAM_BLOCKS_GE_12",
    compute(ctx) {
      const teamBlk = new Map<number, number>();
      for (const s of ctx.stats) {
        teamBlk.set(s.teamId, (teamBlk.get(s.teamId) ?? 0) + s.blocks);
      }
      const qualifying: { teamId: number; blocks: number }[] = [];
      for (const [teamId, blk] of teamBlk) {
        if (blk >= 12) qualifying.push({ teamId, blocks: blk });
      }
      return {
        hit: qualifying.length > 0,
        meta: { teams: qualifying, maxBlocks: Math.max(...[...teamBlk.values()], 0) },
      };
    },
  },

  {
    key: "BLOWOUT_25_PLUS",
    compute(ctx) {
      const blowouts: { gameId: string; margin: number }[] = [];
      for (const g of ctx.games) {
        const margin = Math.abs(g.homeScore - g.awayScore);
        if (margin >= 25) blowouts.push({ gameId: g.id, margin });
      }
      return {
        hit: blowouts.length > 0,
        meta: { games: blowouts, count: blowouts.length, maxMargin: Math.max(...blowouts.map((b) => b.margin), 0) },
      };
    },
  },
];
