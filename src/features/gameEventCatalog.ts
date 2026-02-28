import type { Game, Team, GameContext, PlayerGameContext, PlayerGameStat, GameOdds } from "@prisma/client";

export interface GameEventContext {
  game: Game & { homeTeam: Team; awayTeam: Team };
  context: GameContext;
  playerContexts: PlayerGameContext[];
  stats: PlayerGameStat[];
  odds: GameOdds | null;
}

export interface GameEventDef {
  key: string;
  type: "condition" | "outcome";
  sides: ("home" | "away" | "game")[];
  compute: (ctx: GameEventContext, side: string) => { hit: boolean; meta?: Record<string, unknown> };
}

function getRankOff(ctx: GameEventContext, side: string): number | null {
  return side === "home" ? ctx.context.homeRankOff : ctx.context.awayRankOff;
}

function getRankDef(ctx: GameEventContext, side: string): number | null {
  return side === "home" ? ctx.context.homeRankDef : ctx.context.awayRankDef;
}

function getRankPace(ctx: GameEventContext, side: string): number | null {
  return side === "home" ? ctx.context.homeRankPace : ctx.context.awayRankPace;
}

function getStreak(ctx: GameEventContext, side: string): number | null {
  return side === "home" ? ctx.context.homeStreak : ctx.context.awayStreak;
}

function getWins(ctx: GameEventContext, side: string): number {
  return side === "home" ? ctx.context.homeWins : ctx.context.awayWins;
}

function getLosses(ctx: GameEventContext, side: string): number {
  return side === "home" ? ctx.context.homeLosses : ctx.context.awayLosses;
}

function getIsB2b(ctx: GameEventContext, side: string): boolean {
  return side === "home" ? ctx.context.homeIsB2b : ctx.context.awayIsB2b;
}

function getRestDays(ctx: GameEventContext, side: string): number | null {
  return side === "home" ? ctx.context.homeRestDays : ctx.context.awayRestDays;
}

function getPpg(ctx: GameEventContext, side: string): number {
  return side === "home" ? ctx.context.homePpg : ctx.context.awayPpg;
}

function getOppg(ctx: GameEventContext, side: string): number {
  return side === "home" ? ctx.context.homeOppg : ctx.context.awayOppg;
}

const SIDES_PER_TEAM: ("home" | "away")[] = ["home", "away"];
const SIDES_GAME: "game"[] = ["game"];

// ── CONDITION EVENTS ──

const conditionEvents: GameEventDef[] = [
  // Offense tiers (per-side)
  { key: "TOP_5_OFF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankOff(ctx, side) ?? 99) <= 5 }) },
  { key: "TOP_10_OFF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankOff(ctx, side) ?? 99) <= 10 }) },
  { key: "BOTTOM_10_OFF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankOff(ctx, side) ?? 0) >= 21 }) },
  { key: "BOTTOM_5_OFF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankOff(ctx, side) ?? 0) >= 26 }) },

  // Defense tiers (per-side)
  { key: "TOP_5_DEF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankDef(ctx, side) ?? 99) <= 5 }) },
  { key: "TOP_10_DEF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankDef(ctx, side) ?? 99) <= 10 }) },
  { key: "BOTTOM_10_DEF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankDef(ctx, side) ?? 0) >= 21 }) },
  { key: "BOTTOM_5_DEF", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: (getRankDef(ctx, side) ?? 0) >= 26 }) },

  // Pace (game-wide)
  {
    key: "BOTH_TOP_10_PACE", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => ({
      hit: (ctx.context.homeRankPace ?? 99) <= 10 && (ctx.context.awayRankPace ?? 99) <= 10,
    }),
  },
  {
    key: "BOTH_BOTTOM_10_PACE", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => ({
      hit: (ctx.context.homeRankPace ?? 0) >= 21 && (ctx.context.awayRankPace ?? 0) >= 21,
    }),
  },
  {
    key: "PACE_MISMATCH", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      const hp = ctx.context.homeRankPace;
      const ap = ctx.context.awayRankPace;
      if (hp == null || ap == null) return { hit: false };
      return { hit: Math.abs(hp - ap) >= 15 };
    },
  },

  // Schedule (per-side)
  { key: "ON_B2B", type: "condition", sides: SIDES_PER_TEAM, compute: (ctx, side) => ({ hit: getIsB2b(ctx, side) }) },
  {
    key: "RESTED_3_PLUS", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => {
      const rest = getRestDays(ctx, side);
      return { hit: rest != null && rest >= 3 };
    },
  },
  {
    key: "BOTH_ON_B2B", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.context.homeIsB2b && ctx.context.awayIsB2b }),
  },

  // Records/streaks (per-side)
  {
    key: "WINNING_RECORD", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => ({ hit: getWins(ctx, side) > getLosses(ctx, side) }),
  },
  {
    key: "WIN_STREAK_5", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => ({ hit: (getStreak(ctx, side) ?? 0) >= 5 }),
  },
  {
    key: "LOSING_STREAK_5", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => ({ hit: (getStreak(ctx, side) ?? 0) <= -5 }),
  },

  // Matchup quality (game-wide)
  {
    key: "TOP_OFF_VS_BOTTOM_DEF", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      const homeOff = getRankOff(ctx, "home");
      const awayDef = getRankDef(ctx, "away");
      const awayOff = getRankOff(ctx, "away");
      const homeDef = getRankDef(ctx, "home");

      const homeAttacking = (homeOff ?? 99) <= 10 && (awayDef ?? 0) >= 21;
      const awayAttacking = (awayOff ?? 99) <= 10 && (homeDef ?? 0) >= 21;

      if (homeAttacking || awayAttacking) {
        return {
          hit: true,
          meta: { offenseSide: homeAttacking ? "home" : "away" },
        };
      }
      return { hit: false };
    },
  },
  {
    key: "BOTH_TOP_10_OFF", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => ({
      hit: (getRankOff(ctx, "home") ?? 99) <= 10 && (getRankOff(ctx, "away") ?? 99) <= 10,
    }),
  },

  // Lines from odds (game-wide)
  {
    key: "SPREAD_UNDER_3", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      return { hit: Math.abs(ctx.odds.spreadHome) < 3 };
    },
  },
  {
    key: "SPREAD_3_TO_7", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      const abs = Math.abs(ctx.odds.spreadHome);
      return { hit: abs >= 3 && abs <= 7 };
    },
  },
  {
    key: "SPREAD_OVER_10", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      return { hit: Math.abs(ctx.odds.spreadHome) > 10 };
    },
  },
  {
    key: "TOTAL_LINE_OVER_230", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.totalOver) return { hit: false };
      return { hit: ctx.odds.totalOver > 230 };
    },
  },
  {
    key: "TOTAL_LINE_OVER_235", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.totalOver) return { hit: false };
      return { hit: ctx.odds.totalOver > 235 };
    },
  },
  {
    key: "TOTAL_LINE_UNDER_210", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.totalOver) return { hit: false };
      return { hit: ctx.odds.totalOver < 210 };
    },
  },

  // Player context (per-side)
  {
    key: "HAS_TOP_10_SCORER", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => {
      const teamId = side === "home" ? ctx.game.homeTeamId : ctx.game.awayTeamId;
      const has = ctx.playerContexts.some((p) => p.teamId === teamId && p.rankPpg != null && p.rankPpg <= 10);
      return { hit: has };
    },
  },
  {
    key: "HAS_TOP_10_REBOUNDER", type: "condition", sides: SIDES_PER_TEAM,
    compute: (ctx, side) => {
      const teamId = side === "home" ? ctx.game.homeTeamId : ctx.game.awayTeamId;
      const has = ctx.playerContexts.some((p) => p.teamId === teamId && p.rankRpg != null && p.rankRpg <= 10);
      return { hit: has };
    },
  },

  // Top-5 scorer vs bottom-10 defense (game-wide)
  {
    key: "TOP_5_SCORER_VS_BOTTOM_10_DEF", type: "condition", sides: SIDES_GAME,
    compute: (ctx) => {
      for (const pCtx of ctx.playerContexts) {
        if (pCtx.rankPpg != null && pCtx.rankPpg <= 5) {
          const oppTeamId = pCtx.teamId === ctx.game.homeTeamId ? ctx.game.awayTeamId : ctx.game.homeTeamId;
          const oppSide = oppTeamId === ctx.game.homeTeamId ? "home" : "away";
          const oppDef = getRankDef(ctx, oppSide);
          if (oppDef != null && oppDef >= 21) {
            return { hit: true, meta: { playerId: pCtx.playerId, oppTeamId } };
          }
        }
      }
      return { hit: false };
    },
  },
];

// ── HELPER: Find team's top player by stat ──

function getTeamTopPlayer(
  ctx: GameEventContext,
  side: "home" | "away",
  stat: "ppg" | "rpg" | "apg"
): PlayerGameContext | null {
  const teamId = side === "home" ? ctx.game.homeTeamId : ctx.game.awayTeamId;
  const candidates = ctx.playerContexts.filter((p) => p.teamId === teamId);
  if (candidates.length === 0) return null;
  
  return candidates.reduce((best, p) => {
    const pVal = stat === "ppg" ? p.ppg : stat === "rpg" ? p.rpg : p.apg;
    const bVal = stat === "ppg" ? best.ppg : stat === "rpg" ? best.rpg : best.apg;
    return pVal > bVal ? p : best;
  });
}

function getPlayerStatLine(ctx: GameEventContext, playerId: number): PlayerGameStat | null {
  return ctx.stats.find((s) => s.playerId === playerId) ?? null;
}

// ── OUTCOME EVENTS ──

const outcomeEvents: GameEventDef[] = [
  // ═══════════════════════════════════════════════════════════════
  // BETTING LINE OUTCOMES (based on pre-game odds)
  // ═══════════════════════════════════════════════════════════════
  
  // Spread outcomes
  {
    key: "HOME_COVERED", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      const homeMargin = ctx.game.homeScore - ctx.game.awayScore;
      return { hit: homeMargin + ctx.odds.spreadHome > 0, meta: { spread: ctx.odds.spreadHome, margin: homeMargin } };
    },
  },
  {
    key: "AWAY_COVERED", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      const awayMargin = ctx.game.awayScore - ctx.game.homeScore;
      const spreadAway = -ctx.odds.spreadHome;
      return { hit: awayMargin + spreadAway > 0, meta: { spread: spreadAway, margin: awayMargin } };
    },
  },
  {
    key: "FAVORITE_COVERED", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      const homeMargin = ctx.game.homeScore - ctx.game.awayScore;
      const homeFav = ctx.odds.spreadHome < 0;
      if (homeFav) {
        return { hit: homeMargin + ctx.odds.spreadHome > 0, meta: { favSide: "home", spread: ctx.odds.spreadHome } };
      } else {
        const awayMargin = -homeMargin;
        const spreadAway = -ctx.odds.spreadHome;
        return { hit: awayMargin + spreadAway > 0, meta: { favSide: "away", spread: spreadAway } };
      }
    },
  },
  {
    key: "UNDERDOG_COVERED", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.spreadHome) return { hit: false };
      const homeMargin = ctx.game.homeScore - ctx.game.awayScore;
      const homeDog = ctx.odds.spreadHome > 0;
      if (homeDog) {
        return { hit: homeMargin + ctx.odds.spreadHome > 0, meta: { dogSide: "home", spread: ctx.odds.spreadHome } };
      } else {
        const awayMargin = -homeMargin;
        const spreadAway = -ctx.odds.spreadHome;
        return { hit: awayMargin + spreadAway > 0, meta: { dogSide: "away", spread: spreadAway } };
      }
    },
  },
  
  // Total (over/under) outcomes
  {
    key: "OVER_HIT", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.totalOver) return { hit: false };
      const actual = ctx.game.homeScore + ctx.game.awayScore;
      return { hit: actual > ctx.odds.totalOver, meta: { line: ctx.odds.totalOver, actual } };
    },
  },
  {
    key: "UNDER_HIT", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      if (!ctx.odds?.totalOver) return { hit: false };
      const actual = ctx.game.homeScore + ctx.game.awayScore;
      return { hit: actual < ctx.odds.totalOver, meta: { line: ctx.odds.totalOver, actual } };
    },
  },
  
  // ═══════════════════════════════════════════════════════════════
  // PLAYER-SPECIFIC OUTCOMES (targeting identifiable pre-game players)
  // ═══════════════════════════════════════════════════════════════
  
  // Top scorer on team hits points threshold
  {
    key: "HOME_TOP_SCORER_25_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points >= 25, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  {
    key: "HOME_TOP_SCORER_30_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points >= 30, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  {
    key: "AWAY_TOP_SCORER_25_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points >= 25, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  {
    key: "AWAY_TOP_SCORER_30_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points >= 30, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  
  // Top scorer exceeds their season average
  {
    key: "HOME_TOP_SCORER_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "ppg");
      if (!top || top.ppg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points > top.ppg, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  {
    key: "AWAY_TOP_SCORER_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "ppg");
      if (!top || top.ppg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.points > top.ppg, meta: { playerId: top.playerId, ppgPre: top.ppg, points: stat.points } };
    },
  },
  
  // Top rebounder on team hits rebounds threshold
  {
    key: "HOME_TOP_REBOUNDER_10_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "rpg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds >= 10, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  {
    key: "HOME_TOP_REBOUNDER_12_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "rpg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds >= 12, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  {
    key: "AWAY_TOP_REBOUNDER_10_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "rpg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds >= 10, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  {
    key: "AWAY_TOP_REBOUNDER_12_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "rpg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds >= 12, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  
  // Top rebounder exceeds average
  {
    key: "HOME_TOP_REBOUNDER_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "rpg");
      if (!top || top.rpg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds > top.rpg, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  {
    key: "AWAY_TOP_REBOUNDER_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "rpg");
      if (!top || top.rpg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.rebounds > top.rpg, meta: { playerId: top.playerId, rpgPre: top.rpg, rebounds: stat.rebounds } };
    },
  },
  
  // Top assist man on team hits assists threshold
  {
    key: "HOME_TOP_ASSIST_8_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "apg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists >= 8, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  {
    key: "HOME_TOP_ASSIST_10_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "apg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists >= 10, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  {
    key: "AWAY_TOP_ASSIST_8_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "apg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists >= 8, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  {
    key: "AWAY_TOP_ASSIST_10_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "apg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists >= 10, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  
  // Top assist man exceeds average
  {
    key: "HOME_TOP_ASSIST_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "apg");
      if (!top || top.apg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists > top.apg, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  {
    key: "AWAY_TOP_ASSIST_EXCEEDS_AVG", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "apg");
      if (!top || top.apg === 0) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      return { hit: stat.assists > top.apg, meta: { playerId: top.playerId, apgPre: top.apg, assists: stat.assists } };
    },
  },
  
  // Top scorer double-double (identifiable player)
  {
    key: "HOME_TOP_SCORER_DOUBLE_DOUBLE", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "home", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      let cats = 0;
      if (stat.points >= 10) cats++;
      if (stat.rebounds >= 10) cats++;
      if (stat.assists >= 10) cats++;
      return { hit: cats >= 2, meta: { playerId: top.playerId, pts: stat.points, reb: stat.rebounds, ast: stat.assists } };
    },
  },
  {
    key: "AWAY_TOP_SCORER_DOUBLE_DOUBLE", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const top = getTeamTopPlayer(ctx, "away", "ppg");
      if (!top) return { hit: false };
      const stat = getPlayerStatLine(ctx, top.playerId);
      if (!stat) return { hit: false };
      let cats = 0;
      if (stat.points >= 10) cats++;
      if (stat.rebounds >= 10) cats++;
      if (stat.assists >= 10) cats++;
      return { hit: cats >= 2, meta: { playerId: top.playerId, pts: stat.points, reb: stat.rebounds, ast: stat.assists } };
    },
  },

  // ═══════════════════════════════════════════════════════════════
  // GENERIC GAME OUTCOMES (kept for backward compatibility)
  // ═══════════════════════════════════════════════════════════════
  
  // Game totals
  {
    key: "TOTAL_OVER_220", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore > 220 }),
  },
  {
    key: "TOTAL_OVER_230", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore > 230 }),
  },
  {
    key: "TOTAL_OVER_240", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore > 240 }),
  },
  {
    key: "TOTAL_OVER_250", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore > 250 }),
  },
  {
    key: "TOTAL_UNDER_200", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore < 200 }),
  },
  {
    key: "TOTAL_UNDER_210", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore + ctx.game.awayScore < 210 }),
  },

  // Spread / margin (HOME_COVERED moved to betting section above)
  {
    key: "HOME_WIN", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.homeScore > ctx.game.awayScore }),
  },
  {
    key: "AWAY_WIN", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: ctx.game.awayScore > ctx.game.homeScore }),
  },
  {
    key: "MARGIN_UNDER_5", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: Math.abs(ctx.game.homeScore - ctx.game.awayScore) < 5 }),
  },
  {
    key: "MARGIN_UNDER_10", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: Math.abs(ctx.game.homeScore - ctx.game.awayScore) < 10 }),
  },
  {
    key: "BLOWOUT_20_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => ({ hit: Math.abs(ctx.game.homeScore - ctx.game.awayScore) >= 20 }),
  },
  {
    key: "OVERTIME", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      // Detect OT via total minutes -- regulation is 240 min * 60 sec = 14400 sec per team (5 players x 48 min)
      // If total player minutes exceed the regulation amount, it went to OT
      const totalMinSec = ctx.stats.reduce((sum, s) => sum + s.minutes, 0);
      return { hit: totalMinSec > 14400 * 2 + 1200 }; // 2 teams x 240 min + buffer for > regulation
    },
  },

  // Player outcomes
  {
    key: "PLAYER_30_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => s.points >= 30);
      return player ? { hit: true, meta: { playerId: player.playerId, points: player.points } } : { hit: false };
    },
  },
  {
    key: "PLAYER_40_PLUS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => s.points >= 40);
      return player ? { hit: true, meta: { playerId: player.playerId, points: player.points } } : { hit: false };
    },
  },
  {
    key: "PLAYER_DOUBLE_DOUBLE", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => {
        let cats = 0;
        if (s.points >= 10) cats++;
        if (s.rebounds >= 10) cats++;
        if (s.assists >= 10) cats++;
        if (s.steals >= 10) cats++;
        if (s.blocks >= 10) cats++;
        return cats >= 2;
      });
      return player ? { hit: true, meta: { playerId: player.playerId } } : { hit: false };
    },
  },
  {
    key: "PLAYER_TRIPLE_DOUBLE", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => {
        let cats = 0;
        if (s.points >= 10) cats++;
        if (s.rebounds >= 10) cats++;
        if (s.assists >= 10) cats++;
        if (s.steals >= 10) cats++;
        if (s.blocks >= 10) cats++;
        return cats >= 3;
      });
      return player ? { hit: true, meta: { playerId: player.playerId } } : { hit: false };
    },
  },
  {
    key: "PLAYER_10_PLUS_ASSISTS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => s.assists >= 10);
      return player ? { hit: true, meta: { playerId: player.playerId, assists: player.assists } } : { hit: false };
    },
  },
  {
    key: "PLAYER_10_PLUS_REBOUNDS", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => s.rebounds >= 10);
      return player ? { hit: true, meta: { playerId: player.playerId, rebounds: player.rebounds } } : { hit: false };
    },
  },
  {
    key: "PLAYER_5_PLUS_THREES", type: "outcome", sides: SIDES_GAME,
    compute: (ctx) => {
      const player = ctx.stats.find((s) => (s.fg3m ?? 0) >= 5);
      return player ? { hit: true, meta: { playerId: player.playerId, fg3m: player.fg3m } } : { hit: false };
    },
  },
];

export const GAME_EVENT_CATALOG: GameEventDef[] = [...conditionEvents, ...outcomeEvents];
