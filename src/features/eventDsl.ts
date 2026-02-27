import type { Game, Team, PlayerGameStat, Player } from "@prisma/client";
import type { NightContext, TeamAgg, EventResult } from "./eventCatalog.js";

type GameWithTeams = Game & { homeTeam: Team; awayTeam: Team };
type StatWithPlayer = PlayerGameStat & { player: Player };

// ── Player predicates ──

export function anyPlayer(
  ctx: NightContext,
  predicate: (s: StatWithPlayer) => boolean,
): { hit: boolean; matching: StatWithPlayer[] } {
  const matching = ctx.stats.filter(predicate);
  return { hit: matching.length > 0, matching };
}

export function countPlayers(
  ctx: NightContext,
  predicate: (s: StatWithPlayer) => boolean,
): { count: number; uniqueIds: number[]; matching: StatWithPlayer[] } {
  const matching = ctx.stats.filter(predicate);
  const uniqueIds = [...new Set(matching.map((s) => s.playerId))];
  return { count: uniqueIds.length, uniqueIds, matching };
}

// ── Game predicates ──

export function anyGame(
  ctx: NightContext,
  predicate: (g: GameWithTeams) => boolean,
): { hit: boolean; matching: GameWithTeams[] } {
  const matching = ctx.games.filter(predicate);
  return { hit: matching.length > 0, matching };
}

export function countGames(
  ctx: NightContext,
  predicate: (g: GameWithTeams) => boolean,
): { count: number; matching: GameWithTeams[] } {
  const matching = ctx.games.filter(predicate);
  return { count: matching.length, matching };
}

// ── Team aggregate predicates ──

function resolveTeamAggs(ctx: NightContext): TeamAgg[] {
  if (ctx.teamAggregates) return ctx.teamAggregates;
  const map = new Map<number, TeamAgg>();
  for (const s of ctx.stats) {
    let agg = map.get(s.teamId);
    if (!agg) {
      agg = { teamId: s.teamId, points: 0, rebounds: 0, assists: 0, steals: 0, blocks: 0, turnovers: 0, minutes: 0 };
      map.set(s.teamId, agg);
    }
    agg.points += s.points;
    agg.rebounds += s.rebounds;
    agg.assists += s.assists;
    agg.steals += s.steals;
    agg.blocks += s.blocks;
    agg.turnovers += s.turnovers;
    agg.minutes += s.minutes;
  }
  return [...map.values()];
}

export function anyTeam(
  ctx: NightContext,
  predicate: (agg: TeamAgg) => boolean,
): { hit: boolean; matching: TeamAgg[] } {
  const aggs = resolveTeamAggs(ctx);
  const matching = aggs.filter(predicate);
  return { hit: matching.length > 0, matching };
}

export function countTeams(
  ctx: NightContext,
  predicate: (agg: TeamAgg) => boolean,
): { count: number; matching: TeamAgg[] } {
  const aggs = resolveTeamAggs(ctx);
  const matching = aggs.filter(predicate);
  return { count: matching.length, matching };
}

// ── Score-safe context ──

export function withScoredGames(ctx: NightContext): NightContext {
  const scored = ctx.games.filter(
    (g) => g.homeScore != null && g.awayScore != null,
  );
  return { ...ctx, games: scored };
}

// ── Score-based helpers (work on Game rows, not aggregates) ──

export function teamsScoring(
  ctx: NightContext,
  minScore: number,
): { teamIds: number[]; maxScore: number } {
  const teams: number[] = [];
  let maxScore = 0;
  for (const g of ctx.games) {
    if (g.homeScore == null || g.awayScore == null) continue;
    if (g.homeScore >= minScore) teams.push(g.homeTeamId);
    if (g.awayScore >= minScore) teams.push(g.awayTeamId);
    maxScore = Math.max(maxScore, g.homeScore, g.awayScore);
  }
  return { teamIds: [...new Set(teams)], maxScore };
}

// ── Per-team player grouping ──

export function perTeamPlayerCount(
  ctx: NightContext,
  playerPredicate: (s: StatWithPlayer) => boolean,
): Map<number, Set<number>> {
  const map = new Map<number, Set<number>>();
  for (const s of ctx.stats) {
    if (playerPredicate(s)) {
      let set = map.get(s.teamId);
      if (!set) {
        set = new Set();
        map.set(s.teamId, set);
      }
      set.add(s.playerId);
    }
  }
  return map;
}
