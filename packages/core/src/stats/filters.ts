import type { Prisma } from "@bluey/db";

export interface RollupFilters {
  season?: number;
  dateFrom?: string;
  dateTo?: string;
  opponentTeamId?: number;
  homeAway?: "home" | "away" | "either";
  minMinutes?: number;
  stage?: number;
}

export function buildGameWhere(
  filters: RollupFilters,
  teamId?: number
): Prisma.GameWhereInput {
  const where: Prisma.GameWhereInput = {};

  if (filters.season != null) where.season = filters.season;
  if (filters.stage != null) where.stage = filters.stage;

  if (filters.dateFrom || filters.dateTo) {
    where.date = {};
    if (filters.dateFrom) where.date.gte = new Date(filters.dateFrom);
    if (filters.dateTo) where.date.lte = new Date(filters.dateTo);
  }

  if (teamId != null) {
    const homeAway = filters.homeAway ?? "either";
    const opp = filters.opponentTeamId;

    const homeCond: Prisma.GameWhereInput = { homeTeamId: teamId };
    const awayCond: Prisma.GameWhereInput = { awayTeamId: teamId };

    if (opp != null) {
      homeCond.awayTeamId = opp;
      awayCond.homeTeamId = opp;
    }

    if (homeAway === "home") {
      Object.assign(where, homeCond);
    } else if (homeAway === "away") {
      Object.assign(where, awayCond);
    } else {
      where.OR = [homeCond, awayCond];
    }
  }

  return where;
}

export function buildStatWhere(filters: RollupFilters): Prisma.PlayerGameStatWhereInput {
  const where: Prisma.PlayerGameStatWhereInput = {};
  if (filters.minMinutes != null && filters.minMinutes > 0) {
    where.minutes = { gte: filters.minMinutes * 60 };
  }
  return where;
}
