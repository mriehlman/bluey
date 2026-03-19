import { prisma } from "../db/prisma.js";
import * as fs from "fs";
import * as path from "path";

const MS_PER_DAY = 86_400_000;
const MIN_GAMES_FOR_CONTEXT = 10;
const MIN_GAMES_FOR_PLAYER_RANK = 10;
const MIN_MPG_FOR_PLAYER_RANK = 20;

function hasFinalScore(game: { homeScore: number; awayScore: number; status: string | null }): boolean {
  const hasAnyScore = (game.homeScore ?? 0) > 0 || (game.awayScore ?? 0) > 0;
  if (!hasAnyScore) return false;
  if (game.status == null) return true;
  return game.status.includes("Final");
}

interface TeamAccum {
  teamId: number;
  gamesPlayed: number;
  wins: number;
  losses: number;
  streak: number;
  totalScored: number;
  totalAllowed: number;
  totalRebounds: number;
  totalAssists: number;
  totalFg3m: number;
  totalFg3a: number;
  totalFtm: number;
  totalFta: number;
  totalFga: number;
  totalOreb: number;
  totalTov: number;
  totalMinutesSec: number;
  hasSplits: boolean;
  lastGameDate: Date | null;
}

interface PlayerAccum {
  playerId: number;
  teamId: number;
  gamesPlayed: number;
  starts: number;
  totalPoints: number;
  totalRebounds: number;
  totalAssists: number;
  totalMinutesSec: number;
  totalFg3m: number;
  totalFg3a: number;
  totalFtm: number;
  totalFta: number;
  last5Points: number[];
}

export interface TeamSnapshot {
  teamId: number;
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
  gamesPlayed: number;
}

export interface PlayerSnapshot {
  playerId: number;
  teamId: number;
  gamesPlayed: number;
  ppg: number;
  rpg: number;
  apg: number;
  mpg: number;
  fg3Pct: number | null;
  ftPct: number | null;
  last5Ppg: number | null;
  rankPpg: number | null;
  rankRpg: number | null;
  rankApg: number | null;
}

function makeTeamAccum(teamId: number): TeamAccum {
  return {
    teamId,
    gamesPlayed: 0,
    wins: 0,
    losses: 0,
    streak: 0,
    totalScored: 0,
    totalAllowed: 0,
    totalRebounds: 0,
    totalAssists: 0,
    totalFg3m: 0,
    totalFg3a: 0,
    totalFtm: 0,
    totalFta: 0,
    totalFga: 0,
    totalOreb: 0,
    totalTov: 0,
    totalMinutesSec: 0,
    hasSplits: true,
    lastGameDate: null,
  };
}

function makePlayerAccum(playerId: number, teamId: number): PlayerAccum {
  return {
    playerId,
    teamId,
    gamesPlayed: 0,
    starts: 0,
    totalPoints: 0,
    totalRebounds: 0,
    totalAssists: 0,
    totalMinutesSec: 0,
    totalFg3m: 0,
    totalFg3a: 0,
    totalFtm: 0,
    totalFta: 0,
    last5Points: [],
  };
}

type InjuryStatus = "out" | "doubtful" | "questionable" | "probable" | "unknown";
type TeamInjurySummary = {
  out: number;
  doubtful: number;
  questionable: number;
  probable: number;
  byPlayerName: Map<string, InjuryStatus>;
};

function normalizeName(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function injuryNameToCanonical(raw: string): string {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (!s.includes(",")) return s;
  const [last, first] = s.split(",").map((p) => p.trim());
  return `${first} ${last}`.trim();
}

function classifyInjuryStatus(raw: unknown): InjuryStatus {
  const s = String(raw ?? "").toLowerCase().trim();
  if (!s) return "unknown";
  if (s.includes("out")) return "out";
  if (s.includes("doubtful")) return "doubtful";
  if (s.includes("questionable") || s.includes("game time")) return "questionable";
  if (s.includes("probable")) return "probable";
  return "unknown";
}

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function buildTeamAliasLookup(
  teams: Array<{ id: number; city: string | null; name: string | null; code: string | null }>,
): Map<string, number> {
  const out = new Map<string, number>();
  for (const t of teams) {
    const citySafe = String(t.city ?? "").trim();
    const nameSafe = String(t.name ?? "").trim();
    const codeSafe = String(t.code ?? "").trim();
    const full = `${citySafe} ${nameSafe}`.trim();
    const aliases = [full, nameSafe, codeSafe].filter(Boolean);
    if (full.toLowerCase() === "los angeles clippers") aliases.push("la clippers");
    if (full.toLowerCase() === "philadelphia 76ers") aliases.push("sixers");
    for (const alias of aliases) out.set(normalizeName(alias), t.id);
  }
  return out;
}

function loadEarlyInjuriesForDate(
  date: string,
  teamLookup: Map<string, number>,
  cache: Map<string, Map<number, TeamInjurySummary>>,
): Map<number, TeamInjurySummary> {
  const cached = cache.get(date);
  if (cached) return cached;

  const result = new Map<number, TeamInjurySummary>();
  const filePath = path.join(getDataDir(), "raw", "injuries", `${date}.early.json`);
  if (!fs.existsSync(filePath)) {
    cache.set(date, result);
    return result;
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as { rows?: unknown };
    const rows = Array.isArray(parsed.rows) ? parsed.rows : [];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      const rec = row as Record<string, unknown>;
      const teamRaw = String(rec.Team ?? "").trim();
      const playerRaw = String(rec["Player Name"] ?? "").trim();
      if (!teamRaw || !playerRaw) continue;
      const teamId = teamLookup.get(normalizeName(teamRaw));
      if (!teamId) continue;
      const status = classifyInjuryStatus(rec["Current Status"]);
      const playerKey = normalizeName(injuryNameToCanonical(playerRaw));
      if (!result.has(teamId)) {
        result.set(teamId, {
          out: 0,
          doubtful: 0,
          questionable: 0,
          probable: 0,
          byPlayerName: new Map<string, InjuryStatus>(),
        });
      }
      const teamSummary = result.get(teamId)!;
      if (status === "out") teamSummary.out++;
      else if (status === "doubtful") teamSummary.doubtful++;
      else if (status === "questionable") teamSummary.questionable++;
      else if (status === "probable") teamSummary.probable++;
      if (playerKey) teamSummary.byPlayerName.set(playerKey, status);
    }
  } catch {
    // Best-effort read; keep empty if malformed.
  }

  cache.set(date, result);
  return result;
}

function computeLineupSignals(
  teamId: number,
  players: Map<number, PlayerAccum>,
  injuries: TeamInjurySummary | undefined,
  priorStarters: string[] | undefined,
): {
  certainty: number;
  lateScratchRisk: number;
} {
  const starterNames = (priorStarters ?? [])
    .map((n) => normalizeName(n))
    .filter(Boolean)
    .slice(0, 5);
  if (starterNames.length > 0) {
    let outStarters = 0;
    let doubtfulStarters = 0;
    let questionableStarters = 0;
    let probableStarters = 0;
    for (const name of starterNames) {
      const status = injuries?.byPlayerName.get(name);
      if (status === "out") outStarters++;
      else if (status === "doubtful") doubtfulStarters++;
      else if (status === "questionable") questionableStarters++;
      else if (status === "probable") probableStarters++;
    }

    // Previous-game starters are our baseline lineup. Morning injuries imply replacement pressure.
    const replacementPressure = outStarters * 1.0 + doubtfulStarters * 0.8 + questionableStarters * 0.45 + probableStarters * 0.15;
    const certainty = Math.max(0, Math.min(1, 1 - replacementPressure / 5));
    const lateScratchRisk = Math.max(
      0,
      Math.min(1, (questionableStarters * 1.0 + doubtfulStarters * 0.6 + probableStarters * 0.2) / 5),
    );
    return { certainty, lateScratchRisk };
  }

  // Fallback when prior-game starters are unavailable.
  const candidates = [...players.values()]
    .filter((p) => p.teamId === teamId && p.gamesPlayed > 0)
    .map((p) => {
      const mpg = p.totalMinutesSec / Math.max(1, p.gamesPlayed) / 60;
      const startRate = p.starts / Math.max(1, p.gamesPlayed);
      return { p, mpg, startRate };
    })
    .sort((a, b) => {
      if (b.startRate !== a.startRate) return b.startRate - a.startRate;
      if (b.mpg !== a.mpg) return b.mpg - a.mpg;
      return b.p.gamesPlayed - a.p.gamesPlayed;
    })
    .slice(0, 5);

  if (candidates.length === 0) return { certainty: 0.5, lateScratchRisk: 0.5 };

  const teamOut = injuries?.out ?? 0;
  const teamDoubtful = injuries?.doubtful ?? 0;
  const teamQuestionable = injuries?.questionable ?? 0;
  const teamProbable = injuries?.probable ?? 0;

  // Team-level approximation until we have a reliable pregame lineup API.
  let penalty = teamOut * 1.0 + teamDoubtful * 0.75 + teamQuestionable * 0.4 + teamProbable * 0.1;
  let risk = teamOut * 0.4 + teamDoubtful * 0.55 + teamQuestionable * 0.7 + teamProbable * 0.2;
  penalty = Math.min(penalty, candidates.length);
  risk = Math.min(risk, candidates.length);

  const certainty = Math.max(0, Math.min(1, 1 - penalty / 5));
  const lateScratchRisk = Math.max(0, Math.min(1, risk / 5));
  return { certainty, lateScratchRisk };
}

function computePace(accum: TeamAccum): number | null {
  if (!accum.hasSplits || accum.gamesPlayed === 0 || accum.totalMinutesSec === 0) return null;
  const possessions = accum.totalFga - accum.totalOreb + accum.totalTov + 0.44 * accum.totalFta;
  const teamMinutes = accum.totalMinutesSec / 60;
  const gameMinutes = 48;
  return possessions * (gameMinutes / teamMinutes) * accum.gamesPlayed;
}

function computeTeamSnapshot(accum: TeamAccum): TeamSnapshot | null {
  if (accum.gamesPlayed < MIN_GAMES_FOR_CONTEXT) return null;

  const gp = accum.gamesPlayed;
  return {
    teamId: accum.teamId,
    wins: accum.wins,
    losses: accum.losses,
    ppg: accum.totalScored / gp,
    oppg: accum.totalAllowed / gp,
    pace: computePace(accum),
    rebPg: accum.hasSplits ? accum.totalRebounds / gp : null,
    astPg: accum.totalAssists / gp,
    fg3Pct: accum.totalFg3a > 0 ? accum.totalFg3m / accum.totalFg3a : null,
    ftPct: accum.totalFta > 0 ? accum.totalFtm / accum.totalFta : null,
    rankOff: null,
    rankDef: null,
    rankPace: null,
    streak: accum.streak,
    lastGameDate: accum.lastGameDate,
    gamesPlayed: accum.gamesPlayed,
  };
}

function computePlayerSnapshot(accum: PlayerAccum): PlayerSnapshot | null {
  if (accum.gamesPlayed === 0) return null;

  const gp = accum.gamesPlayed;
  // Handle inconsistent data: some seasons store minutes in seconds (~1800-2800),
  // others store in minutes (~20-50). If total is > 200 per game avg, it's likely seconds.
  const avgRaw = accum.totalMinutesSec / gp;
  const mpg = avgRaw > 200 ? avgRaw / 60 : avgRaw;

  return {
    playerId: accum.playerId,
    teamId: accum.teamId,
    gamesPlayed: gp,
    ppg: accum.totalPoints / gp,
    rpg: accum.totalRebounds / gp,
    apg: accum.totalAssists / gp,
    mpg,
    fg3Pct: accum.totalFg3a > 0 ? accum.totalFg3m / accum.totalFg3a : null,
    ftPct: accum.totalFta > 0 ? accum.totalFtm / accum.totalFta : null,
    last5Ppg: accum.last5Points.length >= 5
      ? accum.last5Points.slice(-5).reduce((a, b) => a + b, 0) / 5
      : null,
    rankPpg: null,
    rankRpg: null,
    rankApg: null,
  };
}

function rankTeams(snapshots: Map<number, TeamSnapshot>): void {
  const teams = [...snapshots.values()];

  const byPpg = [...teams].sort((a, b) => b.ppg - a.ppg);
  byPpg.forEach((t, i) => { t.rankOff = i + 1; });

  const byOppg = [...teams].sort((a, b) => a.oppg - b.oppg);
  byOppg.forEach((t, i) => { t.rankDef = i + 1; });

  const withPace = teams.filter((t) => t.pace != null);
  const byPace = [...withPace].sort((a, b) => (b.pace ?? 0) - (a.pace ?? 0));
  byPace.forEach((t, i) => { t.rankPace = i + 1; });
}

function rankPlayers(snapshots: Map<number, PlayerSnapshot>): void {
  const eligible = [...snapshots.values()].filter(
    (p) => p.gamesPlayed >= MIN_GAMES_FOR_PLAYER_RANK && p.mpg >= MIN_MPG_FOR_PLAYER_RANK,
  );

  const byPpg = [...eligible].sort((a, b) => b.ppg - a.ppg);
  byPpg.forEach((p, i) => { p.rankPpg = i + 1; });

  const byRpg = [...eligible].sort((a, b) => b.rpg - a.rpg);
  byRpg.forEach((p, i) => { p.rankRpg = i + 1; });

  const byApg = [...eligible].sort((a, b) => b.apg - a.apg);
  byApg.forEach((p, i) => { p.rankApg = i + 1; });
}

export interface ContextForDate {
  teamSnapshots: Map<number, TeamSnapshot>;
  playerSnapshots: Map<number, PlayerSnapshot>;
}

/**
 * Compute pre-game context snapshots for all teams and players as of a given
 * date within a season. Reusable for both batch (historical) and live
 * (upcoming game prediction) flows.
 */
export async function computeContextForDate(
  season: number,
  asOfDate: Date,
  teamIds?: number[],
): Promise<ContextForDate> {
  const games = await prisma.game.findMany({
    where: {
      season,
      date: { lt: asOfDate },
    },
    include: {
      playerStats: true,
    },
    orderBy: { date: "asc" },
  });

  const teamAccums = new Map<number, TeamAccum>();
  const playerAccums = new Map<number, PlayerAccum>();

  for (const game of games) {
    if (!hasFinalScore(game)) continue;

    const homeAccum = teamAccums.get(game.homeTeamId) ?? makeTeamAccum(game.homeTeamId);
    const awayAccum = teamAccums.get(game.awayTeamId) ?? makeTeamAccum(game.awayTeamId);

    const homeWon = game.homeScore > game.awayScore;

    for (const accum of [homeAccum, awayAccum]) {
      const isHome = accum.teamId === game.homeTeamId;
      const scored = isHome ? game.homeScore : game.awayScore;
      const allowed = isHome ? game.awayScore : game.homeScore;
      const won = isHome ? homeWon : !homeWon;

      accum.gamesPlayed++;
      accum.totalScored += scored;
      accum.totalAllowed += allowed;
      if (won) {
        accum.wins++;
        accum.streak = accum.streak >= 0 ? accum.streak + 1 : 1;
      } else {
        accum.losses++;
        accum.streak = accum.streak <= 0 ? accum.streak - 1 : -1;
      }
      accum.lastGameDate = game.date;
    }

    const statsByTeam = new Map<number, typeof game.playerStats>();
    for (const stat of game.playerStats) {
      const arr = statsByTeam.get(stat.teamId) ?? [];
      arr.push(stat);
      statsByTeam.set(stat.teamId, arr);
    }

    for (const [tId, stats] of statsByTeam) {
      const accum = tId === game.homeTeamId ? homeAccum : awayAccum;
      let gameFga = 0, gameOreb = 0, gameFg3m = 0, gameFg3a = 0, gameFtm = 0, gameFta = 0;
      let gameTov = 0, gameMinSec = 0, gameReb = 0, gameAst = 0;
      let missSplits = false;

      for (const s of stats) {
        gameReb += s.rebounds;
        gameAst += s.assists;
        gameMinSec += s.minutes;
        gameTov += s.turnovers;

        if (s.fga != null) gameFga += s.fga; else missSplits = true;
        if (s.oreb != null) gameOreb += s.oreb; else missSplits = true;
        if (s.fg3m != null) gameFg3m += s.fg3m;
        if (s.fg3a != null) gameFg3a += s.fg3a;
        if (s.ftm != null) gameFtm += s.ftm;
        if (s.fta != null) gameFta += s.fta;

        const pAccum = playerAccums.get(s.playerId) ?? makePlayerAccum(s.playerId, s.teamId);
        pAccum.teamId = s.teamId;
        pAccum.gamesPlayed++;
        if (s.starter) pAccum.starts++;
        pAccum.totalPoints += s.points;
        pAccum.totalRebounds += s.rebounds;
        pAccum.totalAssists += s.assists;
        pAccum.totalMinutesSec += s.minutes;
        pAccum.totalFg3m += s.fg3m ?? 0;
        pAccum.totalFg3a += s.fg3a ?? 0;
        pAccum.totalFtm += s.ftm ?? 0;
        pAccum.totalFta += s.fta ?? 0;
        pAccum.last5Points.push(s.points);
        if (pAccum.last5Points.length > 5) pAccum.last5Points.shift();
        playerAccums.set(s.playerId, pAccum);
      }

      accum.totalRebounds += gameReb;
      accum.totalAssists += gameAst;
      accum.totalMinutesSec += gameMinSec;
      accum.totalFga += gameFga;
      accum.totalOreb += gameOreb;
      accum.totalFg3m += gameFg3m;
      accum.totalFg3a += gameFg3a;
      accum.totalFtm += gameFtm;
      accum.totalFta += gameFta;
      accum.totalTov += gameTov;
      if (missSplits) accum.hasSplits = false;
    }

    teamAccums.set(game.homeTeamId, homeAccum);
    teamAccums.set(game.awayTeamId, awayAccum);
  }

  const teamSnapshots = new Map<number, TeamSnapshot>();
  for (const [id, accum] of teamAccums) {
    const snap = computeTeamSnapshot(accum);
    if (snap) teamSnapshots.set(id, snap);
  }
  rankTeams(teamSnapshots);

  const playerSnapshots = new Map<number, PlayerSnapshot>();
  for (const [id, accum] of playerAccums) {
    const snap = computePlayerSnapshot(accum);
    if (snap) playerSnapshots.set(id, snap);
  }
  rankPlayers(playerSnapshots);

  if (!teamIds || teamIds.length === 0) {
    return { teamSnapshots, playerSnapshots };
  }

  const requestedTeams = new Set(teamIds);
  const filteredTeamSnapshots = new Map<number, TeamSnapshot>();
  for (const [id, snap] of teamSnapshots) {
    if (requestedTeams.has(id)) {
      filteredTeamSnapshots.set(id, snap);
    }
  }

  const filteredPlayerSnapshots = new Map<number, PlayerSnapshot>();
  for (const [id, snap] of playerSnapshots) {
    if (requestedTeams.has(snap.teamId)) {
      filteredPlayerSnapshots.set(id, snap);
    }
  }

  return { teamSnapshots: filteredTeamSnapshots, playerSnapshots: filteredPlayerSnapshots };
}

function getCurrentSeason(): number {
  const now = new Date();
  // NBA season starts in October, so Oct-Dec uses current year, Jan-Sep uses previous year
  return now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
}

export async function buildGameContext(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const defaultSeason = getCurrentSeason();
  const fromSeason = Number(flags["from-season"] || flags.season) || defaultSeason;
  const toSeason = Number(flags["to-season"] || flags.season || fromSeason);

  if (!fromSeason) {
    console.error("Usage: build:game-context --season YYYY  or  --from-season YYYY --to-season YYYY");
    process.exit(1);
  }

  console.log(`Building game context for season(s) ${fromSeason}${toSeason !== fromSeason ? ` to ${toSeason}` : ""}`);
  const teams = await prisma.team.findMany({
    select: { id: true, city: true, name: true, code: true },
  });
  const players = await prisma.player.findMany({
    select: { id: true, firstname: true, lastname: true },
  });
  const playerNameById = new Map<number, string>();
  for (const p of players) {
    playerNameById.set(p.id, normalizeName(`${p.firstname ?? ""} ${p.lastname ?? ""}`));
  }
  const teamLookup = buildTeamAliasLookup(teams);
  const injuryCache = new Map<string, Map<number, TeamInjurySummary>>();

  for (let season = fromSeason; season <= toSeason; season++) {
    console.log(`\n=== Building GameContext for season ${season} ===\n`);

    const existing = await prisma.gameContext.count({
      where: { game: { season } },
    });
    if (existing > 0) {
      console.log(`  Clearing ${existing} existing GameContext rows...`);
      await prisma.gameContext.deleteMany({ where: { game: { season } } });
    }

    const existingPCtx = await prisma.playerGameContext.count({
      where: { game: { season } },
    });
    if (existingPCtx > 0) {
      console.log(`  Clearing ${existingPCtx} existing PlayerGameContext rows...`);
      await prisma.playerGameContext.deleteMany({ where: { game: { season } } });
    }

    const games = await prisma.game.findMany({
      where: { season },
      include: { playerStats: true },
      orderBy: { date: "asc" },
    });

    console.log(`  Found ${games.length} games for season ${season}`);

    const teamAccums = new Map<number, TeamAccum>();
    const playerAccums = new Map<number, PlayerAccum>();
    const lastStartersByTeam = new Map<number, string[]>();
    let contextCount = 0;
    let playerCtxCount = 0;

    const h2hMap = new Map<string, { homeWins: number; awayWins: number }>();

    for (let gi = 0; gi < games.length; gi++) {
      const game = games[gi];
      if (!hasFinalScore(game)) continue;

      const homeAccum = teamAccums.get(game.homeTeamId) ?? makeTeamAccum(game.homeTeamId);
      const awayAccum = teamAccums.get(game.awayTeamId) ?? makeTeamAccum(game.awayTeamId);
      teamAccums.set(game.homeTeamId, homeAccum);
      teamAccums.set(game.awayTeamId, awayAccum);

      // -- SNAPSHOT BEFORE updating with this game's data --
      if (homeAccum.gamesPlayed >= MIN_GAMES_FOR_CONTEXT && awayAccum.gamesPlayed >= MIN_GAMES_FOR_CONTEXT) {
        const allTeamSnaps = new Map<number, TeamSnapshot>();
        for (const [id, acc] of teamAccums) {
          const snap = computeTeamSnapshot(acc);
          if (snap) allTeamSnaps.set(id, snap);
        }
        rankTeams(allTeamSnaps);

        const homeSnap = allTeamSnaps.get(game.homeTeamId)!;
        const awaySnap = allTeamSnaps.get(game.awayTeamId)!;

        const homeRestDays = homeAccum.lastGameDate
          ? Math.floor((game.date.getTime() - homeAccum.lastGameDate.getTime()) / MS_PER_DAY) - 1
          : null;
        const awayRestDays = awayAccum.lastGameDate
          ? Math.floor((game.date.getTime() - awayAccum.lastGameDate.getTime()) / MS_PER_DAY) - 1
          : null;

        const h2hKey = [game.homeTeamId, game.awayTeamId].sort((a, b) => a - b).join("-");
        const h2h = h2hMap.get(h2hKey) ?? { homeWins: 0, awayWins: 0 };
        const gameDateStr = game.date.toISOString().slice(0, 10);
        const injuriesByTeam = loadEarlyInjuriesForDate(gameDateStr, teamLookup, injuryCache);
        const homeInj = injuriesByTeam.get(game.homeTeamId);
        const awayInj = injuriesByTeam.get(game.awayTeamId);
        const homeLineup = computeLineupSignals(
          game.homeTeamId,
          playerAccums,
          homeInj,
          lastStartersByTeam.get(game.homeTeamId),
        );
        const awayLineup = computeLineupSignals(
          game.awayTeamId,
          playerAccums,
          awayInj,
          lastStartersByTeam.get(game.awayTeamId),
        );

        await prisma.gameContext.create({
          data: {
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
            homeRestDays,
            awayRestDays,
            homeIsB2b: homeRestDays === 0,
            awayIsB2b: awayRestDays === 0,
            homeInjuryOutCount: homeInj?.out ?? 0,
            homeInjuryDoubtfulCount: homeInj?.doubtful ?? 0,
            homeInjuryQuestionableCount: homeInj?.questionable ?? 0,
            homeInjuryProbableCount: homeInj?.probable ?? 0,
            awayInjuryOutCount: awayInj?.out ?? 0,
            awayInjuryDoubtfulCount: awayInj?.doubtful ?? 0,
            awayInjuryQuestionableCount: awayInj?.questionable ?? 0,
            awayInjuryProbableCount: awayInj?.probable ?? 0,
            homeLineupCertainty: homeLineup.certainty,
            awayLineupCertainty: awayLineup.certainty,
            homeLateScratchRisk: homeLineup.lateScratchRisk,
            awayLateScratchRisk: awayLineup.lateScratchRisk,
            h2hHomeWins: h2h.homeWins,
            h2hAwayWins: h2h.awayWins,
          },
        });
        contextCount++;

        // Player contexts for this game
        const allPlayerSnaps = new Map<number, PlayerSnapshot>();
        for (const [id, pAcc] of playerAccums) {
          const snap = computePlayerSnapshot(pAcc);
          if (snap) allPlayerSnaps.set(id, snap);
        }
        rankPlayers(allPlayerSnaps);

        const playerCtxBatch = [];
        for (const stat of game.playerStats) {
          const pSnap = allPlayerSnaps.get(stat.playerId);
          if (!pSnap || pSnap.gamesPlayed === 0) continue;

          const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
          const oppSnap = allTeamSnaps.get(oppTeamId);

          playerCtxBatch.push({
            gameId: game.id,
            playerId: stat.playerId,
            teamId: stat.teamId,
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

        if (playerCtxBatch.length > 0) {
          const result = await prisma.playerGameContext.createMany({
            data: playerCtxBatch,
            skipDuplicates: true,
          });
          playerCtxCount += result.count;
        }
      }

      // -- UPDATE accumulators with this game's data --
      const homeWon = game.homeScore > game.awayScore;

      for (const accum of [homeAccum, awayAccum]) {
        const isHome = accum.teamId === game.homeTeamId;
        const scored = isHome ? game.homeScore : game.awayScore;
        const allowed = isHome ? game.awayScore : game.homeScore;
        const won = isHome ? homeWon : !homeWon;

        accum.gamesPlayed++;
        accum.totalScored += scored;
        accum.totalAllowed += allowed;
        if (won) {
          accum.wins++;
          accum.streak = accum.streak >= 0 ? accum.streak + 1 : 1;
        } else {
          accum.losses++;
          accum.streak = accum.streak <= 0 ? accum.streak - 1 : -1;
        }
        accum.lastGameDate = game.date;
      }

      // H2H tracking
      const h2hKey = [game.homeTeamId, game.awayTeamId].sort((a, b) => a - b).join("-");
      const h2h = h2hMap.get(h2hKey) ?? { homeWins: 0, awayWins: 0 };
      if (homeWon) h2h.homeWins++; else h2h.awayWins++;
      h2hMap.set(h2hKey, h2h);

      // Aggregate player stats per team for this game
      const statsByTeam = new Map<number, typeof game.playerStats>();
      for (const stat of game.playerStats) {
        const arr = statsByTeam.get(stat.teamId) ?? [];
        arr.push(stat);
        statsByTeam.set(stat.teamId, arr);
      }

      for (const [tId, stats] of statsByTeam) {
        const accum = tId === game.homeTeamId ? homeAccum : awayAccum;
        let gameFga = 0, gameOreb = 0, gameFg3m = 0, gameFg3a = 0, gameFtm = 0, gameFta = 0;
        let gameTov = 0, gameMinSec = 0, gameReb = 0, gameAst = 0;
        let missSplits = false;

        for (const s of stats) {
          gameReb += s.rebounds;
          gameAst += s.assists;
          gameMinSec += s.minutes;
          gameTov += s.turnovers;

          if (s.fga != null) gameFga += s.fga; else missSplits = true;
          if (s.oreb != null) gameOreb += s.oreb; else missSplits = true;
          if (s.fg3m != null) gameFg3m += s.fg3m;
          if (s.fg3a != null) gameFg3a += s.fg3a;
          if (s.ftm != null) gameFtm += s.ftm;
          if (s.fta != null) gameFta += s.fta;

          const pAccum = playerAccums.get(s.playerId) ?? makePlayerAccum(s.playerId, s.teamId);
          pAccum.teamId = s.teamId;
          pAccum.gamesPlayed++;
          if (s.starter) pAccum.starts++;
          pAccum.totalPoints += s.points;
          pAccum.totalRebounds += s.rebounds;
          pAccum.totalAssists += s.assists;
          pAccum.totalMinutesSec += s.minutes;
          pAccum.totalFg3m += s.fg3m ?? 0;
          pAccum.totalFg3a += s.fg3a ?? 0;
          pAccum.totalFtm += s.ftm ?? 0;
          pAccum.totalFta += s.fta ?? 0;
          pAccum.last5Points.push(s.points);
          if (pAccum.last5Points.length > 5) pAccum.last5Points.shift();
          playerAccums.set(s.playerId, pAccum);
        }

        accum.totalRebounds += gameReb;
        accum.totalAssists += gameAst;
        accum.totalMinutesSec += gameMinSec;
        accum.totalFga += gameFga;
        accum.totalOreb += gameOreb;
        accum.totalFg3m += gameFg3m;
        accum.totalFg3a += gameFg3a;
        accum.totalFtm += gameFtm;
        accum.totalFta += gameFta;
        accum.totalTov += gameTov;
        if (missSplits) accum.hasSplits = false;

        // Store this game's actual starters as baseline for next game's proposed lineup.
        const starterNames = stats
          .filter((s) => s.starter)
          .map((s) => playerNameById.get(s.playerId) ?? "")
          .map((n) => normalizeName(n))
          .filter(Boolean);
        if (starterNames.length > 0) {
          lastStartersByTeam.set(tId, starterNames.slice(0, 5));
        }
      }

      if ((gi + 1) % 100 === 0) {
        console.log(`  Processed ${gi + 1}/${games.length} games (${contextCount} contexts, ${playerCtxCount} player contexts)`);
      }
    }

    console.log(`\n  Season ${season} complete: ${contextCount} GameContext rows, ${playerCtxCount} PlayerGameContext rows`);
  }
}
