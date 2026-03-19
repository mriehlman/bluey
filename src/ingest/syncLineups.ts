import * as fs from "fs/promises";
import * as fssync from "fs";
import * as path from "path";
import { prisma } from "../db/prisma.js";
import { dateStringToUtcMidday } from "./utils.js";

type SnapshotSpec = { label: string; timeEt: string };
type InjuryStatus = "out" | "doubtful" | "questionable" | "probable" | "unknown";

type PlayerCandidate = {
  playerId: number;
  name: string;
  games: number;
  starts: number;
  startRate: number;
  avgMinutes: number;
  injuryStatus: InjuryStatus | null;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (!args[i].startsWith("--")) continue;
    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

function isDateString(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function dateRange(from: string, to: string): string[] {
  const out: string[] = [];
  const cur = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  while (cur <= end) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

function getDataDir(): string {
  return process.env.DATA_DIR || "./data";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeName(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function displayNameToCanonical(raw: string): string {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (!s.includes(",")) return s;
  const [last, first] = s.split(",").map((p) => p.trim());
  return `${first} ${last}`.trim();
}

function classifyStatus(raw: unknown): InjuryStatus {
  const s = String(raw ?? "").toLowerCase().trim();
  if (!s) return "unknown";
  if (s.includes("out")) return "out";
  if (s.includes("doubtful")) return "doubtful";
  if (s.includes("questionable") || s.includes("game time")) return "questionable";
  if (s.includes("probable")) return "probable";
  return "unknown";
}

function parseSnapshotSpecs(flags: Record<string, string>): SnapshotSpec[] {
  const snapshotsFlag = flags.snapshots;
  const requested = (snapshotsFlag ?? "early,final")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const out: SnapshotSpec[] = [];
  for (const name of requested) {
    if (name === "early") out.push({ label: "early", timeEt: "07:00" });
    else if (name === "final") out.push({ label: "final", timeEt: "17:30" });
  }
  if (out.length === 0) {
    throw new Error("Invalid snapshots. Use --snapshots early,final");
  }
  return out;
}

function teamAliases(city?: string | null, name?: string | null, code?: string | null): string[] {
  const citySafe = String(city ?? "").trim();
  const nameSafe = String(name ?? "").trim();
  const codeSafe = String(code ?? "").trim();
  const full = `${citySafe} ${nameSafe}`.trim();
  const aliases = [nameSafe, full, codeSafe].filter((s) => s.length > 0);
  if (full.toLowerCase() === "los angeles clippers") aliases.push("la clippers");
  if (full.toLowerCase() === "philadelphia 76ers") aliases.push("sixers");
  return aliases;
}

async function buildTeamLookup(): Promise<Map<string, number>> {
  const teams = await prisma.team.findMany({
    select: { id: true, name: true, city: true, code: true },
  });
  const map = new Map<string, number>();
  for (const t of teams) {
    for (const a of teamAliases(t.city, t.name, t.code)) {
      map.set(normalizeName(a), t.id);
    }
  }
  return map;
}

async function loadInjuryMapForSnapshot(
  date: string,
  snapshotLabel: string,
  teamLookup: Map<string, number>,
): Promise<Map<number, Map<string, InjuryStatus>>> {
  const out = new Map<number, Map<string, InjuryStatus>>();
  const filePath = path.join(getDataDir(), "raw", "injuries", `${date}.${snapshotLabel}.json`);
  if (!fssync.existsSync(filePath)) return out;

  let parsed: unknown;
  try {
    parsed = JSON.parse(await fs.readFile(filePath, "utf8")) as unknown;
  } catch {
    return out;
  }

  const rows = (parsed as { rows?: unknown }).rows;
  if (!Array.isArray(rows)) return out;

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const rec = row as Record<string, unknown>;
    const teamRaw = String(rec.Team ?? "").trim();
    const playerRaw = String(rec["Player Name"] ?? "").trim();
    if (!teamRaw || !playerRaw) continue;
    const teamId = teamLookup.get(normalizeName(teamRaw));
    if (!teamId) continue;
    const status = classifyStatus(rec["Current Status"]);
    const playerKey = normalizeName(displayNameToCanonical(playerRaw));
    if (!playerKey) continue;
    if (!out.has(teamId)) out.set(teamId, new Map());
    out.get(teamId)!.set(playerKey, status);
  }
  return out;
}

async function buildTeamCandidates(teamId: number, date: string, lookbackGames: number): Promise<PlayerCandidate[]> {
  const beforeDate = dateStringToUtcMidday(date);
  const recentGames = await prisma.game.findMany({
    where: {
      date: { lt: beforeDate },
      OR: [{ homeTeamId: teamId }, { awayTeamId: teamId }],
    },
    orderBy: { date: "desc" },
    take: lookbackGames,
    select: { id: true },
  });
  if (recentGames.length === 0) return [];

  const stats = await prisma.playerGameStat.findMany({
    where: {
      teamId,
      gameId: { in: recentGames.map((g) => g.id) },
    },
    select: {
      playerId: true,
      starter: true,
      minutes: true,
      player: { select: { firstname: true, lastname: true } },
    },
  });

  const agg = new Map<number, { games: number; starts: number; minSum: number; name: string }>();
  for (const s of stats) {
    const existing = agg.get(s.playerId) ?? {
      games: 0,
      starts: 0,
      minSum: 0,
      name: `${s.player.firstname ?? ""} ${s.player.lastname ?? ""}`.trim(),
    };
    existing.games++;
    if (s.starter) existing.starts++;
    existing.minSum += s.minutes;
    agg.set(s.playerId, existing);
  }

  const candidates: PlayerCandidate[] = Array.from(agg.entries()).map(([playerId, a]) => ({
    playerId,
    name: a.name || `player-${playerId}`,
    games: a.games,
    starts: a.starts,
    startRate: a.games > 0 ? a.starts / a.games : 0,
    avgMinutes: a.games > 0 ? a.minSum / a.games : 0,
    injuryStatus: null,
  }));

  candidates.sort((a, b) => {
    if (b.startRate !== a.startRate) return b.startRate - a.startRate;
    if (b.avgMinutes !== a.avgMinutes) return b.avgMinutes - a.avgMinutes;
    if (b.games !== a.games) return b.games - a.games;
    return a.name.localeCompare(b.name);
  });
  return candidates;
}

function scoreCandidate(c: PlayerCandidate): number {
  return c.startRate * 100 + c.avgMinutes / 60;
}

function projectStarters(candidates: PlayerCandidate[]): PlayerCandidate[] {
  const available = candidates.filter((c) => c.injuryStatus !== "out" && c.injuryStatus !== "doubtful");
  const starters = [...available].sort((a, b) => scoreCandidate(b) - scoreCandidate(a)).slice(0, 5);
  if (starters.length >= 5) return starters;
  const used = new Set(starters.map((s) => s.playerId));
  const fillers = candidates
    .filter((c) => !used.has(c.playerId))
    .sort((a, b) => scoreCandidate(b) - scoreCandidate(a))
    .slice(0, 5 - starters.length);
  return [...starters, ...fillers];
}

async function buildLineupSnapshot(date: string, snapshot: SnapshotSpec, lookbackGames: number): Promise<unknown> {
  const dateMidday = dateStringToUtcMidday(date);
  const games = await prisma.game.findMany({
    where: { date: dateMidday },
    select: {
      id: true,
      tipoffTimeUtc: true,
      homeTeamId: true,
      awayTeamId: true,
      homeTeam: { select: { id: true, name: true, code: true, city: true } },
      awayTeam: { select: { id: true, name: true, code: true, city: true } },
    },
    orderBy: { tipoffTimeUtc: "asc" },
  });

  const teamLookup = await buildTeamLookup();
  const injuryByTeam = await loadInjuryMapForSnapshot(date, snapshot.label, teamLookup);

  const byTeamCandidates = new Map<number, PlayerCandidate[]>();
  const teamIds = Array.from(new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId])));
  for (const teamId of teamIds) {
    byTeamCandidates.set(teamId, await buildTeamCandidates(teamId, date, lookbackGames));
  }

  const projectedGames = games.map((g) => {
    const homeCandidates = (byTeamCandidates.get(g.homeTeamId) ?? []).map((c) => {
      const teamMap = injuryByTeam.get(g.homeTeamId);
      const status = teamMap?.get(normalizeName(c.name)) ?? null;
      return { ...c, injuryStatus: status };
    });
    const awayCandidates = (byTeamCandidates.get(g.awayTeamId) ?? []).map((c) => {
      const teamMap = injuryByTeam.get(g.awayTeamId);
      const status = teamMap?.get(normalizeName(c.name)) ?? null;
      return { ...c, injuryStatus: status };
    });

    const homeProjected = projectStarters(homeCandidates);
    const awayProjected = projectStarters(awayCandidates);

    return {
      gameId: g.id,
      tipoffTimeUtc: g.tipoffTimeUtc?.toISOString() ?? null,
      homeTeam: {
        id: g.homeTeam.id,
        code: g.homeTeam.code,
        name: `${g.homeTeam.city ?? ""} ${g.homeTeam.name ?? ""}`.trim(),
        projectedStarters: homeProjected.map((p) => ({
          playerId: p.playerId,
          name: p.name,
          startRate: Number(p.startRate.toFixed(3)),
          avgMinutes: Number(p.avgMinutes.toFixed(1)),
          injuryStatus: p.injuryStatus,
        })),
      },
      awayTeam: {
        id: g.awayTeam.id,
        code: g.awayTeam.code,
        name: `${g.awayTeam.city ?? ""} ${g.awayTeam.name ?? ""}`.trim(),
        projectedStarters: awayProjected.map((p) => ({
          playerId: p.playerId,
          name: p.name,
          startRate: Number(p.startRate.toFixed(3)),
          avgMinutes: Number(p.avgMinutes.toFixed(1)),
          injuryStatus: p.injuryStatus,
        })),
      },
    };
  });

  return {
    date,
    snapshotLabel: snapshot.label,
    snapshotTimeEt: snapshot.timeEt,
    generatedAt: new Date().toISOString(),
    source: "derived-lineup-projection-v1",
    lookbackGames,
    gameCount: projectedGames.length,
    games: projectedGames,
  };
}

export async function syncLineups(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const date = flags.date;
  const from = flags.from;
  const to = flags.to;
  const snapshots = parseSnapshotSpecs(flags);
  const skipExisting = (flags["skip-existing"] ?? "true") === "true";
  const lookbackGames = Math.max(3, Number(flags["lookback-games"] ?? "8"));
  const delayMs = Math.max(0, Number(flags["delay-ms"] ?? "0"));

  let dates: string[] = [];
  if (date) {
    if (!isDateString(date)) throw new Error(`Invalid --date '${date}'`);
    dates = [date];
  } else if (from && to) {
    if (!isDateString(from) || !isDateString(to) || from > to) {
      throw new Error(`Invalid --from/--to range: ${from} -> ${to}`);
    }
    dates = dateRange(from, to);
  } else {
    throw new Error("Usage: sync:lineups --date YYYY-MM-DD OR --from YYYY-MM-DD --to YYYY-MM-DD");
  }

  const outDir = path.join(getDataDir(), "raw", "lineups");
  await fs.mkdir(outDir, { recursive: true });

  const tasks: Array<{ date: string; snapshot: SnapshotSpec; outPath: string }> = [];
  let skipped = 0;
  for (const d of dates) {
    for (const s of snapshots) {
      const outPath = path.join(outDir, `${d}.${s.label}.json`);
      if (skipExisting && fssync.existsSync(outPath)) {
        skipped++;
        continue;
      }
      tasks.push({ date: d, snapshot: s, outPath });
    }
  }

  console.log("\n=== Sync Lineups (Derived Pregame) ===");
  console.log(`Dates: ${dates[0]} -> ${dates[dates.length - 1]} (${dates.length})`);
  console.log(`Snapshots: ${snapshots.map((s) => `${s.label}@${s.timeEt}`).join(", ")}`);
  console.log(`Lookback games: ${lookbackGames}`);
  console.log(`Skip existing: ${skipExisting}`);
  console.log(`Will build: ${tasks.length}`);
  if (skipExisting) console.log(`Skipped existing: ${skipped}`);

  let ok = 0;
  let failed = 0;
  for (let i = 0; i < tasks.length; i++) {
    const t = tasks[i];
    try {
      const payload = await buildLineupSnapshot(t.date, t.snapshot, lookbackGames);
      await fs.writeFile(t.outPath, JSON.stringify(payload, null, 2), "utf8");
      ok++;
    } catch (err) {
      failed++;
      console.warn(`  ${t.date} (${t.snapshot.label}) failed: ${(err as Error).message}`);
    }
    if ((i + 1) % 25 === 0 || i + 1 === tasks.length) {
      console.log(`Progress: ${i + 1}/${tasks.length} | ok=${ok} failed=${failed}`);
    }
    if (delayMs > 0) await sleep(delayMs);
  }

  console.log(`\nDone. Built ${ok}/${tasks.length} lineup snapshot files; failed=${failed}.`);
}
