import * as fs from "fs";
import * as path from "path";
import { getDataDir } from "../config/paths";

export type InjuryStatus = "out" | "doubtful" | "questionable" | "probable" | "unknown";

export type TeamInjurySummary = {
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


export function buildTeamAliasLookup(
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

export function loadEarlyInjuriesForDate(
  date: string,
  teamLookup: Map<string, number>,
  cache: Map<string, Map<number, TeamInjurySummary>>,
  dataDir?: string,
): Map<number, TeamInjurySummary> {
  const cached = cache.get(date);
  if (cached) return cached;

  const result = new Map<number, TeamInjurySummary>();
  const dir = dataDir ?? getDataDir();
  const filePath = path.join(dir, "raw", "injuries", `${date}.early.json`);
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
    // Best-effort read
  }

  cache.set(date, result);
  return result;
}

export function computeLineupSignalsFromCounts(injuries: TeamInjurySummary | undefined): {
  certainty: number;
  lateScratchRisk: number;
} {
  if (!injuries) return { certainty: 1, lateScratchRisk: 0 };
  const penalty = injuries.out * 1.0 + injuries.doubtful * 0.75 + injuries.questionable * 0.4 + injuries.probable * 0.1;
  const risk = injuries.out * 0.4 + injuries.doubtful * 0.55 + injuries.questionable * 0.7 + injuries.probable * 0.2;
  return {
    certainty: Math.max(0, Math.min(1, 1 - Math.min(penalty, 5) / 5)),
    lateScratchRisk: Math.max(0, Math.min(1, Math.min(risk, 5) / 5)),
  };
}
