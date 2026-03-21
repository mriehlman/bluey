import { spawn } from "child_process";
import { join } from "path";
import { getRepoRoot } from "../config/paths";

const SCRIPT_PATH = join(
  getRepoRoot(),
  "scripts",
  "nba_fetch.py",
);

interface PlayerStatRow {
  playerId: number;
  playerName: string;
  teamId: number;
  minutes: string;
  points: number;
  rebounds: number;
  assists: number;
  steals: number;
  blocks: number;
  turnovers: number;
  fgm: number;
  fga: number;
  fg3m: number;
  fg3a: number;
  ftm: number;
  fta: number;
  oreb: number;
  dreb: number;
  pf: number;
  plusMinus: number;
}

interface BoxScoreResult {
  gameId: string;
  playerStats: PlayerStatRow[];
  homeTeamId?: number;
  awayTeamId?: number;
  homeScore?: number;
  awayScore?: number;
  date?: string;
}

interface GameResult {
  gameId: string;
  date: string;
  homeTeamId: number;
  awayTeamId: number;
  homeScore: number;
  awayScore: number;
  status: string;
}

async function runPythonScript<T>(args: string[]): Promise<T> {
  return new Promise((resolve, reject) => {
    const proc = spawn("python", [SCRIPT_PATH, ...args], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    proc.stderr.on("data", (data) => {
      stderr += data.toString();
      // Log progress to console
      process.stderr.write(data);
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}: ${stderr}`));
        return;
      }
      try {
        const result = JSON.parse(stdout);
        resolve(result as T);
      } catch (e) {
        reject(new Error(`Failed to parse JSON: ${stdout}`));
      }
    });

    proc.on("error", (err) => {
      reject(err);
    });
  });
}

export async function fetchGamesForDate(date: string): Promise<GameResult[]> {
  return runPythonScript<GameResult[]>(["games-date", date]);
}

export async function fetchBoxScore(gameId: string): Promise<BoxScoreResult> {
  return runPythonScript<BoxScoreResult>(["box-score", gameId]);
}

export async function fetchBoxScoresForDate(date: string): Promise<BoxScoreResult[]> {
  return runPythonScript<BoxScoreResult[]>(["box-scores-date", date]);
}
