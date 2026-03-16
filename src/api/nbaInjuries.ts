import { spawn } from "child_process";
import { join } from "path";

const SCRIPT_PATH = join(process.cwd(), "scripts", "nba_injuries_fetch.py");

export type InjuryReportRow = Record<string, unknown>;

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
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Injury fetcher exited with code ${code}: ${stderr}`));
        return;
      }
      try {
        const parsed = JSON.parse(stdout) as T;
        resolve(parsed);
      } catch {
        reject(new Error(`Failed to parse injury fetch JSON: ${stdout}`));
      }
    });

    proc.on("error", (err) => {
      reject(err);
    });
  });
}

export async function fetchInjuryReportForDate(
  date: string,
  snapshotTimeEt = "17:30",
): Promise<InjuryReportRow[]> {
  return runPythonScript<InjuryReportRow[]>(["report-date", date, snapshotTimeEt]);
}
