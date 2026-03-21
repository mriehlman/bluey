import { NextResponse } from "next/server";
import * as path from "path";
import { spawn } from "child_process";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

function getRepoRoot(): string {
  const cwd = process.cwd();
  if (cwd.endsWith("dashboard")) return path.join(cwd, "..");
  return cwd;
}

function runCliCommand(
  repoRoot: string,
  npmScript: string,
  args: string[],
): Promise<{ ok: boolean; stderr: string }> {
  return new Promise((resolve) => {
    const proc = spawn("npm", ["run", npmScript, "--", ...args], {
      cwd: repoRoot,
      shell: true,
      env: {
        ...process.env,
        DATA_DIR: path.join(repoRoot, "data"),
        REPO_ROOT: repoRoot,
        CWD: repoRoot,
      },
    });
    let stderr = "";
    proc.stderr?.on("data", (d) => { stderr += d.toString(); });
    proc.on("close", (code) => resolve({ ok: code === 0, stderr }));
  });
}

export async function POST(req: Request) {
  const repoRoot = getRepoRoot();
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date") ?? new Date().toISOString().slice(0, 10);

  const steps: { step: string; ok: boolean; message?: string }[] = [];

  // 1. Sync box scores FIRST (scores + player stats) - needed for hit/miss on completed games
  const statsResult = await runCliCommand(repoRoot, "sync:stats", ["--date", dateStr]);
  steps.push({
    step: "stats",
    ok: statsResult.ok,
    message: statsResult.ok ? undefined : statsResult.stderr.slice(0, 200),
  });

  // 2. Sync upcoming (games + live odds) - skip only when all games have final scores
  const gamesResult = await runCliCommand(repoRoot, "sync:upcoming", ["--date", dateStr, "--skip-existing", "true"]);
  steps.push({
    step: "games+odds",
    ok: gamesResult.ok,
    message: gamesResult.ok ? undefined : gamesResult.stderr.slice(0, 200),
  });

  // 3. Sync injuries - skip if files already exist
  const injuriesResult = await runCliCommand(repoRoot, "sync:injuries", ["--date", dateStr, "--skip-existing", "true"]);
  steps.push({
    step: "injuries",
    ok: injuriesResult.ok,
    message: injuriesResult.ok ? undefined : injuriesResult.stderr.slice(0, 200),
  });

  // 4. Sync lineups - skip if files already exist (default is true, pass explicitly)
  const lineupsResult = await runCliCommand(repoRoot, "sync:lineups", ["--date", dateStr, "--skip-existing", "true"]);
  steps.push({
    step: "lineups",
    ok: lineupsResult.ok,
    message: lineupsResult.ok ? undefined : lineupsResult.stderr.slice(0, 200),
  });

  const allOk = steps.every((s) => s.ok);
  return NextResponse.json({
    ok: allOk,
    date: dateStr,
    steps,
    message: allOk ? "Sync complete" : "Sync completed with some failures",
  });
}
