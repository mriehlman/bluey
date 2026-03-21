import * as fs from "fs";
import * as path from "path";

function isRepoRoot(dir: string): boolean {
  return (
    fs.existsSync(path.join(dir, "packages")) &&
    fs.existsSync(path.join(dir, "package.json"))
  );
}

function findRepoRootFromCwd(): string {
  let dir = process.cwd();
  while (true) {
    if (isRepoRoot(dir)) return dir;
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return process.cwd();
}

export function getRepoRoot(): string {
  if (process.env.REPO_ROOT) return process.env.REPO_ROOT;
  return findRepoRootFromCwd();
}

export function getDataDir(): string {
  if (process.env.DATA_DIR) return process.env.DATA_DIR;
  return path.join(getRepoRoot(), "data");
}
