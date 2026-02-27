import { resolve } from "path";

export async function readJson<T>(filePath: string): Promise<T> {
  return Bun.file(resolve(filePath)).json() as Promise<T>;
}

export function parseDateOnly(dateStr: string): Date {
  const match = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (!match) throw new Error(`Cannot parse date: ${dateStr}`);
  return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
}

export function parseMinutesToSeconds(value: string | number | null | undefined): number {
  if (value == null || value === "") return 0;

  if (typeof value === "number") {
    return Math.round(value * 60);
  }

  const str = value.trim();
  if (str === "" || str === "0") return 0;

  const colonMatch = str.match(/^(\d+):(\d{1,2})$/);
  if (colonMatch) {
    return Number(colonMatch[1]) * 60 + Number(colonMatch[2]);
  }

  const num = Number(str);
  if (!isNaN(num)) return Math.round(num * 60);

  return 0;
}

export function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

export function isRegularSeasonGame(row: { stage?: string | number; league?: string }): boolean {
  if (String(row.stage) !== "2") return false;
  const league = (row.league ?? "standard").toLowerCase();
  if (league !== "standard") return false;
  return true;
}
