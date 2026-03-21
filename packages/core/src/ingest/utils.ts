import { resolve } from "path";

/**
 * Get the calendar date (YYYY-MM-DD) in Eastern time for a UTC timestamp.
 * NBA game dates use Eastern as the reference — a game at 10:30 PM PT March 1
 * is still "March 1" for TV/schedule purposes.
 */
export function getEasternDateFromUtc(utcDate: Date): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(utcDate);
  const year = parts.find((p) => p.type === "year")!.value;
  const month = parts.find((p) => p.type === "month")!.value;
  const day = parts.find((p) => p.type === "day")!.value;
  return `${year}-${month}-${day}`;
}

/**
 * Parse YYYY-MM-DD to a Date at noon UTC (avoids timezone boundary issues for DB storage).
 */
export function dateStringToUtcMidday(dateStr: string): Date {
  return new Date(dateStr + "T12:00:00.000Z");
}

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
