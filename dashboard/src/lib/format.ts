export function fmtDate(d: Date | string | null | undefined): string {
  if (!d) return "—";
  const date = typeof d === "string" ? new Date(d) : d;
  return date.toISOString().slice(0, 10);
}

export function fmtFloat(n: number | null | undefined, decimals = 2): string {
  if (n == null) return "—";
  return n.toFixed(decimals);
}

export function jsonPretty(obj: unknown): string {
  if (obj == null) return "null";
  return JSON.stringify(obj, null, 2);
}

export function parseDateParam(d: string): Date {
  const match = d.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) throw new Error(`Invalid date format: ${d}`);
  return new Date(`${d}T00:00:00.000Z`);
}

/** Get YYYY-MM-DD in Eastern time for a UTC timestamp. NBA uses Eastern for game dates. */
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
