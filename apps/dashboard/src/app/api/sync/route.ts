import { NextResponse } from "next/server";
import { syncNbaStatsForDate, syncUpcomingFromNba } from "@bluey/core/ingest/syncNbaStats";
import { syncOddsForDate } from "@bluey/core/ingest/syncOdds";
import { syncInjuries } from "@bluey/core/ingest/syncInjuries";
import { syncLineups } from "@bluey/core/ingest/syncLineups";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

export async function POST(req: Request) {
  const url = new URL(req.url);
  const dateStr = url.searchParams.get("date") ?? new Date().toISOString().slice(0, 10);

  const steps: { step: string; ok: boolean; message?: string }[] = [];

  try {
    await syncNbaStatsForDate(dateStr);
    steps.push({ step: "stats", ok: true });
  } catch (e) {
    steps.push({ step: "stats", ok: false, message: String(e).slice(0, 200) });
  }

  try {
    await syncUpcomingFromNba(dateStr);
    steps.push({ step: "games", ok: true });
  } catch (e) {
    steps.push({ step: "games", ok: false, message: String(e).slice(0, 200) });
  }

  try {
    await syncOddsForDate(dateStr);
    steps.push({ step: "odds", ok: true });
  } catch (e) {
    steps.push({ step: "odds", ok: false, message: String(e).slice(0, 200) });
  }

  try {
    await syncInjuries(["--date", dateStr]);
    steps.push({ step: "injuries", ok: true });
  } catch (e) {
    steps.push({ step: "injuries", ok: false, message: String(e).slice(0, 200) });
  }

  try {
    await syncLineups(["--date", dateStr]);
    steps.push({ step: "lineups", ok: true });
  } catch (e) {
    steps.push({ step: "lineups", ok: false, message: String(e).slice(0, 200) });
  }

  const allOk = steps.every((s) => s.ok);
  return NextResponse.json({
    ok: allOk,
    date: dateStr,
    steps,
    message: allOk ? "Sync complete" : "Sync completed with some failures",
  });
}
