import { NextResponse } from "next/server";
import { syncNbaStatsForDate, syncUpcomingFromNba } from "@bluey/core/ingest/syncNbaStats";
import { syncOddsForDate, syncPlayerPropsForDate } from "@bluey/core/ingest/syncOdds";
import { syncInjuries } from "@bluey/core/ingest/syncInjuries";
import { syncLineups } from "@bluey/core/ingest/syncLineups";
import { getEasternDateFromUtc } from "@bluey/core/ingest/utils";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

export async function POST(req: Request) {
  const url = new URL(req.url);
  const requestedDate = url.searchParams.get("date") ?? getEasternDateFromUtc(new Date());
  const todayEastern = getEasternDateFromUtc(new Date());
  const syncToday = (url.searchParams.get("syncToday") ?? "1") !== "0";
  const syncDates = syncToday && requestedDate !== todayEastern
    ? [requestedDate, todayEastern]
    : [requestedDate];

  const steps: { step: string; ok: boolean; message?: string }[] = [];

  for (const dateStr of syncDates) {
    try {
      await syncNbaStatsForDate(dateStr);
      steps.push({ step: `${dateStr}:stats`, ok: true });
    } catch (e) {
      console.error(`[sync] stats failed for ${dateStr}:`, String(e).slice(0, 300));
      steps.push({ step: `${dateStr}:stats`, ok: false, message: String(e).slice(0, 200) });
    }

    try {
      await syncUpcomingFromNba(dateStr);
      steps.push({ step: `${dateStr}:games`, ok: true });
    } catch (e) {
      console.error(`[sync] games failed for ${dateStr}:`, String(e).slice(0, 300));
      steps.push({ step: `${dateStr}:games`, ok: false, message: String(e).slice(0, 200) });
    }

    try {
      await syncOddsForDate(dateStr);
      steps.push({ step: `${dateStr}:odds`, ok: true });
    } catch (e) {
      console.error(`[sync] odds failed for ${dateStr}:`, String(e).slice(0, 300));
      steps.push({ step: `${dateStr}:odds`, ok: false, message: String(e).slice(0, 200) });
    }

    try {
      await syncPlayerPropsForDate(dateStr);
      steps.push({ step: `${dateStr}:player-props`, ok: true });
    } catch (e) {
      console.error(`[sync] player-props failed for ${dateStr}:`, String(e).slice(0, 300));
      steps.push({ step: `${dateStr}:player-props`, ok: false, message: String(e).slice(0, 200) });
    }

    try {
      await syncInjuries(["--date", dateStr]);
      steps.push({ step: `${dateStr}:injuries`, ok: true });
    } catch (e) {
      steps.push({ step: `${dateStr}:injuries`, ok: false, message: String(e).slice(0, 200) });
    }

    try {
      await syncLineups(["--date", dateStr]);
      steps.push({ step: `${dateStr}:lineups`, ok: true });
    } catch (e) {
      steps.push({ step: `${dateStr}:lineups`, ok: false, message: String(e).slice(0, 200) });
    }
  }

  const allOk = steps.every((s) => s.ok);
  const noneOk = steps.every((s) => !s.ok);
  return NextResponse.json({
    ok: allOk,
    date: requestedDate,
    todayEastern,
    syncedDates: syncDates,
    steps,
    message: allOk ? "Sync complete" : "Sync completed with some failures",
  }, { status: allOk ? 200 : noneOk ? 500 : 207 });
}
