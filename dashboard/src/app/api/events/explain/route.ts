import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { parseDateParam } from "@/lib/format";

export async function POST(req: Request) {
  const { date } = await req.json();
  if (!date) {
    return NextResponse.json({ error: "date required (YYYY-MM-DD)" }, { status: 400 });
  }

  let dateVal: Date;
  try {
    dateVal = parseDateParam(date);
  } catch {
    return NextResponse.json({ error: "Invalid date format" }, { status: 400 });
  }

  // TODO: Call engine's live computation endpoint to get real-time event
  // explanation. For now, return stored events from the database.
  // Future: fetch(`${ENGINE_URL}/api/events/explain?date=${date}`)

  const events = await prisma.nightEvent.findMany({
    where: { date: dateVal },
    orderBy: { eventKey: "asc" },
  });

  return NextResponse.json({
    date,
    source: "stored",
    count: events.length,
    events: events.map((e) => ({
      eventKey: e.eventKey,
      season: e.season,
      value: e.value,
      meta: e.meta,
    })),
  });
}
