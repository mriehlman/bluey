import { NextResponse } from "next/server";

/** Disabled: exposes internal event data. Only predictions API is public. */
export async function POST() {
  return NextResponse.json({ error: "Not found" }, { status: 404 });
}
