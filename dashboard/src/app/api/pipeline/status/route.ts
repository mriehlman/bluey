import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

/** Disabled: exposes internal DB counts. Only predictions API is public. */
export async function GET() {
  return NextResponse.json({ error: "Not found" }, { status: 404 });
}
