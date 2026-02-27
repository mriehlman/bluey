import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export async function POST(req: Request) {
  const { patternId, enabled, notes } = await req.json();
  if (!patternId) {
    return NextResponse.json({ error: "patternId required" }, { status: 400 });
  }

  const data: Record<string, unknown> = {};
  if (typeof enabled === "boolean") data.enabled = enabled;
  if (typeof notes === "string") data.notes = notes;

  const entry = await prisma.patternWatchlist.update({
    where: { patternId },
    data,
  });

  return NextResponse.json(entry);
}
