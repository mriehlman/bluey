import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export async function POST(req: Request) {
  const { patternId, notes } = await req.json();
  if (!patternId) {
    return NextResponse.json({ error: "patternId required" }, { status: 400 });
  }

  const entry = await prisma.patternWatchlist.upsert({
    where: { patternId },
    create: { patternId, notes: notes ?? null, enabled: true },
    update: { notes: notes ?? null, enabled: true },
  });

  return NextResponse.json(entry);
}
