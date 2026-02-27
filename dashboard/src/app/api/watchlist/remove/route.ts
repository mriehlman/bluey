import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export async function POST(req: Request) {
  const { patternId } = await req.json();
  if (!patternId) {
    return NextResponse.json({ error: "patternId required" }, { status: 400 });
  }

  await prisma.patternWatchlist.delete({ where: { patternId } });

  return NextResponse.json({ ok: true });
}
