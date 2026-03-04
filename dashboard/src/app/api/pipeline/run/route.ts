import { NextResponse } from "next/server";

/** Disabled: pipeline runs are internal. Only predictions API is public. */
export async function POST() {
  return NextResponse.json({ error: "Not found" }, { status: 404 });
}

/*
// Original implementation kept for reference - re-enable for admin use
import { spawn } from "child_process";
import path from "path";

export const maxDuration = 300;

const STEPS = [
  "sync:odds",
  "sync:player-props",
  "build:night-aggregates",
  "build:nightly-events",
  "build:nights",
  "search:patterns",
  "patterns:dedupe",
  "build:game-context",
  "build:game-events",
  "search:game-patterns",
  "predict:games",
  "predict:players",
] as const;

type StepName = (typeof STEPS)[number];

function isValidStep(s: string): s is StepName {
  return (STEPS as readonly string[]).includes(s);
}

const PROJECT_ROOT = path.resolve(process.cwd(), "..");

export async function POST(req: Request) {
  const body = await req.json();
  const { step, flags = {} } = body as {
    step: string;
    flags?: Record<string, string>;
  };

  if (!step || !isValidStep(step)) {
    return Response.json(
      { error: `Invalid step. Valid: ${STEPS.join(", ")}` },
      { status: 400 },
    );
  }

  const args = ["run", "src/cli/index.ts", step];
  for (const [k, v] of Object.entries(flags)) {
    if (v !== "" && v !== undefined) {
      args.push(`--${k}`, v);
    }
  }

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;

      function close() {
        if (!closed) {
          closed = true;
          controller.close();
        }
      }

      function push(text: string) {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(text));
        } catch {
          closed = true;
        }
      }

      const child = spawn("bun", args, {
        cwd: PROJECT_ROOT,
        env: { ...process.env },
        stdio: ["ignore", "pipe", "pipe"],
        shell: true,
      });

      push(`=== Running: ${step} ===\n`);

      child.stdout.on("data", (chunk: Buffer) => {
        push(chunk.toString());
      });

      child.stderr.on("data", (chunk: Buffer) => {
        push(chunk.toString());
      });

      child.on("error", (err) => {
        push(`\n=== SPAWN ERROR: ${err.message} ===\n`);
        close();
      });

      child.on("close", (code) => {
        if (code === 0) {
          push(`\n=== Completed (exit 0) ===\n`);
        } else {
          push(`\n=== Failed (exit ${code}) ===\n`);
        }
        close();
      });
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-cache",
      "X-Content-Type-Options": "nosniff",
    },
  });
}
