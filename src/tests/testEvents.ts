/**
 * Event QA — asserts known dates have expected event behavior.
 * Exit nonzero on any failure. No test framework required.
 *
 * Usage: bun run src/tests/testEvents.ts
 */
import { prisma } from "../db/prisma.js";

interface DateAssertion {
  date: string;
  expectations: {
    nightProcessed: boolean;
    minGameCount?: number;
    eventsPresent?: string[];
    eventsAbsent?: string[];
  };
}

// Hardcoded known dates — update these based on your actual data.
// These dates are spread across seasons to catch drift.
const KNOWN_DATES: DateAssertion[] = [
  {
    date: "2022-10-18",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
      eventsPresent: [],
      eventsAbsent: [],
    },
  },
  {
    date: "2022-12-25",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2023-01-15",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2023-10-24",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2023-12-25",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2024-01-15",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2024-10-22",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2024-12-25",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2025-01-15",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
  {
    date: "2025-02-01",
    expectations: {
      nightProcessed: true,
      minGameCount: 1,
    },
  },
];

let passed = 0;
let failed = 0;
const failures: string[] = [];

function assert(condition: boolean, message: string) {
  if (condition) {
    passed++;
  } else {
    failed++;
    failures.push(message);
    console.error(`  FAIL: ${message}`);
  }
}

async function run() {
  console.log("=== Event QA Tests ===\n");

  for (const { date, expectations } of KNOWN_DATES) {
    console.log(`Testing ${date}...`);
    const d = new Date(date);

    const nightProcessed = await prisma.nightEvent.findUnique({
      where: { date_eventKey: { date: d, eventKey: "NIGHT_PROCESSED" } },
    });

    if (expectations.nightProcessed) {
      assert(nightProcessed != null, `${date}: NIGHT_PROCESSED should exist`);

      if (nightProcessed && expectations.minGameCount != null) {
        const meta = nightProcessed.meta as Record<string, unknown> | null;
        const gameCount = (meta?.gameCount as number) ?? 0;
        assert(
          gameCount >= expectations.minGameCount,
          `${date}: gameCount=${gameCount}, expected >= ${expectations.minGameCount}`
        );
      }
    } else {
      assert(nightProcessed == null, `${date}: NIGHT_PROCESSED should NOT exist`);
    }

    if (expectations.eventsPresent) {
      for (const eventKey of expectations.eventsPresent) {
        const ev = await prisma.nightEvent.findUnique({
          where: { date_eventKey: { date: d, eventKey } },
        });
        assert(ev != null, `${date}: ${eventKey} should exist`);
      }
    }

    if (expectations.eventsAbsent) {
      for (const eventKey of expectations.eventsAbsent) {
        const ev = await prisma.nightEvent.findUnique({
          where: { date_eventKey: { date: d, eventKey } },
        });
        assert(ev == null, `${date}: ${eventKey} should NOT exist`);
      }
    }
  }

  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (failures.length > 0) {
    console.error("\nFailures:");
    for (const f of failures) {
      console.error(`  - ${f}`);
    }
    process.exit(1);
  }

  console.log("\nAll event QA checks passed.");
}

run().catch((err) => {
  console.error("Test runner error:", err);
  process.exit(1);
});
