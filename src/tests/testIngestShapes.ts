/**
 * Ingest Shape Contract Tests — asserts the raw JSON data files
 * maintain their expected structure so ingestion won't silently break.
 * Exit nonzero on any failure.
 *
 * Usage: bun run src/tests/testIngestShapes.ts
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";

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

function run() {
  console.log("=== Ingest Shape Contract Tests ===\n");

  const dataDir = join(process.cwd(), "data");

  // --- games.json shape ---
  console.log("Checking games.json shape...");
  const gamesPath = join(dataDir, "games.json");
  assert(existsSync(gamesPath), "data/games.json exists");

  if (existsSync(gamesPath)) {
    const raw = JSON.parse(readFileSync(gamesPath, "utf-8"));

    assert(Array.isArray(raw?.games), "games.json has top-level { games: [...] }");

    if (Array.isArray(raw.games) && raw.games.length > 0) {
      const sample = raw.games[0];

      assert(sample.id != null, "game has .id");
      assert(sample.date?.start != null, "game has .date.start");
      assert(sample.teams?.visitors != null, "game has .teams.visitors");
      assert(sample.teams?.home != null, "game has .teams.home");
      assert(sample.teams?.visitors?.id != null, "game.teams.visitors has .id");
      assert(sample.teams?.home?.id != null, "game.teams.home has .id");
      assert(sample.scores?.visitors != null, "game has .scores.visitors");
      assert(sample.scores?.home != null, "game has .scores.home");
      assert(sample.season != null, "game has .season");
      assert(sample.stage != null, "game has .stage");
      assert(sample.league != null, "game has .league");

      assert(
        typeof sample.scores?.visitors?.points === "number" ||
          sample.scores?.visitors?.points != null,
        "scores.visitors has points"
      );
      assert(
        typeof sample.scores?.home?.points === "number" ||
          sample.scores?.home?.points != null,
        "scores.home has points"
      );

      console.log(`  Sampled game id=${sample.id}, date=${sample.date?.start}`);
    }
  }

  // --- playerstats.json shape ---
  console.log("\nChecking playerstats.json shape...");
  const statsPath = join(dataDir, "playerstats.json");
  assert(existsSync(statsPath), "data/playerstats.json exists");

  if (existsSync(statsPath)) {
    const raw = JSON.parse(readFileSync(statsPath, "utf-8"));

    assert(Array.isArray(raw), "playerstats.json is an array of wrappers");

    if (Array.isArray(raw) && raw.length > 0) {
      const wrapper = raw[0];
      assert(Array.isArray(wrapper?.response), "wrapper has .response[]");

      if (Array.isArray(wrapper.response) && wrapper.response.length > 0) {
        const stat = wrapper.response[0];

        assert(stat.player != null, "stat has .player");
        assert(stat.player?.id != null, "stat.player has .id");
        assert(stat.team != null, "stat has .team");
        assert(stat.team?.id != null, "stat.team has .id");
        assert(stat.game != null, "stat has .game");
        assert(stat.game?.id != null, "stat.game has .id");
        assert(stat.points != null, "stat has .points");
        assert(stat.assists != null, "stat has .assists");
        assert(stat.totReb != null || stat.rebounds != null, "stat has rebounds (totReb or rebounds)");
        assert(stat.min != null, "stat has .min");

        console.log(
          `  Sampled stat: player=${stat.player?.id}, game=${stat.game?.id}`
        );
      }
    }
  }

  // --- Summary ---
  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (failures.length > 0) {
    console.error("\nFailures:");
    for (const f of failures) {
      console.error(`  - ${f}`);
    }
    process.exit(1);
  }

  console.log("\nAll shape contract checks passed.");
}

run();
