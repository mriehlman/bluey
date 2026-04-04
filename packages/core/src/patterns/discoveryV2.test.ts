import { describe, expect, test } from "bun:test";
import { evaluateBroadPatternSafeguard, resolveAndValidateOutcomeSeasonGate } from "./discoveryV2";

describe("resolveAndValidateOutcomeSeasonGate", () => {
  test("single-season train + minOutcomeTrainSeasons=2 (explicit) fails fast", () => {
    expect(() =>
      resolveAndValidateOutcomeSeasonGate({
        requestedMinOutcomeTrainSeasons: 2,
        trainSeasons: [2023],
        explicitMinOutcomeTrainSeasons: true,
      }),
    ).toThrow(/min-outcome-train-seasons=2 exceeds train split season count=1/i);
  });

  test("single-season train + minOutcomeTrainSeasons=1 (explicit) runs", () => {
    const result = resolveAndValidateOutcomeSeasonGate({
      requestedMinOutcomeTrainSeasons: 1,
      trainSeasons: [2023],
      explicitMinOutcomeTrainSeasons: true,
    });
    expect(result.effectiveMinOutcomeTrainSeasons).toBe(1);
    expect(result.trainSeasonCount).toBe(1);
  });

  test("multi-season train + minOutcomeTrainSeasons=2 (explicit) runs", () => {
    const result = resolveAndValidateOutcomeSeasonGate({
      requestedMinOutcomeTrainSeasons: 2,
      trainSeasons: [2022, 2023, 2023, 2024],
      explicitMinOutcomeTrainSeasons: true,
    });
    expect(result.effectiveMinOutcomeTrainSeasons).toBe(2);
    expect(result.trainSeasonCount).toBe(3);
    expect(result.trainSeasonList).toEqual([2022, 2023, 2024]);
  });

  test("single-season train + implicit minOutcomeTrainSeasons=2 still fails validation", () => {
    expect(() =>
      resolveAndValidateOutcomeSeasonGate({
        requestedMinOutcomeTrainSeasons: 2,
        trainSeasons: [2023],
        explicitMinOutcomeTrainSeasons: false,
      }),
    ).toThrow(/could not auto-adjust to a compatible split/i);
  });
});

describe("evaluateBroadPatternSafeguard", () => {
  test("strong broad TOTAL pattern survives", () => {
    const result = evaluateBroadPatternSafeguard({
      enabled: true,
      trainCoverage: 0.3575,
      forwardPosterior: 0.7031,
      discoveryScore: 0.4130,
      trainToValPosteriorDrop: 0.0157,
    });
    expect(result.applies).toBeTrue();
    expect(result.passed).toBeTrue();
  });

  test("weak broad AWAY_WIN pattern is rejected", () => {
    const result = evaluateBroadPatternSafeguard({
      enabled: true,
      trainCoverage: 0.4163,
      forwardPosterior: 0.5390,
      discoveryScore: 0.1400,
      trainToValPosteriorDrop: 0.0001,
    });
    expect(result.applies).toBeTrue();
    expect(result.passed).toBeFalse();
    expect(result.failedChecks).toContain("forwardPosterior<0.60");
    expect(result.failedChecks).toContain("discoveryScore<0.20");
  });

  test("baseline narrow pattern remains accepted (safeguard not applied)", () => {
    const result = evaluateBroadPatternSafeguard({
      enabled: true,
      trainCoverage: 0.228,
      forwardPosterior: 0.5892,
      discoveryScore: 0.0165,
      trainToValPosteriorDrop: -0.0073,
    });
    expect(result.applies).toBeFalse();
    expect(result.passed).toBeTrue();
  });
});

