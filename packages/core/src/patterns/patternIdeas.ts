import * as fs from "fs/promises";
import { prisma } from "@bluey/db";
import { DISCOVERY_DEFAULTS, VALIDATION_DEFAULTS } from "../config/tuning";
import { matchesConditions } from "./metaModelCore";

type TokenizedGame = {
  gameId: string;
  season: number;
  date: Date;
  tokens: Set<string>;
  outcomes: Set<string>;
};

type SplitStats = {
  n: number;
  wins: number;
  rawHitRate: number;
  posteriorHitRate: number;
  edge: number;
  lift: number;
};

type SeasonSplit =
  | { mode: "default"; train: TokenizedGame[]; val: TokenizedGame[]; forward: TokenizedGame[] }
  | {
      mode: "loso";
      train: TokenizedGame[];
      losoFolds: Array<{ season: number; valRows: TokenizedGame[] }>;
      forward: TokenizedGame[];
    };

type PatternIdea = {
  id: string;
  description?: string;
  outcomeType: string;
  conditions: string[];
  enabled?: boolean;
  tags?: string[];
  notes?: string;
  minSamples?: {
    train?: number;
    val?: number;
    forward?: number;
  };
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--") continue;
    if (arg.startsWith("--") && i + 1 < args.length) {
      const value = args[i + 1];
      if (!value.startsWith("--")) {
        flags[arg.slice(2)] = value;
        i++;
      }
    }
  }
  return flags;
}

function hasOutcome(outcomes: Set<string>, outcomeType: string): boolean {
  if (outcomes.has(outcomeType)) return true;
  if (outcomeType.includes(":")) return false;
  return outcomes.has(`${outcomeType}:game`) || outcomes.has(`${outcomeType}:home`) || outcomes.has(`${outcomeType}:away`);
}

function baseRateFor(rows: TokenizedGame[], outcomeType: string): number {
  if (rows.length === 0) return 0;
  const wins = rows.filter((r) => hasOutcome(r.outcomes, outcomeType)).length;
  return wins / rows.length;
}

function computeStats(
  rows: TokenizedGame[],
  outcomeType: string,
  conditions: string[],
  baseRate: number,
  priorStrength: number,
): SplitStats {
  let n = 0;
  let wins = 0;
  for (const r of rows) {
    if (!matchesConditions(r.tokens, conditions)) continue;
    n++;
    if (hasOutcome(r.outcomes, outcomeType)) wins++;
  }
  const rawHitRate = n > 0 ? wins / n : 0;
  const posteriorHitRate = (wins + priorStrength * baseRate) / Math.max(1, n + priorStrength);
  const edge = posteriorHitRate - baseRate;
  const lift = baseRate > 0 ? posteriorHitRate / baseRate : 0;
  return { n, wins, rawHitRate, posteriorHitRate, edge, lift };
}

function splitRowsBySeason(
  rows: TokenizedGame[],
  trainSeason: number,
  valSeason: number,
  forwardFromSeason: number,
): SeasonSplit {
  const train = rows.filter((r) => r.season === trainSeason);
  const val = rows.filter((r) => r.season === valSeason);
  const forward = rows.filter((r) => r.season >= forwardFromSeason);
  return { mode: "default", train, val, forward };
}

function splitRowsBySeasonLOSO(rows: TokenizedGame[], forwardFromSeason: number): SeasonSplit {
  const losoRows = rows.filter((r) => r.season < forwardFromSeason);
  const forward = rows.filter((r) => r.season >= forwardFromSeason);
  const seasons = [...new Set(losoRows.map((r) => r.season))].sort((a, b) => a - b);
  const losoFolds = seasons.map((season) => ({
    season,
    valRows: losoRows.filter((r) => r.season === season),
  }));
  return { mode: "loso", train: losoRows, losoFolds, forward };
}

async function loadTokenizedGames(): Promise<TokenizedGame[]> {
  const tokenRows = await prisma.$queryRawUnsafe<Array<{ gameId: string; season: number; date: Date; tokens: string[] }>>(
    `SELECT "gameId", "season", "date", "tokens" FROM "GameFeatureToken"`,
  );
  const outcomeRows = await prisma.gameEvent.findMany({
    where: { type: "outcome" },
    select: { gameId: true, eventKey: true, side: true },
  });

  const outcomesByGame = new Map<string, Set<string>>();
  for (const row of outcomeRows) {
    const key = `${row.eventKey}:${row.side}`;
    const set = outcomesByGame.get(row.gameId) ?? new Set<string>();
    set.add(key);
    outcomesByGame.set(row.gameId, set);
  }

  const games: TokenizedGame[] = [];
  for (const row of tokenRows) {
    const outcomes = outcomesByGame.get(row.gameId);
    if (!outcomes || outcomes.size === 0) continue;
    games.push({
      gameId: row.gameId,
      season: row.season,
      date: new Date(row.date),
      tokens: new Set(row.tokens ?? []),
      outcomes,
    });
  }
  return games;
}

function pct(n: number): string {
  return `${(n * 100).toFixed(2)}%`;
}

export async function evaluatePatternIdeas(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const file = flags.file ?? "config/pattern-ideas.json";
  const cvMode = (flags.cv ?? "loso") as "default" | "loso";
  const trainSeason = Number(flags["train-season"] ?? DISCOVERY_DEFAULTS.trainSeason);
  const valSeason = Number(flags["val-season"] ?? DISCOVERY_DEFAULTS.valSeason);
  const forwardFrom = Number(flags["forward-from"] ?? DISCOVERY_DEFAULTS.forwardFrom);
  const minLosoFoldsPass = Math.max(1, Number(flags["min-loso-folds-pass"] ?? DISCOVERY_DEFAULTS.minLosoFoldsPass));
  const priorStrength = Number(flags["prior-strength"] ?? DISCOVERY_DEFAULTS.priorStrength);
  const top = Math.max(1, Number(flags.top ?? 25));

  const minTrainEdge = Number(flags["min-train-edge"] ?? VALIDATION_DEFAULTS.minTrainEdge);
  const minValEdge = Number(flags["min-val-edge"] ?? VALIDATION_DEFAULTS.minValEdge);
  const minForwardEdge = Number(flags["min-forward-edge"] ?? VALIDATION_DEFAULTS.minForwardEdge);
  const minValPosterior = Number(flags["min-val-posterior"] ?? VALIDATION_DEFAULTS.minValPosterior);
  const minForwardPosterior = Number(flags["min-forward-posterior"] ?? VALIDATION_DEFAULTS.minForwardPosterior);
  const defaultMinTrainSamples = Math.max(1, Number(flags["min-train-samples"] ?? VALIDATION_DEFAULTS.minTrainSamples));
  const defaultMinValSamples = Math.max(1, Number(flags["min-val-samples"] ?? VALIDATION_DEFAULTS.minValSamples));
  const defaultMinForwardSamples = Math.max(1, Number(flags["min-forward-samples"] ?? VALIDATION_DEFAULTS.minForwardSamples));

  console.log("\n=== Evaluate Pattern Ideas ===\n");
  console.log(`Loading ideas from: ${file}`);
  const raw = await fs.readFile(file, "utf-8");
  const parsed = JSON.parse(raw) as PatternIdea[];
  const ideas = parsed.filter((i) => i.enabled !== false);
  if (ideas.length === 0) {
    console.log("No enabled pattern ideas found.");
    return;
  }
  console.log(`Loaded ${ideas.length} enabled ideas (${parsed.length - ideas.length} disabled).`);

  const games = await loadTokenizedGames();
  const splits: SeasonSplit =
    cvMode === "loso"
      ? splitRowsBySeasonLOSO(games, forwardFrom)
      : splitRowsBySeason(games, trainSeason, valSeason, forwardFrom);
  if (splits.train.length === 0 || splits.forward.length === 0) {
    throw new Error("Insufficient split coverage. Build tokenized games for train/forward seasons.");
  }
  if (splits.mode === "default" && splits.val.length === 0) {
    throw new Error("Insufficient val coverage for default split.");
  }
  if (splits.mode === "loso" && splits.losoFolds.length < 2) {
    throw new Error("LOSO requires at least 2 seasons before forward.");
  }
  const effectiveMinLosoFolds =
    splits.mode === "loso"
      ? Math.min(minLosoFoldsPass, splits.losoFolds.length)
      : minLosoFoldsPass;

  type Row = {
    id: string;
    outcomeType: string;
    conditions: string[];
    train: SplitStats;
    val: SplitStats;
    forward: SplitStats;
    trainOk: boolean;
    valOk: boolean;
    forwardOk: boolean;
    overallOk: boolean;
    losoPasses?: number;
    description?: string;
    notes?: string;
  };

  const rows: Row[] = [];
  for (const idea of ideas) {
    const minTrainSamples = Math.max(1, Number(idea.minSamples?.train ?? defaultMinTrainSamples));
    const minValSamples = Math.max(1, Number(idea.minSamples?.val ?? defaultMinValSamples));
    const minForwardSamples = Math.max(1, Number(idea.minSamples?.forward ?? defaultMinForwardSamples));

    const trainBase = baseRateFor(splits.train, idea.outcomeType);
    const forwardBase = baseRateFor(splits.forward, idea.outcomeType);
    const train = computeStats(splits.train, idea.outcomeType, idea.conditions ?? [], trainBase, priorStrength);
    const forward = computeStats(splits.forward, idea.outcomeType, idea.conditions ?? [], forwardBase, priorStrength);
    const trainOk = train.n >= minTrainSamples && train.edge >= minTrainEdge;
    const forwardOk =
      forward.n >= minForwardSamples &&
      forward.edge >= minForwardEdge &&
      forward.posteriorHitRate >= minForwardPosterior;

    let val: SplitStats;
    let valOk: boolean;
    let losoPasses: number | undefined;
    if (splits.mode === "default") {
      const valBase = baseRateFor(splits.val, idea.outcomeType);
      val = computeStats(splits.val, idea.outcomeType, idea.conditions ?? [], valBase, priorStrength);
      valOk =
        val.n >= minValSamples &&
        val.edge >= minValEdge &&
        val.posteriorHitRate >= minValPosterior;
    } else {
      const foldStats = splits.losoFolds.map((fold) => {
        const foldBase = baseRateFor(fold.valRows, idea.outcomeType);
        return computeStats(fold.valRows, idea.outcomeType, idea.conditions ?? [], foldBase, priorStrength);
      });
      losoPasses = foldStats.filter(
        (s) => s.n >= minValSamples && s.edge >= minValEdge && s.posteriorHitRate >= minValPosterior,
      ).length;
      valOk = losoPasses >= effectiveMinLosoFolds;
      const totalN = foldStats.reduce((acc, s) => acc + s.n, 0);
      const totalWins = foldStats.reduce((acc, s) => acc + s.wins, 0);
      const meanEdge = foldStats.length > 0 ? foldStats.reduce((acc, s) => acc + s.edge, 0) / foldStats.length : 0;
      const meanPosterior =
        foldStats.length > 0
          ? foldStats.reduce((acc, s) => acc + s.posteriorHitRate, 0) / foldStats.length
          : 0.5;
      val = {
        n: totalN,
        wins: totalWins,
        rawHitRate: totalN > 0 ? totalWins / totalN : 0,
        posteriorHitRate: meanPosterior,
        edge: meanEdge,
        lift: meanEdge > 0 ? 1 + meanEdge : 0,
      };
    }

    rows.push({
      id: idea.id,
      description: idea.description,
      notes: idea.notes,
      outcomeType: idea.outcomeType,
      conditions: idea.conditions ?? [],
      train,
      val,
      forward,
      trainOk,
      valOk,
      forwardOk,
      overallOk: trainOk && valOk && forwardOk,
      losoPasses,
    });
  }

  rows.sort(
    (a, b) =>
      Number(b.overallOk) - Number(a.overallOk) ||
      b.forward.edge - a.forward.edge ||
      b.forward.posteriorHitRate - a.forward.posteriorHitRate ||
      b.forward.n - a.forward.n,
  );

  const passed = rows.filter((r) => r.overallOk).length;
  console.log(
    `Evaluated ${rows.length} ideas | passed=${passed} | cv=${cvMode}` +
      (cvMode === "loso" ? ` folds-pass>=${effectiveMinLosoFolds}` : ""),
  );
  console.log(
    `Thresholds: train(edge>=${minTrainEdge}, n>=${defaultMinTrainSamples}) | val(edge>=${minValEdge}, posterior>=${minValPosterior}, n>=${defaultMinValSamples}) | forward(edge>=${minForwardEdge}, posterior>=${minForwardPosterior}, n>=${defaultMinForwardSamples})`,
  );

  const shown = rows.slice(0, top);
  console.log(`\nTop ${shown.length} ideas:\n`);
  for (const r of shown) {
    const status = r.overallOk ? "PASS" : "FAIL";
    const loso = cvMode === "loso" ? ` | loso=${r.losoPasses}/${effectiveMinLosoFolds}` : "";
    console.log(
      `[${status}] ${r.id}${loso}\n` +
        `  outcome=${r.outcomeType}\n` +
        `  train n=${r.train.n}, post=${pct(r.train.posteriorHitRate)}, edge=${pct(r.train.edge)}\n` +
        `  val   n=${r.val.n}, post=${pct(r.val.posteriorHitRate)}, edge=${pct(r.val.edge)}\n` +
        `  fwd   n=${r.forward.n}, post=${pct(r.forward.posteriorHitRate)}, edge=${pct(r.forward.edge)}\n` +
        `  conditions=${r.conditions.join(" & ")}${r.description ? `\n  idea=${r.description}` : ""}${r.notes ? `\n  notes=${r.notes}` : ""}`,
    );
  }
}
