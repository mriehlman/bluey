import { prisma } from "../db/prisma.js";
import {
  DEFAULT_META_MODEL_RELATIVE_PATH,
  deriveMetaContextFeatures,
  outcomeFamily,
  saveMetaModel,
  scoreMetaModel,
  loadMetaModel,
  applyMetaCalibrator,
  type MetaCalibrator,
  type MetaModel,
} from "./metaModelCore.js";
import { getDiscoveryV2MatchesForGameIds } from "./discoveryV2.js";

type MetaRow = {
  outcomeType: string;
  conditions: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
  hitBool: boolean;
  date: Date;
};

type Sample = {
  outcomeType: string;
  x: number[];
  family: string;
  y: number;
  date: Date;
};

type Metrics = { logLoss: number; brier: number; accuracy: number };
type LinearModel = {
  means: number[];
  stds: number[];
  bias: number;
  weights: number[];
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

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function shuffleArray<T>(arr: T[]): void {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
}

function cosineDecayLr(baseLr: number, epoch: number, totalEpochs: number, minLr: number = 0.001): number {
  const progress = epoch / Math.max(1, totalEpochs - 1);
  return minLr + (baseLr - minLr) * 0.5 * (1 + Math.cos(Math.PI * progress));
}

function safeFeature(raw: number, fallback: number): number {
  return Number.isFinite(raw) ? raw : fallback;
}

function buildFeatureVector(row: {
  outcomeType: string;
  conditions?: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
}): number[] {
  const derived = deriveMetaContextFeatures({
    outcomeType: row.outcomeType,
    conditions: row.conditions ?? [],
  });
  return [
    safeFeature(row.posteriorHitRate, 0.5),
    safeFeature(row.edge, 0),
    safeFeature(row.score, 0),
    Math.log(Math.max(1, safeFeature(row.n, 1))),
    safeFeature(derived.portabilityScore, 0.5),
    safeFeature(derived.dependencyRisk, 0.5),
    safeFeature(derived.recentAtoT, 1),
  ];
}

function std(values: number[], mu: number): number {
  if (values.length === 0) return 1;
  const v = values.reduce((acc, val) => acc + (val - mu) ** 2, 0) / values.length;
  const s = Math.sqrt(v);
  return s > 1e-9 ? s : 1;
}

function computeMetrics(
  rows: Sample[],
  model: Pick<MetaModel, "means" | "stds" | "bias" | "weights" | "familyWeights">,
): Metrics {
  if (rows.length === 0) {
    return { logLoss: 0, brier: 0, accuracy: 0 };
  }
  let logLoss = 0;
  let brier = 0;
  let correct = 0;
  for (const row of rows) {
    const norm = row.x.map((v, i) => {
      const sd = model.stds[i] ?? 1;
      const mu = model.means[i] ?? 0;
      return (v - mu) / sd;
    });
    const familyBias = model.familyWeights[row.family] ?? 0;
    const z =
      model.bias +
      model.weights.reduce((sum, w, idx) => sum + w * (norm[idx] ?? 0), 0) +
      familyBias;
    const p = Math.min(1 - 1e-6, Math.max(1e-6, 1 / (1 + Math.exp(-z))));
    logLoss += -(row.y * Math.log(p) + (1 - row.y) * Math.log(1 - p));
    brier += (p - row.y) ** 2;
    if ((p >= 0.5 ? 1 : 0) === row.y) correct++;
  }
  return {
    logLoss: logLoss / rows.length,
    brier: brier / rows.length,
    accuracy: correct / rows.length,
  };
}

function computeMetricsSimple(
  rows: Sample[],
  model: LinearModel,
): Metrics {
  if (rows.length === 0) {
    return { logLoss: 0, brier: 0, accuracy: 0 };
  }
  let logLoss = 0;
  let brier = 0;
  let correct = 0;
  for (const row of rows) {
    const x = row.x.map((v, i) => {
      const sd = model.stds[i] ?? 1;
      const mu = model.means[i] ?? 0;
      return (v - mu) / sd;
    });
    const z =
      model.bias +
      model.weights.reduce((sum, w, idx) => sum + w * (x[idx] ?? 0), 0);
    const p = Math.min(1 - 1e-6, Math.max(1e-6, 1 / (1 + Math.exp(-z))));
    logLoss += -(row.y * Math.log(p) + (1 - row.y) * Math.log(1 - p));
    brier += (p - row.y) ** 2;
    if ((p >= 0.5 ? 1 : 0) === row.y) correct++;
  }
  return {
    logLoss: logLoss / rows.length,
    brier: brier / rows.length,
    accuracy: correct / rows.length,
  };
}

function fitLinearModel(
  train: Sample[],
  epochs: number,
  lr: number,
  l2: number,
): LinearModel {
  const dims = train[0]?.x.length ?? 0;
  const means = Array.from({ length: dims }, (_, i) => mean(train.map((s) => s.x[i] ?? 0)));
  const stds = Array.from({ length: dims }, (_, i) => std(train.map((s) => s.x[i] ?? 0), means[i] ?? 0));

  let bias = 0;
  const weights = Array.from({ length: dims }, () => 0);
  const shuffled = [...train];

  for (let epoch = 0; epoch < epochs; epoch++) {
    shuffleArray(shuffled);
    const epochLr = cosineDecayLr(lr, epoch, epochs);
    let gBias = 0;
    const gW = Array.from({ length: dims }, () => 0);
    for (const row of shuffled) {
      const x = row.x.map((v, i) => (v - (means[i] ?? 0)) / (stds[i] ?? 1));
      const z =
        bias +
        weights.reduce((sum, w, idx) => sum + w * (x[idx] ?? 0), 0);
      const p = 1 / (1 + Math.exp(-z));
      const err = p - row.y;
      gBias += err;
      for (let i = 0; i < dims; i++) {
        gW[i] += err * (x[i] ?? 0);
      }
    }
    const invN = 1 / shuffled.length;
    bias -= epochLr * invN * gBias;
    for (let i = 0; i < dims; i++) {
      weights[i] -= epochLr * (invN * gW[i] + l2 * weights[i]);
    }
  }
  return { means, stds, bias, weights };
}

function fitPlattCalibrator(
  probs: number[],
  labels: number[],
  epochs: number = 300,
  lr: number = 0.05,
): { a: number; b: number } {
  let a = 1;
  let b = 0;
  if (probs.length === 0 || labels.length === 0 || probs.length !== labels.length) {
    return { a, b };
  }
  for (let e = 0; e < epochs; e++) {
    let gA = 0;
    let gB = 0;
    for (let i = 0; i < probs.length; i++) {
      const p = Math.min(1 - 1e-6, Math.max(1e-6, probs[i]));
      const z = Math.log(p / (1 - p));
      const y = labels[i];
      const pred = 1 / (1 + Math.exp(-(a * z + b)));
      const err = pred - y;
      gA += err * z;
      gB += err;
    }
    const invN = 1 / probs.length;
    a -= lr * invN * gA;
    b -= lr * invN * gB;
  }
  return { a, b };
}

function fitIsotonicCalibrator(
  probs: number[],
  labels: number[],
): MetaCalibrator {
  if (probs.length === 0 || labels.length === 0 || probs.length !== labels.length) {
    return { type: "isotonic", x: [0, 1], y: [0.5, 0.5] };
  }
  const pairs = probs
    .map((p, i) => ({ p: Math.min(1 - 1e-6, Math.max(1e-6, p)), y: labels[i] }))
    .sort((a, b) => a.p - b.p);
  type Block = { sumY: number; count: number; minX: number; maxX: number };
  const blocks: Block[] = [];
  for (const pair of pairs) {
    blocks.push({ sumY: pair.y, count: 1, minX: pair.p, maxX: pair.p });
    while (blocks.length >= 2) {
      const last = blocks[blocks.length - 1];
      const prev = blocks[blocks.length - 2];
      const lastMean = last.sumY / last.count;
      const prevMean = prev.sumY / prev.count;
      if (prevMean <= lastMean + 1e-12) break;
      blocks.splice(blocks.length - 2, 2, {
        sumY: prev.sumY + last.sumY,
        count: prev.count + last.count,
        minX: prev.minX,
        maxX: last.maxX,
      });
    }
  }
  const x: number[] = [0];
  const y: number[] = [Math.min(1 - 1e-6, Math.max(1e-6, blocks[0] ? blocks[0].sumY / blocks[0].count : 0.5))];
  for (const block of blocks) {
    const meanY = Math.min(1 - 1e-6, Math.max(1e-6, block.sumY / block.count));
    x.push(block.maxX);
    y.push(meanY);
  }
  x.push(1);
  y.push(y[y.length - 1] ?? 0.5);
  return { type: "isotonic", x, y };
}

type CalibrationMethod = "platt" | "isotonic" | "auto";

function calibratorByMethod(
  method: Exclude<CalibrationMethod, "auto">,
  probs: number[],
  labels: number[],
): MetaCalibrator {
  return method === "isotonic"
    ? fitIsotonicCalibrator(probs, labels)
    : fitPlattCalibrator(probs, labels);
}

function selectBestCalibrator(
  requested: CalibrationMethod,
  probs: number[],
  labels: number[],
): { methodUsed: "platt" | "isotonic"; calibrator: MetaCalibrator } {
  if (requested === "platt") {
    return { methodUsed: "platt", calibrator: fitPlattCalibrator(probs, labels) };
  }
  if (requested === "isotonic") {
    return { methodUsed: "isotonic", calibrator: fitIsotonicCalibrator(probs, labels) };
  }
  const candidates: Array<{ methodUsed: "platt" | "isotonic"; calibrator: MetaCalibrator }> = [
    { methodUsed: "platt", calibrator: calibratorByMethod("platt", probs, labels) },
    { methodUsed: "isotonic", calibrator: calibratorByMethod("isotonic", probs, labels) },
  ];
  let best = candidates[0];
  let bestScore = Number.POSITIVE_INFINITY;
  for (const candidate of candidates) {
    const rows = probs.map((p, i) => ({ y: labels[i], p: applyMetaCalibrator(candidate.calibrator, p) }));
    const metric = computeBinaryMetrics(rows).logLoss;
    if (metric < bestScore) {
      bestScore = metric;
      best = candidate;
    }
  }
  return best;
}

async function loadMetaRows(
  statuses: string[],
  fromDate?: Date,
  toDate?: Date,
): Promise<MetaRow[]> {
  const statusClause = statuses.map((s) => `'${s.replaceAll("'", "''")}'`).join(",");
  const where: string[] = [`p."status" IN (${statusClause})`];
  if (fromDate) where.push(`h."date" >= '${fromDate.toISOString().slice(0, 10)}'`);
  if (toDate) where.push(`h."date" <= '${toDate.toISOString().slice(0, 10)}'`);

  const rows = await prisma.$queryRawUnsafe<MetaRow[]>(
    `SELECT
      p."outcomeType" as "outcomeType",
      p."conditions" as "conditions",
      p."posteriorHitRate" as "posteriorHitRate",
      p."edge" as "edge",
      p."score" as "score",
      p."n" as "n",
      h."hitBool" as "hitBool",
      h."date" as "date"
     FROM "PatternV2Hit" h
     JOIN "PatternV2" p ON p."id" = h."patternId"
     WHERE ${where.join(" AND ")}`,
  );
  return rows;
}

function monthStartUtc(d: Date): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));
}

function addMonthsUtc(d: Date, delta: number): Date {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + delta, 1));
}

function monthKey(d: Date): string {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}

function computeBinaryMetrics(rows: Array<{ y: number; p: number }>): Metrics {
  if (rows.length === 0) return { logLoss: 0, brier: 0, accuracy: 0 };
  let ll = 0;
  let brier = 0;
  let correct = 0;
  for (const r of rows) {
    const p = Math.min(1 - 1e-6, Math.max(1e-6, r.p));
    ll += -(r.y * Math.log(p) + (1 - r.y) * Math.log(1 - p));
    brier += (p - r.y) ** 2;
    if ((p >= 0.5 ? 1 : 0) === r.y) correct++;
  }
  return { logLoss: ll / rows.length, brier: brier / rows.length, accuracy: correct / rows.length };
}

function buildFamilyAwareModel(
  train: Sample[],
  validation: Sample[],
  epochs: number,
  lr: number,
  l2: number,
): MetaModel {
  const global = fitLinearModel(train, epochs, lr, l2);
  const byFamily = new Map<string, Sample[]>();
  for (const s of train) {
    const arr = byFamily.get(s.family) ?? [];
    arr.push(s);
    byFamily.set(s.family, arr);
  }
  const familyModels: NonNullable<MetaModel["familyModels"]> = {};
  for (const [family, famTrain] of byFamily) {
    const famValidation = validation.filter((s) => s.family === family);
    if (famTrain.length < 120 || famValidation.length < 20) continue;
    const fm = fitLinearModel(famTrain, epochs, lr, l2);
    familyModels[family] = {
      sampleCount: famTrain.length + famValidation.length,
      trainCount: famTrain.length,
      validationCount: famValidation.length,
      means: fm.means,
      stds: fm.stds,
      bias: fm.bias,
      weights: fm.weights,
      metrics: {
        train: computeMetricsSimple(famTrain, fm),
        validation: computeMetricsSimple(famValidation, fm),
      },
    };
  }
  return {
    version: 2,
    createdAt: new Date().toISOString(),
    source: "PatternV2Hit/PatternV2",
    sampleCount: train.length + validation.length,
    trainCount: train.length,
    validationCount: validation.length,
    featureNames: [
      "posteriorHitRate",
      "edge",
      "score",
      "logN",
      "portabilityScore",
      "dependencyRisk",
      "recentAtoT",
    ],
    means: global.means,
    stds: global.stds,
    bias: global.bias,
    weights: global.weights,
    familyWeights: {},
    familyModels,
    metrics: {
      train: computeMetricsSimple(train, global),
      validation: computeMetricsSimple(validation, global),
    },
  };
}

export async function trainMetaModel(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const statuses = (flags.statuses ?? "deployed,validated")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const valDays = Math.max(7, Number(flags["val-days"] ?? 45));
  const epochs = Math.max(50, Number(flags.epochs ?? 450));
  const lr = Number(flags.lr ?? 0.03);
  const l2 = Number(flags.l2 ?? 0.0008);
  const modelPath = flags.out ?? DEFAULT_META_MODEL_RELATIVE_PATH;
  const calibrationMethod = (flags["calibration-method"] ?? "platt") as CalibrationMethod;
  const fromDate = flags.from ? new Date(`${flags.from}T00:00:00Z`) : undefined;
  const toDate = flags.to ? new Date(`${flags.to}T00:00:00Z`) : undefined;
  if (!["platt", "isotonic", "auto"].includes(calibrationMethod)) {
    throw new Error(`Invalid --calibration-method '${calibrationMethod}'. Use platt|isotonic|auto.`);
  }

  console.log("\n=== Train Meta Model ===\n");
  const rows = await loadMetaRows(statuses, fromDate, toDate);
  if (rows.length < 500) {
    throw new Error(`Not enough training rows (${rows.length}). Need at least 500.`);
  }

  const samples: Sample[] = rows.map((r) => ({
    outcomeType: r.outcomeType,
    x: buildFeatureVector({
      outcomeType: r.outcomeType,
      conditions: r.conditions ?? [],
      posteriorHitRate: r.posteriorHitRate ?? 0.5,
      edge: r.edge ?? 0,
      score: r.score ?? 0,
      n: r.n ?? 1,
    }),
    family: outcomeFamily(r.outcomeType),
    y: r.hitBool ? 1 : 0,
    date: new Date(r.date),
  }));
  samples.sort((a, b) => a.date.getTime() - b.date.getTime());
  const maxDate = samples[samples.length - 1]?.date;
  const cutoff = maxDate ? new Date(maxDate.getTime() - valDays * 86_400_000) : new Date();
  const calDays = Math.floor(valDays / 2);
  const calCutoff = maxDate ? new Date(maxDate.getTime() - calDays * 86_400_000) : new Date();
  const train = samples.filter((s) => s.date < cutoff);
  const validation = samples.filter((s) => s.date >= cutoff && s.date < calCutoff);
  const calibration = samples.filter((s) => s.date >= calCutoff);
  if (train.length < 300 || validation.length < 50 || calibration.length < 50) {
    throw new Error(
      `Insufficient 3-way split: train=${train.length}, val=${validation.length}, cal=${calibration.length}. Adjust --val-days.`,
    );
  }
  console.log(`3-way split: train=${train.length}, val=${validation.length}, cal=${calibration.length}`);

  const dims = train[0]?.x.length ?? 0;
  const means = Array.from({ length: dims }, (_, i) => mean(train.map((s) => s.x[i] ?? 0)));
  const stds = Array.from({ length: dims }, (_, i) => std(train.map((s) => s.x[i] ?? 0), means[i] ?? 0));

  let bias = 0;
  const weights = Array.from({ length: dims }, () => 0);
  const familyWeights: Record<string, number> = {};
  const shuffled = [...train];

  for (let epoch = 0; epoch < epochs; epoch++) {
    shuffleArray(shuffled);
    const epochLr = cosineDecayLr(lr, epoch, epochs);
    let gBias = 0;
    const gW = Array.from({ length: dims }, () => 0);
    const gF: Record<string, number> = {};

    for (const row of shuffled) {
      const x = row.x.map((v, i) => (v - (means[i] ?? 0)) / (stds[i] ?? 1));
      const fWeight = familyWeights[row.family] ?? 0;
      const z =
        bias +
        weights.reduce((sum, w, idx) => sum + w * (x[idx] ?? 0), 0) +
        fWeight;
      const p = 1 / (1 + Math.exp(-z));
      const err = p - row.y;
      gBias += err;
      for (let i = 0; i < dims; i++) {
        gW[i] += err * (x[i] ?? 0);
      }
      gF[row.family] = (gF[row.family] ?? 0) + err;
    }

    const invN = 1 / shuffled.length;
    bias -= epochLr * invN * gBias;
    for (let i = 0; i < dims; i++) {
      weights[i] -= epochLr * (invN * gW[i] + l2 * weights[i]);
    }
    for (const [family, grad] of Object.entries(gF)) {
      familyWeights[family] = (familyWeights[family] ?? 0) - epochLr * (invN * grad + l2 * (familyWeights[family] ?? 0));
    }
  }

  const model: MetaModel = {
    version: 2,
    createdAt: new Date().toISOString(),
    source: "PatternV2Hit/PatternV2",
    sampleCount: samples.length,
    trainCount: train.length,
    validationCount: validation.length,
    featureNames: [
      "posteriorHitRate",
      "edge",
      "score",
      "logN",
      "portabilityScore",
      "dependencyRisk",
      "recentAtoT",
    ],
    means,
    stds,
    bias,
    weights,
    familyWeights,
    metrics: {
      train: computeMetrics(train, { means, stds, bias, weights, familyWeights }),
      validation: computeMetrics(validation, { means, stds, bias, weights, familyWeights }),
    },
  };

  const byFamily = new Map<string, Sample[]>();
  for (const s of samples) {
    const arr = byFamily.get(s.family) ?? [];
    arr.push(s);
    byFamily.set(s.family, arr);
  }
  const familyModels: NonNullable<MetaModel["familyModels"]> = {};
  for (const [family, rowsForFamily] of byFamily) {
    if (rowsForFamily.length < 250) continue;
    const famTrain = rowsForFamily.filter((s) => s.date < cutoff);
    const famValidation = rowsForFamily.filter((s) => s.date >= cutoff);
    if (famTrain.length < 150 || famValidation.length < 50) continue;
    const fm = fitLinearModel(famTrain, epochs, lr, l2);
    familyModels[family] = {
      sampleCount: rowsForFamily.length,
      trainCount: famTrain.length,
      validationCount: famValidation.length,
      means: fm.means,
      stds: fm.stds,
      bias: fm.bias,
      weights: fm.weights,
      metrics: {
        train: computeMetricsSimple(famTrain, fm),
        validation: computeMetricsSimple(famValidation, fm),
      },
    };
  }
  if (Object.keys(familyModels).length > 0) {
    model.familyModels = familyModels;
  }

  const calPreds = calibration.map((s) =>
    scoreMetaModel(model, {
      outcomeType: s.outcomeType,
      posteriorHitRate: s.x[0] ?? 0.5,
      edge: s.x[1] ?? 0,
      score: s.x[2] ?? 0,
      n: Math.max(1, Math.exp(s.x[3] ?? 0)),
      portabilityScore: s.x[4],
      dependencyRisk: s.x[5],
      recentAtoT: s.x[6],
    }, { calibrated: false }),
  );
  const globalCalibration = selectBestCalibrator(
    calibrationMethod,
    calPreds,
    calibration.map((s) => s.y),
  );
  model.calibrators = {
    global: globalCalibration.calibrator,
    family: {},
  };
  console.log(`Calibration global: method=${globalCalibration.methodUsed}, n=${calibration.length} (separate holdout)`);
  for (const family of [...new Set(calibration.map((s) => s.family))]) {
    const famRows = calibration.filter((s) => s.family === family);
    if (famRows.length < 40) continue;
    const famPreds = famRows.map((s) =>
      scoreMetaModel(model, {
        outcomeType: s.outcomeType,
        posteriorHitRate: s.x[0] ?? 0.5,
        edge: s.x[1] ?? 0,
        score: s.x[2] ?? 0,
        n: Math.max(1, Math.exp(s.x[3] ?? 0)),
        portabilityScore: s.x[4],
        dependencyRisk: s.x[5],
        recentAtoT: s.x[6],
      }, { calibrated: false }),
    );
    const familyCalibration = selectBestCalibrator(
      calibrationMethod,
      famPreds,
      famRows.map((s) => s.y),
    );
    model.calibrators.family![family] = familyCalibration.calibrator;
    console.log(`Calibration family=${family}: method=${familyCalibration.methodUsed}, n=${famRows.length}`);
  }

  const outPath = await saveMetaModel(model, modelPath);
  console.log(`Rows: total=${samples.length}, train=${train.length}, val=${validation.length}, cal=${calibration.length}`);
  console.log(
    `Validation: logloss=${model.metrics.validation.logLoss.toFixed(4)}, brier=${model.metrics.validation.brier.toFixed(4)}, accuracy=${(model.metrics.validation.accuracy * 100).toFixed(1)}%`,
  );
  if (model.familyModels) {
    console.log("Family models:");
    for (const [family, fm] of Object.entries(model.familyModels)) {
      console.log(
        `  ${family}: n=${fm.sampleCount}, val logloss=${fm.metrics.validation.logLoss.toFixed(4)}, brier=${fm.metrics.validation.brier.toFixed(4)}, acc=${(fm.metrics.validation.accuracy * 100).toFixed(1)}%`,
      );
    }
  }
  console.log(`Saved model -> ${outPath}`);
}

export async function predictMetaScore(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const dateStr = flags.date ?? new Date().toISOString().slice(0, 10);
  const modelPath = flags.model ?? DEFAULT_META_MODEL_RELATIVE_PATH;
  const topN = Math.max(1, Number(flags.top ?? 5));
  const model = await loadMetaModel(modelPath);
  if (!model) {
    throw new Error(`No meta model found at '${modelPath}'. Run train:meta-model first.`);
  }

  const targetDate = new Date(`${dateStr}T00:00:00Z`);
  const games = await prisma.game.findMany({
    where: { date: targetDate },
    include: { homeTeam: true, awayTeam: true },
    orderBy: { tipoffTimeUtc: "asc" },
  });
  if (games.length === 0) {
    console.log(`No games found for ${dateStr}`);
    return;
  }
  const matchesByGame = await getDiscoveryV2MatchesForGameIds(games.map((g) => g.id));

  console.log(`\n=== Meta Scores for ${dateStr} ===\n`);
  for (const game of games) {
    const home = game.homeTeam.code ?? game.homeTeam.name ?? String(game.homeTeamId);
    const away = game.awayTeam.code ?? game.awayTeam.name ?? String(game.awayTeamId);
    const matches = matchesByGame.get(game.id) ?? [];
    if (matches.length === 0) {
      console.log(`${away} @ ${home}: no discovery v2 matches`);
      continue;
    }
    const ranked = matches
      .map((m) => ({
        ...m,
        metaScore: scoreMetaModel(model, {
          outcomeType: m.outcomeType,
          conditions: m.conditions,
          posteriorHitRate: m.posteriorHitRate,
          edge: m.edge,
          score: m.score,
          n: m.n,
        }),
      }))
      .sort((a, b) => b.metaScore - a.metaScore || b.score - a.score);
    const deduped = ranked.filter((row, idx, arr) =>
      arr.findIndex((x) => x.outcomeType === row.outcomeType) === idx,
    );

    console.log(`${away} @ ${home}`);
    for (const r of deduped.slice(0, topN)) {
      console.log(
        `  ${r.outcomeType} | meta ${(r.metaScore * 100).toFixed(1)}% | posterior ${(r.posteriorHitRate * 100).toFixed(1)}% | edge ${(r.edge * 100).toFixed(2)}% | n=${r.n}`,
      );
    }
    console.log("");
  }
}

export async function evaluateMetaModelMonthly(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const statuses = (flags.statuses ?? "deployed,validated")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const trainMonths = Math.max(3, Number(flags["train-months"] ?? 6));
  const epochs = Math.max(50, Number(flags.epochs ?? 250));
  const lr = Number(flags.lr ?? 0.03);
  const l2 = Number(flags.l2 ?? 0.0008);
  const minTrain = Math.max(200, Number(flags["min-train"] ?? 500));
  const minTest = Math.max(50, Number(flags["min-test"] ?? 100));
  const rolling = (flags.rolling ?? "true") !== "false";
  const fromDate = flags.from ? new Date(`${flags.from}T00:00:00Z`) : undefined;
  const toDate = flags.to ? new Date(`${flags.to}T00:00:00Z`) : undefined;

  console.log("\n=== Meta Model Monthly Walk-Forward ===\n");
  const rows = await loadMetaRows(statuses, fromDate, toDate);
  if (rows.length < minTrain + minTest) {
    throw new Error(`Not enough rows (${rows.length}) for monthly walk-forward.`);
  }
  const samples: Sample[] = rows.map((r) => ({
    outcomeType: r.outcomeType,
    x: buildFeatureVector({
      outcomeType: r.outcomeType,
      conditions: r.conditions ?? [],
      posteriorHitRate: r.posteriorHitRate ?? 0.5,
      edge: r.edge ?? 0,
      score: r.score ?? 0,
      n: r.n ?? 1,
    }),
    family: outcomeFamily(r.outcomeType),
    y: r.hitBool ? 1 : 0,
    date: new Date(r.date),
  }));
  samples.sort((a, b) => a.date.getTime() - b.date.getTime());
  const months = [...new Set(samples.map((s) => monthKey(s.date)))];
  if (months.length <= trainMonths) {
    throw new Error(`Need more months of data. Found=${months.length}, required>${trainMonths}.`);
  }

  const allPreds: Array<{ y: number; p: number }> = [];
  let folds = 0;
  for (let i = trainMonths; i < months.length; i++) {
    const [y, m] = months[i].split("-").map(Number);
    const testStart = new Date(Date.UTC(y, m - 1, 1));
    const testEnd = addMonthsUtc(testStart, 1);
    const trainStart = rolling ? addMonthsUtc(testStart, -trainMonths) : monthStartUtc(samples[0].date);
    const trainRows = samples.filter((s) => s.date >= trainStart && s.date < testStart);
    const testRows = samples.filter((s) => s.date >= testStart && s.date < testEnd);
    if (trainRows.length < minTrain || testRows.length < minTest) {
      continue;
    }
    const model = buildFamilyAwareModel(trainRows, testRows, epochs, lr, l2);
    const preds = testRows.map((r) => ({
      y: r.y,
      p: scoreMetaModel(model, {
        outcomeType: r.outcomeType,
        posteriorHitRate: r.x[0],
        edge: r.x[1],
        score: r.x[2],
        n: Math.max(1, Math.exp(r.x[3] ?? 0)),
        portabilityScore: r.x[4],
        dependencyRisk: r.x[5],
        recentAtoT: r.x[6],
      }),
    }));
    const metrics = computeBinaryMetrics(preds);
    allPreds.push(...preds);
    folds++;
    console.log(
      `${months[i]} | train=${trainRows.length} test=${testRows.length} | logloss=${metrics.logLoss.toFixed(4)} brier=${metrics.brier.toFixed(4)} acc=${(metrics.accuracy * 100).toFixed(1)}%`,
    );
  }

  const agg = computeBinaryMetrics(allPreds);
  console.log("\nAggregate:");
  console.log(`  folds=${folds}, samples=${allPreds.length}`);
  console.log(`  logloss=${agg.logLoss.toFixed(4)}`);
  console.log(`  brier=${agg.brier.toFixed(4)}`);
  console.log(`  accuracy=${(agg.accuracy * 100).toFixed(1)}%`);
}

export async function evaluateMetaModelPurged(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const statuses = (flags.statuses ?? "deployed,validated")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const folds = Math.max(3, Number(flags.folds ?? 6));
  const embargoDays = Math.max(0, Number(flags["embargo-days"] ?? 7));
  const epochs = Math.max(50, Number(flags.epochs ?? 250));
  const lr = Number(flags.lr ?? 0.03);
  const l2 = Number(flags.l2 ?? 0.0008);
  const minTrain = Math.max(200, Number(flags["min-train"] ?? 500));
  const minTest = Math.max(50, Number(flags["min-test"] ?? 100));
  const fromDate = flags.from ? new Date(`${flags.from}T00:00:00Z`) : undefined;
  const toDate = flags.to ? new Date(`${flags.to}T00:00:00Z`) : undefined;

  console.log("\n=== Meta Model Purged/Embargo Eval ===\n");
  const rows = await loadMetaRows(statuses, fromDate, toDate);
  const samples: Sample[] = rows
    .map((r) => ({
      outcomeType: r.outcomeType,
      x: buildFeatureVector({
        outcomeType: r.outcomeType,
        conditions: r.conditions ?? [],
        posteriorHitRate: r.posteriorHitRate ?? 0.5,
        edge: r.edge ?? 0,
        score: r.score ?? 0,
        n: r.n ?? 1,
      }),
      family: outcomeFamily(r.outcomeType),
      y: r.hitBool ? 1 : 0,
      date: new Date(r.date),
    }))
    .sort((a, b) => a.date.getTime() - b.date.getTime());
  if (samples.length < minTrain + minTest) {
    throw new Error(`Not enough rows (${samples.length}) for purged evaluation.`);
  }

  const minTs = samples[0].date.getTime();
  const maxTs = samples[samples.length - 1].date.getTime();
  const span = Math.max(1, maxTs - minTs);
  const allPreds: Array<{ y: number; p: number }> = [];
  let usedFolds = 0;
  for (let i = 0; i < folds; i++) {
    const foldStart = minTs + Math.floor((span * i) / folds);
    const foldEnd = minTs + Math.floor((span * (i + 1)) / folds);
    const embargoMs = embargoDays * 86_400_000;
    const train = samples.filter((s) => s.date.getTime() < foldStart - embargoMs);
    const test = samples.filter(
      (s) => s.date.getTime() >= foldStart && s.date.getTime() < foldEnd,
    );
    if (train.length < minTrain || test.length < minTest) continue;
    const model = buildFamilyAwareModel(train, test, epochs, lr, l2);
    const preds = test.map((r) => ({
      y: r.y,
      p: scoreMetaModel(model, {
        outcomeType: r.outcomeType,
        posteriorHitRate: r.x[0],
        edge: r.x[1],
        score: r.x[2],
        n: Math.max(1, Math.exp(r.x[3] ?? 0)),
        portabilityScore: r.x[4],
        dependencyRisk: r.x[5],
        recentAtoT: r.x[6],
      }),
    }));
    const m = computeBinaryMetrics(preds);
    usedFolds++;
    allPreds.push(...preds);
    console.log(
      `Fold ${i + 1}/${folds} | train=${train.length} test=${test.length} | logloss=${m.logLoss.toFixed(4)} brier=${m.brier.toFixed(4)} acc=${(m.accuracy * 100).toFixed(1)}%`,
    );
  }
  const agg = computeBinaryMetrics(allPreds);
  console.log("\nAggregate:");
  console.log(`  folds=${usedFolds}, samples=${allPreds.length}, embargoDays=${embargoDays}`);
  console.log(`  logloss=${agg.logLoss.toFixed(4)}`);
  console.log(`  brier=${agg.brier.toFixed(4)}`);
  console.log(`  accuracy=${(agg.accuracy * 100).toFixed(1)}%`);
}
