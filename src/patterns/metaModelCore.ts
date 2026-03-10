import * as fs from "fs/promises";
import * as path from "path";

export const DEFAULT_META_MODEL_RELATIVE_PATH = path.join(
  "data",
  "models",
  "meta-model-v2.json",
);

export interface MetaModelFeatureInput {
  outcomeType: string;
  conditions?: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
  portabilityScore?: number;
  dependencyRisk?: number;
  recentAtoT?: number;
}

export interface MetaModel {
  version: number;
  createdAt: string;
  source: string;
  sampleCount: number;
  trainCount: number;
  validationCount: number;
  featureNames: string[];
  means: number[];
  stds: number[];
  bias: number;
  weights: number[];
  familyWeights: Record<string, number>;
  familyModels?: Record<
    string,
    {
      sampleCount: number;
      trainCount: number;
      validationCount: number;
      means: number[];
      stds: number[];
      bias: number;
      weights: number[];
      metrics: {
        train: { logLoss: number; brier: number; accuracy: number };
        validation: { logLoss: number; brier: number; accuracy: number };
      };
    }
  >;
  calibrators?: {
    global?: MetaCalibrator;
    family?: Record<string, MetaCalibrator>;
  };
  metrics: {
    train: { logLoss: number; brier: number; accuracy: number };
    validation: { logLoss: number; brier: number; accuracy: number };
  };
}

export type PlattCalibrator = {
  type?: "platt";
  a: number;
  b: number;
};

export type IsotonicCalibrator = {
  type: "isotonic";
  x: number[];
  y: number[];
};

export type MetaCalibrator = PlattCalibrator | IsotonicCalibrator;

export function outcomeFamily(outcomeType: string): string {
  const base = outcomeType.replace(/:.*$/, "");
  if (base.startsWith("PLAYER_") || base.includes("TOP_")) return "PLAYER";
  if (base.endsWith("_WIN") || base === "HOME_WIN" || base === "AWAY_WIN") return "MONEYLINE";
  if (base.includes("COVERED")) return "SPREAD";
  if (base.startsWith("TOTAL_") || base.startsWith("OVER_") || base.startsWith("UNDER_")) return "TOTAL";
  if (base.includes("MARGIN") || base.includes("BLOWOUT")) return "MARGIN";
  return "OTHER";
}

export function sigmoid(x: number): number {
  if (x > 35) return 1;
  if (x < -35) return 0;
  return 1 / (1 + Math.exp(-x));
}

function applyIsotonicCalibrator(cal: IsotonicCalibrator, p: number): number {
  const clipped = Math.min(1 - 1e-6, Math.max(1e-6, p));
  const xs = cal.x ?? [];
  const ys = cal.y ?? [];
  if (xs.length === 0 || ys.length === 0 || xs.length !== ys.length) return clipped;
  if (xs.length === 1) return Math.min(1 - 1e-6, Math.max(1e-6, ys[0] ?? clipped));
  if (clipped <= xs[0]) return Math.min(1 - 1e-6, Math.max(1e-6, ys[0] ?? clipped));
  const last = xs.length - 1;
  if (clipped >= xs[last]) return Math.min(1 - 1e-6, Math.max(1e-6, ys[last] ?? clipped));
  for (let i = 1; i < xs.length; i++) {
    const x0 = xs[i - 1] ?? 0;
    const x1 = xs[i] ?? 1;
    if (clipped > x1) continue;
    const y0 = ys[i - 1] ?? x0;
    const y1 = ys[i] ?? x1;
    if (Math.abs(x1 - x0) < 1e-12) return Math.min(1 - 1e-6, Math.max(1e-6, y1));
    const t = (clipped - x0) / (x1 - x0);
    return Math.min(1 - 1e-6, Math.max(1e-6, y0 + t * (y1 - y0)));
  }
  return clipped;
}

export function applyMetaCalibrator(calibrator: MetaCalibrator | undefined, p: number): number {
  if (!calibrator) return p;
  if ("type" in calibrator && calibrator.type === "isotonic") {
    return applyIsotonicCalibrator(calibrator, p);
  }
  const clipped = Math.min(1 - 1e-6, Math.max(1e-6, p));
  const z = Math.log(clipped / (1 - clipped));
  return sigmoid((calibrator.a ?? 1) * z + (calibrator.b ?? 0));
}

function quantileLabelToUnit(label: string): number | null {
  const qMatch = label.match(/^Q(\d+)$/);
  if (qMatch) {
    const q = Number(qMatch[1]);
    if (!Number.isFinite(q)) return null;
    return Math.max(0, Math.min(1, (q - 1) / 4));
  }
  if (label.startsWith("LT_") || label.startsWith("LE_")) return 0.2;
  if (label.startsWith("GE_") || label.startsWith("GT_")) return 0.8;
  if (label.startsWith("RANGE_")) return 0.5;
  return null;
}

export function deriveMetaContextFeatures(input: {
  outcomeType: string;
  conditions?: string[];
}): {
  portabilityScore: number;
  dependencyRisk: number;
  recentAtoT: number;
} {
  const conditions = input.conditions ?? [];
  const family = outcomeFamily(input.outcomeType);
  let dependencyRisk =
    family === "PLAYER" ? 0.65 :
    family === "TOTAL" ? 0.35 :
    family === "SPREAD" ? 0.3 :
    family === "MONEYLINE" ? 0.25 :
    0.4;
  let portabilityScore =
    family === "PLAYER" ? 0.45 :
    family === "TOTAL" ? 0.6 :
    family === "SPREAD" ? 0.62 :
    family === "MONEYLINE" ? 0.65 :
    0.55;

  let recentAtoT = 1;
  let resilienceUnit: number | null = null;
  let roleDepUnit: number | null = null;

  for (const token of conditions) {
    if (
      token.startsWith("home_role_dependency:") ||
      token.startsWith("away_role_dependency:")
    ) {
      const label = token.split(":")[1] ?? "";
      const unit = quantileLabelToUnit(label);
      if (unit != null) {
        roleDepUnit = roleDepUnit == null ? unit : Math.max(roleDepUnit, unit);
      }
    }
    if (
      token.startsWith("home_playmaking_resilience:") ||
      token.startsWith("away_playmaking_resilience:")
    ) {
      const label = token.split(":")[1] ?? "";
      const unit = quantileLabelToUnit(label);
      if (unit != null) {
        resilienceUnit = resilienceUnit == null ? unit : (resilienceUnit + unit) / 2;
      }
    }
  }

  if (roleDepUnit != null) {
    dependencyRisk = Math.max(0, Math.min(1, dependencyRisk + (roleDepUnit - 0.5) * 0.6));
  }
  if (resilienceUnit != null) {
    const resilienceAdj = (resilienceUnit - 0.5) * 0.35;
    portabilityScore = Math.max(0, Math.min(1, portabilityScore + resilienceAdj));
    recentAtoT = 0.8 + resilienceUnit * 0.8; // roughly 0.8 to 1.6 proxy
  }

  portabilityScore = Math.max(
    0.05,
    Math.min(0.95, portabilityScore - dependencyRisk * 0.2),
  );
  dependencyRisk = Math.max(0.05, Math.min(0.95, dependencyRisk));

  return { portabilityScore, dependencyRisk, recentAtoT };
}

export function scoreMetaModel(
  model: MetaModel,
  input: MetaModelFeatureInput,
  options?: { calibrated?: boolean },
): number {
  const derived = deriveMetaContextFeatures({
    outcomeType: input.outcomeType,
    conditions: input.conditions,
  });
  const raw = [
    Number.isFinite(input.posteriorHitRate) ? input.posteriorHitRate : 0.5,
    Number.isFinite(input.edge) ? input.edge : 0,
    Number.isFinite(input.score) ? input.score : 0,
    Math.log(Math.max(1, Number.isFinite(input.n) ? input.n : 1)),
    Number.isFinite(input.portabilityScore) ? (input.portabilityScore as number) : derived.portabilityScore,
    Number.isFinite(input.dependencyRisk) ? (input.dependencyRisk as number) : derived.dependencyRisk,
    Number.isFinite(input.recentAtoT) ? (input.recentAtoT as number) : derived.recentAtoT,
  ];
  const family = outcomeFamily(input.outcomeType);
  const applyCalibration = (p: number): number => {
    if (options?.calibrated === false) return p;
    const familyCal = model.calibrators?.family?.[family];
    const globalCal = model.calibrators?.global;
    const cal = familyCal ?? globalCal;
    return applyMetaCalibrator(cal, p);
  };
  const familyModel = model.familyModels?.[family];
  if (familyModel) {
    const norm = raw.map((value, idx) => {
      const sd = familyModel.stds[idx] ?? 1;
      const mu = familyModel.means[idx] ?? 0;
      return (value - mu) / sd;
    });
    const linear =
      familyModel.bias +
      familyModel.weights.reduce((sum, w, idx) => sum + w * (norm[idx] ?? 0), 0);
    return applyCalibration(sigmoid(linear));
  }
  const norm = raw.map((value, idx) => {
    const sd = model.stds[idx] ?? 1;
    const mu = model.means[idx] ?? 0;
    return (value - mu) / sd;
  });
  const familyBias = model.familyWeights[family] ?? 0;
  const linear =
    model.bias +
    model.weights.reduce((sum, w, idx) => sum + w * (norm[idx] ?? 0), 0) +
    familyBias;
  return applyCalibration(sigmoid(linear));
}

function modelPathCandidates(relativePath: string): string[] {
  const cwd = process.cwd();
  const direct = path.resolve(cwd, relativePath);
  const parent = path.resolve(cwd, "..", relativePath);
  return [...new Set([direct, parent])];
}

export async function loadMetaModel(
  relativePath: string = DEFAULT_META_MODEL_RELATIVE_PATH,
): Promise<MetaModel | null> {
  const candidates = modelPathCandidates(relativePath);
  for (const p of candidates) {
    try {
      const raw = await fs.readFile(p, "utf8");
      return JSON.parse(raw) as MetaModel;
    } catch {
      // Try next path candidate.
    }
  }
  return null;
}

export async function saveMetaModel(
  model: MetaModel,
  relativePath: string = DEFAULT_META_MODEL_RELATIVE_PATH,
): Promise<string> {
  const outPath = path.resolve(process.cwd(), relativePath);
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, JSON.stringify(model, null, 2), "utf8");
  return outPath;
}
