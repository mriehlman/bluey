"use client";

import { useEffect, useState, useRef, useCallback } from "react";

interface StatusData {
  counts: {
    games: number;
    playerStats: number;
    teams: number;
    players: number;
    gameOdds: number;
    playerPropOdds: number;
    nightAggregates: number;
    nightEvents: number;
    nights: number;
    patterns: number;
    patternHits: number;
    watchlistItems: number;
    gameContexts: number;
    playerGameContexts: number;
    gameEvents: number;
    gamePatterns: number;
    gamePatternHits: number;
  };
  latestGameDate: string | null;
  latestNightDate: string | null;
  latestNightProcessedAt: string | null;
  latestOddsFetchedAt: string | null;
  seasons: number[];
}

interface StepDef {
  id: string;
  label: string;
  description: string;
  flags: FlagDef[];
  statusKeys: (keyof StatusData["counts"])[];
}

interface FlagDef {
  name: string;
  label: string;
  type: "text" | "number";
  placeholder?: string;
}

const SEASON_RANGE_FLAGS: FlagDef[] = [
  { name: "season", label: "Season", type: "number", placeholder: "e.g. 2024" },
  { name: "from-season", label: "From Season", type: "number", placeholder: "2019" },
  { name: "to-season", label: "To Season", type: "number", placeholder: "2025" },
];

const ODDS_STEPS: StepDef[] = [
  {
    id: "sync:odds",
    label: "Sync Game Odds",
    description: "Fetch live game odds (spreads, totals, moneylines) from The Odds API.",
    flags: [
      { name: "date", label: "Date", type: "text", placeholder: "YYYY-MM-DD (optional)" },
    ],
    statusKeys: ["gameOdds"],
  },
  {
    id: "sync:player-props",
    label: "Sync Player Props",
    description: "Fetch player prop odds (points, rebounds, assists, etc.) from The Odds API.",
    flags: [],
    statusKeys: ["playerPropOdds"],
  },
];

const GAME_PIPELINE_STEPS: StepDef[] = [
  {
    id: "build:game-context",
    label: "1. Build Game Context",
    description: "Compute pre-game team/player context (rankings, streaks, pace) for every game.",
    flags: SEASON_RANGE_FLAGS,
    statusKeys: ["gameContexts", "playerGameContexts"],
  },
  {
    id: "build:game-events",
    label: "2. Build Game Events",
    description: "Generate condition and outcome events from context + game results.",
    flags: SEASON_RANGE_FLAGS,
    statusKeys: ["gameEvents"],
  },
  {
    id: "search:game-patterns",
    label: "3. Search Game Patterns",
    description: "Find condition-to-outcome correlations with statistical significance.",
    flags: [
      { name: "minSample", label: "Min Sample", type: "number", placeholder: "15" },
      { name: "minHitRate", label: "Min Hit Rate", type: "text", placeholder: "0.58" },
      { name: "maxLegs", label: "Max Legs", type: "number", placeholder: "3" },
      { name: "maxResults", label: "Max Results", type: "number", placeholder: "2000" },
    ],
    statusKeys: ["gamePatterns", "gamePatternHits"],
  },
  {
    id: "predict:games",
    label: "4. Predict Games",
    description: "Generate predictions for games on a specific date based on stored patterns.",
    flags: [
      { name: "date", label: "Date", type: "text", placeholder: "YYYY-MM-DD" },
      { name: "season", label: "Season", type: "number", placeholder: "2025" },
    ],
    statusKeys: [],
  },
  {
    id: "predict:players",
    label: "5. Predict Players",
    description: "Generate player prop predictions for games on a specific date.",
    flags: [
      { name: "date", label: "Date", type: "text", placeholder: "YYYY-MM-DD" },
      { name: "season", label: "Season", type: "number", placeholder: "2025" },
    ],
    statusKeys: [],
  },
];

const COMMON_FLAGS: FlagDef[] = [
  { name: "season", label: "Season", type: "number", placeholder: "e.g. 2024" },
  { name: "dateFrom", label: "From", type: "text", placeholder: "YYYY-MM-DD" },
  { name: "dateTo", label: "To", type: "text", placeholder: "YYYY-MM-DD" },
];

const LEGACY_STEPS: StepDef[] = [
  {
    id: "build:night-aggregates",
    label: "Build Night Aggregates",
    description: "Precompute team stat totals per night from player box scores.",
    flags: [
      ...COMMON_FLAGS,
      { name: "missingOnly", label: "Missing Only", type: "text", placeholder: "true" },
    ],
    statusKeys: ["nightAggregates"],
  },
  {
    id: "build:nightly-events",
    label: "Build Night Events",
    description: "Run the event catalog over game dates to detect nightly events.",
    flags: COMMON_FLAGS,
    statusKeys: ["nightEvents"],
  },
  {
    id: "build:nights",
    label: "Build Nights",
    description: "Create Night summary records with processing metadata.",
    flags: [{ name: "season", label: "Season", type: "number", placeholder: "e.g. 2024" }],
    statusKeys: ["nights"],
  },
  {
    id: "search:patterns",
    label: "Search Slate Patterns",
    description: "Discover recurring multi-event patterns across all seasons.",
    flags: [
      { name: "minOcc", label: "Min Occ", type: "number", placeholder: "3" },
      { name: "minSeasons", label: "Min Seasons", type: "number", placeholder: "2" },
      { name: "maxCluster", label: "Max Cluster", type: "text", placeholder: "0.6" },
      { name: "minAvg", label: "Min Avg/Season", type: "text", placeholder: "0.5" },
      { name: "maxAvg", label: "Max Avg/Season", type: "text", placeholder: "6" },
      { name: "maxResults", label: "Max Results", type: "number", placeholder: "5000" },
    ],
    statusKeys: ["patterns", "patternHits"],
  },
  {
    id: "patterns:dedupe",
    label: "Dedupe Patterns",
    description: "Analyze and flag redundant or near-duplicate patterns.",
    flags: [
      { name: "threshold", label: "Threshold", type: "text", placeholder: "0.9" },
      { name: "minOcc", label: "Min Occ", type: "number", placeholder: "3" },
      { name: "top", label: "Top N", type: "number", placeholder: "50" },
    ],
    statusKeys: ["patterns"],
  },
];

type StepState = "idle" | "running" | "done" | "error";

export default function PipelinePage() {
  const [status, setStatus] = useState<StatusData | null>(null);
  const [stepStates, setStepStates] = useState<Record<string, StepState>>({});
  const [stepOutputs, setStepOutputs] = useState<Record<string, string>>({});
  const [stepFlags, setStepFlags] = useState<Record<string, Record<string, string>>>({});
  const [expandedStep, setExpandedStep] = useState<string | null>(null);
  const [showLegacy, setShowLegacy] = useState(false);
  const outputRefs = useRef<Record<string, HTMLPreElement | null>>({});

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch("/api/pipeline/status");
      const data = await res.json();
      setStatus(data);
    } catch {
      // silent
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  function setFlag(stepId: string, flag: string, value: string) {
    setStepFlags((prev) => ({
      ...prev,
      [stepId]: { ...prev[stepId], [flag]: value },
    }));
  }

  async function runStep(stepId: string): Promise<"done" | "error"> {
    setStepStates((prev) => ({ ...prev, [stepId]: "running" }));
    setStepOutputs((prev) => ({ ...prev, [stepId]: "" }));
    setExpandedStep(stepId);

    const flags: Record<string, string> = {};
    for (const [k, v] of Object.entries(stepFlags[stepId] ?? {})) {
      if (v) flags[k] = v;
    }

    let fullOutput = "";

    try {
      const res = await fetch("/api/pipeline/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ step: stepId, flags }),
      });

      if (!res.ok || !res.body) {
        const err = await res.text();
        setStepOutputs((prev) => ({ ...prev, [stepId]: `Error: ${err}` }));
        setStepStates((prev) => ({ ...prev, [stepId]: "error" }));
        return "error";
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const text = decoder.decode(value, { stream: true });
        fullOutput += text;
        setStepOutputs((prev) => ({ ...prev, [stepId]: fullOutput }));
        const el = outputRefs.current[stepId];
        if (el) {
          requestAnimationFrame(() => {
            el.scrollTop = el.scrollHeight;
          });
        }
      }

      const isError = fullOutput.includes("=== Failed") || fullOutput.includes("=== ERROR");
      const finalState = isError ? "error" : "done";
      setStepStates((prev) => ({ ...prev, [stepId]: finalState }));
      fetchStatus();
      return finalState;
    } catch (err) {
      fullOutput += `\nFetch error: ${err}`;
      setStepOutputs((prev) => ({ ...prev, [stepId]: fullOutput }));
      setStepStates((prev) => ({ ...prev, [stepId]: "error" }));
      return "error";
    }
  }

  async function runGamePipeline() {
    for (const step of GAME_PIPELINE_STEPS.slice(0, 3)) {
      const result = await runStep(step.id);
      if (result === "error") break;
    }
  }

  async function syncAllOdds() {
    for (const step of ODDS_STEPS) {
      const result = await runStep(step.id);
      if (result === "error") break;
    }
  }

  const fmtDate = (d: string | null) =>
    d ? new Date(d).toISOString().slice(0, 10) : "—";
  const fmtDateTime = (d: string | null) =>
    d ? new Date(d).toLocaleString() : "—";
  const fmtCount = (n: number) => n.toLocaleString();

  const anyRunning = Object.values(stepStates).some((s) => s === "running");

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1>Pipeline</h1>
        <button onClick={fetchStatus} disabled={anyRunning}>
          Refresh Status
        </button>
      </div>

      {status && (
        <div className="card" style={{ marginBottom: "1.5rem" }}>
          <h2 style={{ marginTop: 0 }}>Data Overview</h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
              gap: "0.75rem",
            }}
          >
            <Stat label="Games" value={fmtCount(status.counts.games)} />
            <Stat label="Player Stats" value={fmtCount(status.counts.playerStats)} />
            <Stat label="Teams" value={fmtCount(status.counts.teams)} />
            <Stat label="Players" value={fmtCount(status.counts.players)} />
            <Stat label="Game Odds" value={fmtCount(status.counts.gameOdds)} highlight />
            <Stat label="Player Props" value={fmtCount(status.counts.playerPropOdds)} highlight />
            <Stat label="Game Contexts" value={fmtCount(status.counts.gameContexts)} />
            <Stat label="Game Events" value={fmtCount(status.counts.gameEvents)} />
            <Stat label="Game Patterns" value={fmtCount(status.counts.gamePatterns)} highlight />
            <Stat label="Pattern Hits" value={fmtCount(status.counts.gamePatternHits)} />
          </div>
          <div style={{ marginTop: "0.75rem", fontSize: "0.85rem" }} className="muted">
            Latest game: {fmtDate(status.latestGameDate)} | 
            Odds updated: {fmtDateTime(status.latestOddsFetchedAt)} | 
            Seasons: {status.seasons.length > 0 ? status.seasons.join(", ") : "—"}
          </div>
        </div>
      )}

      <h2 style={{ marginTop: "1rem", marginBottom: "0.5rem" }}>Odds Sync</h2>
      <p className="muted" style={{ fontSize: "0.85rem", marginBottom: "0.75rem" }}>
        Sync live betting odds from The Odds API. Run daily to keep data current.
      </p>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.75rem" }}>
        <button
          onClick={syncAllOdds}
          disabled={anyRunning}
          style={{ background: "#7c3aed" }}
        >
          Sync All Odds
        </button>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        {ODDS_STEPS.map((step) => (
          <StepCard
            key={step.id}
            step={step}
            state={stepStates[step.id] ?? "idle"}
            output={stepOutputs[step.id] ?? ""}
            isExpanded={expandedStep === step.id}
            status={status}
            anyRunning={anyRunning}
            stepFlags={stepFlags}
            setFlag={setFlag}
            setExpandedStep={setExpandedStep}
            runStep={runStep}
            outputRefs={outputRefs}
            fmtCount={fmtCount}
          />
        ))}
      </div>

      <h2 style={{ marginTop: "1.5rem", marginBottom: "0.5rem" }}>Game Prediction Pipeline</h2>
      <p className="muted" style={{ fontSize: "0.85rem", marginBottom: "0.75rem" }}>
        Build pre-game context, generate events, search for predictive patterns, and make predictions.
      </p>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.75rem" }}>
        <button
          onClick={runGamePipeline}
          disabled={anyRunning}
          style={{ background: "#059669" }}
        >
          Run Pipeline (Steps 1-3)
        </button>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        {GAME_PIPELINE_STEPS.map((step) => (
          <StepCard
            key={step.id}
            step={step}
            state={stepStates[step.id] ?? "idle"}
            output={stepOutputs[step.id] ?? ""}
            isExpanded={expandedStep === step.id}
            status={status}
            anyRunning={anyRunning}
            stepFlags={stepFlags}
            setFlag={setFlag}
            setExpandedStep={setExpandedStep}
            runStep={runStep}
            outputRefs={outputRefs}
            fmtCount={fmtCount}
          />
        ))}
      </div>

      <div style={{ marginTop: "2rem", borderTop: "1px solid var(--border)", paddingTop: "1rem" }}>
        <button
          onClick={() => setShowLegacy(!showLegacy)}
          style={{ 
            background: "transparent", 
            border: "1px solid var(--border)",
            color: "var(--text-muted)",
            fontSize: "0.85rem",
          }}
        >
          {showLegacy ? "▼" : "▶"} Legacy: Slate-Level Pipeline
        </button>
        
        {showLegacy && (
          <>
            <p className="muted" style={{ fontSize: "0.8rem", marginTop: "0.5rem", marginBottom: "0.75rem" }}>
              Night/slate-based analysis (deprecated in favor of game-level predictions).
            </p>
            {status && (
              <div style={{ 
                display: "flex", 
                gap: "1rem", 
                marginBottom: "0.75rem",
                fontSize: "0.8rem",
              }} className="muted">
                <span>Night Aggregates: {fmtCount(status.counts.nightAggregates)}</span>
                <span>Night Events: {fmtCount(status.counts.nightEvents)}</span>
                <span>Nights: {fmtCount(status.counts.nights)}</span>
                <span>Slate Patterns: {fmtCount(status.counts.patterns)}</span>
              </div>
            )}
            <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
              {LEGACY_STEPS.map((step) => (
                <StepCard
                  key={step.id}
                  step={step}
                  state={stepStates[step.id] ?? "idle"}
                  output={stepOutputs[step.id] ?? ""}
                  isExpanded={expandedStep === step.id}
                  status={status}
                  anyRunning={anyRunning}
                  stepFlags={stepFlags}
                  setFlag={setFlag}
                  setExpandedStep={setExpandedStep}
                  runStep={runStep}
                  outputRefs={outputRefs}
                  fmtCount={fmtCount}
                />
              ))}
            </div>
          </>
        )}
      </div>
    </>
  );
}

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div>
      <div className="muted" style={{ fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        {label}
      </div>
      <div style={{ 
        fontSize: "1.1rem", 
        fontWeight: 600,
        color: highlight ? "var(--accent-cyan)" : "var(--text-primary)",
      }}>{value}</div>
    </div>
  );
}

function StepBadge({ state }: { state: StepState }) {
  switch (state) {
    case "running":
      return <span className="badge" style={{ background: "var(--warning-bg)", color: "var(--warning)", borderColor: "var(--warning)" }}>running</span>;
    case "done":
      return <span className="badge badge-green">done</span>;
    case "error":
      return <span className="badge badge-red">error</span>;
    default:
      return null;
  }
}

function StepCard({
  step,
  state,
  output,
  isExpanded,
  status,
  anyRunning,
  stepFlags,
  setFlag,
  setExpandedStep,
  runStep,
  outputRefs,
  fmtCount,
}: {
  step: StepDef;
  state: StepState;
  output: string;
  isExpanded: boolean;
  status: StatusData | null;
  anyRunning: boolean;
  stepFlags: Record<string, Record<string, string>>;
  setFlag: (stepId: string, flag: string, value: string) => void;
  setExpandedStep: (stepId: string | null) => void;
  runStep: (stepId: string) => Promise<"done" | "error">;
  outputRefs: React.MutableRefObject<Record<string, HTMLPreElement | null>>;
  fmtCount: (n: number) => string;
}) {
  return (
    <div className="card">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
        }}
      >
        <div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.5rem",
              marginBottom: "0.25rem",
              flexWrap: "wrap",
            }}
          >
            <strong>{step.label}</strong>
            <StepBadge state={state} />
            {status &&
              step.statusKeys.map((k) => (
                <span key={k} className="badge badge-gray">
                  {k}: {fmtCount(status.counts[k])}
                </span>
              ))}
          </div>
          <div className="muted" style={{ fontSize: "0.85rem" }}>
            {step.description}
          </div>
        </div>
        <div style={{ display: "flex", gap: "0.5rem", flexShrink: 0 }}>
          {output && (
            <button
              onClick={() => setExpandedStep(isExpanded ? null : step.id)}
              style={{ background: "#6b7280" }}
            >
              {isExpanded ? "Hide" : "Show"} Output
            </button>
          )}
          <button onClick={() => runStep(step.id)} disabled={anyRunning}>
            {state === "running" ? "Running…" : "Run"}
          </button>
        </div>
      </div>

      {step.flags.length > 0 && (
        <div className="filters" style={{ marginTop: "0.5rem", marginBottom: 0 }}>
          {step.flags.map((f) => (
            <label key={f.name} style={{ fontSize: "0.85rem" }}>
              {f.label}:{" "}
              <input
                type={f.type}
                placeholder={f.placeholder}
                value={stepFlags[step.id]?.[f.name] ?? ""}
                onChange={(e) => setFlag(step.id, f.name, e.target.value)}
                style={{ width: f.type === "number" ? 80 : 110 }}
                disabled={anyRunning}
              />
            </label>
          ))}
        </div>
      )}

      {isExpanded && output && (
        <pre
          ref={(el) => {
            outputRefs.current[step.id] = el;
          }}
          style={{
            marginTop: "0.75rem",
            marginBottom: 0,
            maxHeight: 400,
            overflow: "auto",
            fontSize: "0.8rem",
          }}
        >
          {output}
        </pre>
      )}
    </div>
  );
}
