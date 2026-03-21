"use client";

import { useEffect, useMemo, useState } from "react";

type PredictionListRow = {
  predictionId: string;
  runId: string | null;
  runStartedAt: string | null;
  gameId: string;
  market: string;
  selection: string;
  confidenceScore: number;
  edgeEstimate: number;
  generatedAt: string;
  predictionContractVersion: string;
  modelBundleVersion: string;
  featureSchemaVersion: string;
  rankingPolicyVersion: string;
  aggregationPolicyVersion: string;
  featureSnapshotId: string;
  supportingPatternCount: number;
  supportingPatterns: string[];
  featureSnapshotPayloadSummary: Record<string, unknown>;
  sourceTimeMetadata: {
    oddsTimestampUsed: string | null;
    statsSnapshotCutoff: string | null;
    injuryLineupCutoff: string | null;
  };
};

type RejectionRow = {
  id: string;
  runId: string | null;
  runDate: string;
  gameId: string;
  patternId: string;
  reasons: string[];
};

type PredictionDetail = {
  predictionId: string;
  runId: string | null;
  runStartedAt: string | null;
  runContext: unknown;
  gameId: string;
  market: string;
  selection: string;
  confidenceScore: number;
  edgeEstimate: number;
  generatedAt: string;
  predictionContractVersion: string;
  modelBundleVersion: string;
  featureSchemaVersion: string;
  rankingPolicyVersion: string;
  aggregationPolicyVersion: string;
  featureSnapshotId: string;
  modelVotes: unknown;
  supportingPatterns: string[];
  featureSnapshotPayload: Record<string, unknown>;
};

type PredictionRun = {
  runId: string;
  runStartedAt: string | null;
  runContext: unknown;
  predictionCount: number;
};

type CompareSummary = {
  predictions: number;
  avgConfidence: number | null;
  rejections: number;
};

export default function GovernanceLineagePage() {
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [runId, setRunId] = useState("");
  const [compareRunId, setCompareRunId] = useState("");
  const [gameId, setGameId] = useState("");
  const [market, setMarket] = useState("");
  const [runs, setRuns] = useState<PredictionRun[]>([]);
  const [predictions, setPredictions] = useState<PredictionListRow[]>([]);
  const [rejections, setRejections] = useState<RejectionRow[]>([]);
  const [loadingPredictions, setLoadingPredictions] = useState(false);
  const [loadingRejections, setLoadingRejections] = useState(false);
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [selectedPredictionId, setSelectedPredictionId] = useState<string | null>(null);
  const [selectedPrediction, setSelectedPrediction] = useState<PredictionDetail | null>(null);
  const [comparePrediction, setComparePrediction] = useState<PredictionDetail | null>(null);
  const [activeTab, setActiveTab] = useState<"predictions" | "rejections">("predictions");
  const [error, setError] = useState<string | null>(null);
  const [compareSummary, setCompareSummary] = useState<CompareSummary | null>(null);
  const [loadingCompareSummary, setLoadingCompareSummary] = useState(false);

  async function loadRuns() {
    setLoadingRuns(true);
    try {
      const params = new URLSearchParams();
      if (date) params.set("date", date);
      const res = await fetch(`/api/predictions/runs?${params.toString()}`, { cache: "no-store" });
      const data = await res.json();
      setRuns((data.runs ?? []) as PredictionRun[]);
      if (!runId && data.runs?.length > 0) setRunId(data.runs[0].runId);
    } catch {
      setRuns([]);
    } finally {
      setLoadingRuns(false);
    }
  }

  async function loadPredictions() {
    setLoadingPredictions(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("governance", "1");
      if (date) params.set("date", date);
      if (runId.trim()) params.set("runId", runId.trim());
      if (gameId.trim()) params.set("gameId", gameId.trim());
      if (market.trim()) params.set("market", market.trim());
      const res = await fetch(`/api/predictions?${params.toString()}`, {
        cache: "no-store",
      });
      const data = await res.json();
      setPredictions((data.predictions ?? []) as PredictionListRow[]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load predictions.");
      setPredictions([]);
    } finally {
      setLoadingPredictions(false);
    }
  }

  async function loadRejections() {
    setLoadingRejections(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (date) params.set("date", date);
      if (runId.trim()) params.set("runId", runId.trim());
      if (gameId.trim()) params.set("gameId", gameId.trim());
      const res = await fetch(`/api/predictions/rejections?${params.toString()}`, {
        cache: "no-store",
      });
      const data = await res.json();
      setRejections((data.rejections ?? []) as RejectionRow[]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load rejections.");
      setRejections([]);
    } finally {
      setLoadingRejections(false);
    }
  }

  useEffect(() => {
    void loadRuns();
    void loadPredictions();
    void loadRejections();
  }, []);

  useEffect(() => {
    void loadRuns();
  }, [date]);

  useEffect(() => {
    if (!compareRunId.trim()) {
      setCompareSummary(null);
      return;
    }
    let cancelled = false;
    setLoadingCompareSummary(true);
    (async () => {
      try {
        const predParams = new URLSearchParams();
        predParams.set("governance", "1");
        if (date) predParams.set("date", date);
        predParams.set("runId", compareRunId.trim());
        if (gameId.trim()) predParams.set("gameId", gameId.trim());
        if (market.trim()) predParams.set("market", market.trim());
        predParams.set("limit", "1000");

        const rejParams = new URLSearchParams();
        if (date) rejParams.set("date", date);
        rejParams.set("runId", compareRunId.trim());
        if (gameId.trim()) rejParams.set("gameId", gameId.trim());
        rejParams.set("limit", "2000");

        const [predData, rejData] = await Promise.all([
          fetch(`/api/predictions?${predParams.toString()}`, { cache: "no-store" }).then((r) => r.json()),
          fetch(`/api/predictions/rejections?${rejParams.toString()}`, { cache: "no-store" }).then((r) => r.json()),
        ]);

        const comparePredictions = (predData.predictions ?? []) as PredictionListRow[];
        const avgConfidence = comparePredictions.length > 0
          ? comparePredictions.reduce((sum, row) => sum + row.confidenceScore, 0) / comparePredictions.length
          : null;
        if (!cancelled) {
          setCompareSummary({
            predictions: comparePredictions.length,
            avgConfidence,
            rejections: Array.isArray(rejData.rejections) ? rejData.rejections.length : 0,
          });
        }
      } catch {
        if (!cancelled) setCompareSummary(null);
      } finally {
        if (!cancelled) setLoadingCompareSummary(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [compareRunId, date, gameId, market]);

  useEffect(() => {
    if (!selectedPredictionId) {
      setSelectedPrediction(null);
      setComparePrediction(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`/api/predictions/${encodeURIComponent(selectedPredictionId)}`, {
          cache: "no-store",
        });
        if (!res.ok) return;
        const data = await res.json();
        if (!cancelled) {
          const selected = (data.prediction ?? null) as PredictionDetail | null;
          setSelectedPrediction(selected);
          if (selected && compareRunId.trim()) {
            const compareList = await fetch(
              `/api/predictions?governance=1&date=${encodeURIComponent(date)}&runId=${encodeURIComponent(compareRunId)}&gameId=${encodeURIComponent(selected.gameId)}&market=${encodeURIComponent(selected.market)}`,
              { cache: "no-store" },
            ).then((r) => r.json());
            const match = (compareList.predictions ?? []).find(
              (row: PredictionListRow) => row.selection === selected.selection,
            );
            if (match?.predictionId) {
              const compareDetailData = await fetch(
                `/api/predictions/${encodeURIComponent(match.predictionId)}`,
                { cache: "no-store" },
              ).then((r) => r.json());
              setComparePrediction((compareDetailData.prediction ?? null) as PredictionDetail | null);
            } else {
              setComparePrediction(null);
            }
          } else {
            setComparePrediction(null);
          }
        }
      } catch {
        if (!cancelled) {
          setSelectedPrediction(null);
          setComparePrediction(null);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedPredictionId, compareRunId, date]);

  const rejectionSummary = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of rejections) {
      for (const reason of row.reasons) {
        counts.set(reason, (counts.get(reason) ?? 0) + 1);
      }
    }
    return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6);
  }, [rejections]);

  const selectedRun = useMemo(
    () => runs.find((run) => run.runId === runId) ?? null,
    [runs, runId],
  );

  const compareRun = useMemo(
    () => runs.find((run) => run.runId === compareRunId) ?? null,
    [runs, compareRunId],
  );

  const runSummary = useMemo(() => {
    const distinctGames = new Set(predictions.map((p) => p.gameId)).size;
    const distinctMarkets = new Set(predictions.map((p) => p.market)).size;
    const avgConfidence = predictions.length > 0
      ? predictions.reduce((sum, row) => sum + row.confidenceScore, 0) / predictions.length
      : null;
    const reasonCounts = new Map<string, number>();
    for (const row of rejections) {
      for (const reason of row.reasons) {
        reasonCounts.set(reason, (reasonCounts.get(reason) ?? 0) + 1);
      }
    }
    const topReasons = [...reasonCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3);
    return {
      distinctGames,
      distinctMarkets,
      avgConfidence,
      topReasons,
    };
  }, [predictions, rejections]);

  const diffRows = useMemo(() => {
    if (!selectedPrediction || !comparePrediction) return [];
    const fields: Array<{ key: string; current: unknown; previous: unknown }> = [
      { key: "confidenceScore", current: selectedPrediction.confidenceScore, previous: comparePrediction.confidenceScore },
      { key: "edgeEstimate", current: selectedPrediction.edgeEstimate, previous: comparePrediction.edgeEstimate },
      { key: "modelVotes", current: selectedPrediction.modelVotes, previous: comparePrediction.modelVotes },
      { key: "supportingPatterns", current: selectedPrediction.supportingPatterns, previous: comparePrediction.supportingPatterns },
      { key: "featureSnapshotId", current: selectedPrediction.featureSnapshotId, previous: comparePrediction.featureSnapshotId },
      { key: "featureSnapshotPayload", current: selectedPrediction.featureSnapshotPayload, previous: comparePrediction.featureSnapshotPayload },
      { key: "modelBundleVersion", current: selectedPrediction.modelBundleVersion, previous: comparePrediction.modelBundleVersion },
      { key: "rankingPolicyVersion", current: selectedPrediction.rankingPolicyVersion, previous: comparePrediction.rankingPolicyVersion },
      { key: "aggregationPolicyVersion", current: selectedPrediction.aggregationPolicyVersion, previous: comparePrediction.aggregationPolicyVersion },
    ];
    return fields.filter((row) => JSON.stringify(row.current) !== JSON.stringify(row.previous));
  }, [selectedPrediction, comparePrediction]);

  const compareDeltas = useMemo(() => {
    if (!compareSummary) return null;
    const predDelta = predictions.length - compareSummary.predictions;
    const rejDelta = rejections.length - compareSummary.rejections;
    const currentAvg = runSummary.avgConfidence;
    const compareAvg = compareSummary.avgConfidence;
    const avgDelta = currentAvg != null && compareAvg != null ? currentAvg - compareAvg : null;
    return {
      predDelta,
      rejDelta,
      avgDelta,
    };
  }, [compareSummary, predictions.length, rejections.length, runSummary.avgConfidence]);

  function deltaLabel(value: number, fractionDigits = 0): string {
    const sign = value > 0 ? "+" : "";
    return `${sign}${value.toFixed(fractionDigits)}`;
  }

  function deltaClass(value: number): string {
    if (value > 0) return "delta-chip delta-positive";
    if (value < 0) return "delta-chip delta-negative";
    return "delta-chip delta-neutral";
  }

  function exportJson(filename: string, payload: unknown) {
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="card">
      <h1 style={{ marginTop: 0 }}>Governance Lineage</h1>
      <p className="muted" style={{ marginTop: 0 }}>
        Read-only view of canonical predictions and rejection diagnostics.
      </p>

      <div className="filters">
        <label>
          Date
          <input type="date" value={date} onChange={(e) => setDate(e.target.value)} />
        </label>
        <label>
          Run
          <select value={runId} onChange={(e) => setRunId(e.target.value)}>
            <option value="">All runs</option>
            {runs.map((run) => (
              <option key={run.runId} value={run.runId}>
                {run.runId.slice(0, 8)} ({run.predictionCount})
              </option>
            ))}
          </select>
        </label>
        <label>
          Compare run
          <select value={compareRunId} onChange={(e) => setCompareRunId(e.target.value)}>
            <option value="">No compare</option>
            {runs
              .filter((run) => run.runId !== runId)
              .map((run) => (
                <option key={run.runId} value={run.runId}>
                  {run.runId.slice(0, 8)} ({run.predictionCount})
                </option>
              ))}
          </select>
        </label>
        <label>
          Game ID
          <input
            type="text"
            value={gameId}
            onChange={(e) => setGameId(e.target.value)}
            placeholder="optional game id"
          />
        </label>
        <label>
          Market
          <input
            type="text"
            value={market}
            onChange={(e) => setMarket(e.target.value)}
            placeholder="moneyline, spread..."
          />
        </label>
        <button type="button" onClick={() => { void loadPredictions(); void loadRejections(); }}>
          Refresh
        </button>
        <button
          type="button"
          onClick={() => exportJson(`predictions_${date}.json`, predictions)}
          disabled={predictions.length === 0}
        >
          Export predictions
        </button>
        <button
          type="button"
          onClick={() => exportJson(`rejections_${date}.json`, rejections)}
          disabled={rejections.length === 0}
        >
          Export rejections
        </button>
      </div>
      {loadingRuns ? <p className="muted">Loading runs...</p> : null}

      <div className="card" style={{ marginBottom: 12 }}>
        <h2 style={{ marginTop: 0 }}>Run Summary</h2>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 14 }}>
          <div>
            <div className="muted">Selected run</div>
            <div className="mono">{selectedRun ? selectedRun.runId : "All runs"}</div>
          </div>
          <div>
            <div className="muted">Started</div>
            <div className="mono">
              {selectedRun?.runStartedAt ? new Date(selectedRun.runStartedAt).toISOString() : "n/a"}
            </div>
          </div>
          <div>
            <div className="muted">Predictions (filtered)</div>
            <div className="mono">{predictions.length}</div>
          </div>
          <div>
            <div className="muted">Distinct games (filtered)</div>
            <div className="mono">{runSummary.distinctGames}</div>
          </div>
          <div>
            <div className="muted">Distinct markets (filtered)</div>
            <div className="mono">{runSummary.distinctMarkets}</div>
          </div>
          <div>
            <div className="muted">Avg confidence (filtered)</div>
            <div className="mono">
              {runSummary.avgConfidence == null ? "n/a" : `${(runSummary.avgConfidence * 100).toFixed(1)}%`}
            </div>
          </div>
          <div>
            <div className="muted">Rejections (filtered)</div>
            <div className="mono">{rejections.length}</div>
          </div>
          <div>
            <div className="muted">Compare run</div>
            <div className="mono">{compareRun ? compareRun.runId : "none"}</div>
          </div>
        </div>
        {compareRun && compareDeltas ? (
          <div style={{ marginTop: 10 }}>
            <div className="muted" style={{ marginBottom: 6 }}>
              Compare deltas (filtered) {loadingCompareSummary ? " - loading..." : ""}
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              <span className={deltaClass(compareDeltas.predDelta)}>
                predictions: {deltaLabel(compareDeltas.predDelta)}
              </span>
              <span className={deltaClass(compareDeltas.rejDelta)}>
                rejections: {deltaLabel(compareDeltas.rejDelta)}
              </span>
              {compareDeltas.avgDelta != null ? (
                <span className={deltaClass(compareDeltas.avgDelta)}>
                  avg confidence: {deltaLabel(compareDeltas.avgDelta, 4)}
                </span>
              ) : (
                <span className="delta-chip delta-neutral">avg confidence: n/a</span>
              )}
            </div>
          </div>
        ) : null}
        {selectedRun?.runContext ? (
          <>
            <h3 style={{ marginBottom: 6 }}>Run context</h3>
            <pre style={{ marginTop: 0 }}>{JSON.stringify(selectedRun.runContext, null, 2)}</pre>
          </>
        ) : null}
        {runSummary.topReasons.length > 0 ? (
          <>
            <h3 style={{ marginBottom: 6 }}>Top rejection reasons (filtered)</h3>
            <div className="mono">
              {runSummary.topReasons.map(([reason, count]) => `${reason}:${count}`).join(", ")}
            </div>
          </>
        ) : null}
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <button type="button" onClick={() => setActiveTab("predictions")}>
          Predictions ({predictions.length})
        </button>
        <button type="button" onClick={() => setActiveTab("rejections")}>
          Rejections ({rejections.length})
        </button>
      </div>

      {error ? <p className="badge badge-red">{error}</p> : null}

      {activeTab === "predictions" ? (
        <>
          {loadingPredictions ? <p className="muted">Loading predictions...</p> : null}
          <table>
            <thead>
              <tr>
                <th>Prediction ID</th>
                <th>Game</th>
                <th>Market / Selection</th>
                <th>Conf</th>
                <th>Edge</th>
                <th>Generated</th>
                <th>Versions</th>
                <th>Snapshot</th>
              </tr>
            </thead>
            <tbody>
              {predictions.map((row) => (
                <tr
                  key={row.predictionId}
                  onClick={() => setSelectedPredictionId(row.predictionId)}
                  style={{ cursor: "pointer" }}
                >
                  <td className="mono">{row.predictionId}</td>
                  <td className="mono">{row.gameId}</td>
                  <td>{row.market} / {row.selection}</td>
                  <td>{(row.confidenceScore * 100).toFixed(1)}%</td>
                  <td>{(row.edgeEstimate * 100).toFixed(2)}%</td>
                  <td className="mono">{new Date(row.generatedAt).toISOString()}</td>
                  <td>
                    <div className="version-chip-wrap">
                      <span className="version-chip">contract {row.predictionContractVersion}</span>
                      <span className="version-chip">model {row.modelBundleVersion}</span>
                      <span className="version-chip">feature {row.featureSchemaVersion}</span>
                      <span className="version-chip">rank {row.rankingPolicyVersion}</span>
                      <span className="version-chip">agg {row.aggregationPolicyVersion}</span>
                    </div>
                  </td>
                  <td className="mono">{row.featureSnapshotId}</td>
                </tr>
              ))}
              {predictions.length === 0 && !loadingPredictions ? (
                <tr>
                  <td colSpan={8} className="muted">No canonical predictions for current filters.</td>
                </tr>
              ) : null}
            </tbody>
          </table>

          {selectedPrediction ? (
            <div className="card" style={{ marginTop: 12 }}>
              <h2 style={{ marginTop: 0 }}>Prediction Details</h2>
              <p className="mono">{selectedPrediction.predictionId}</p>
              <p className="mono">run_id: {selectedPrediction.runId ?? "n/a"}</p>
              <p className="mono">run_started_at: {selectedPrediction.runStartedAt ?? "n/a"}</p>
              <h3>Run context</h3>
              <pre>{JSON.stringify(selectedPrediction.runContext, null, 2)}</pre>
              <h3>Model votes</h3>
              <pre>{JSON.stringify(selectedPrediction.modelVotes, null, 2)}</pre>
              <h3>Supporting patterns</h3>
              <pre>{JSON.stringify(selectedPrediction.supportingPatterns, null, 2)}</pre>
              <h3>Feature snapshot payload</h3>
              <pre>{JSON.stringify(selectedPrediction.featureSnapshotPayload, null, 2)}</pre>
              <h3>Source-time metadata</h3>
              <pre>{JSON.stringify({
                odds_timestamp_used: selectedPrediction.featureSnapshotPayload?.odds_timestamp_used ?? null,
                stats_snapshot_cutoff: selectedPrediction.featureSnapshotPayload?.stats_snapshot_cutoff ?? null,
                injury_lineup_cutoff: selectedPrediction.featureSnapshotPayload?.injury_lineup_cutoff ?? null,
              }, null, 2)}</pre>
              {compareRunId && selectedPrediction ? (
                <div>
                  <h3>Compare/diff vs run {compareRunId}</h3>
                  {comparePrediction ? (
                    diffRows.length > 0 ? (
                      <table>
                        <thead>
                          <tr>
                            <th>Field</th>
                            <th>Current</th>
                            <th>Compare</th>
                          </tr>
                        </thead>
                        <tbody>
                          {diffRows.map((row) => (
                            <tr key={row.key}>
                              <td className="mono">{row.key}</td>
                              <td><pre>{JSON.stringify(row.current, null, 2)}</pre></td>
                              <td><pre>{JSON.stringify(row.previous, null, 2)}</pre></td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      <p className="muted">No diffs for matched prediction.</p>
                    )
                  ) : (
                    <p className="muted">No matching prediction found in compare run.</p>
                  )}
                </div>
              ) : null}
            </div>
          ) : null}
        </>
      ) : (
        <>
          {loadingRejections ? <p className="muted">Loading rejections...</p> : null}
          <table>
            <thead>
              <tr>
                <th>Run Date</th>
                <th>Run ID</th>
                <th>Game ID</th>
                <th>Pattern ID</th>
                <th>Reasons</th>
              </tr>
            </thead>
            <tbody>
              {rejections.map((row) => (
                <tr key={row.id}>
                  <td className="mono">{String(row.runDate).slice(0, 10)}</td>
                  <td className="mono">{row.runId?.slice(0, 8) ?? "n/a"}</td>
                  <td className="mono">{row.gameId}</td>
                  <td className="mono">{row.patternId}</td>
                  <td>{row.reasons.join(", ")}</td>
                </tr>
              ))}
              {rejections.length === 0 && !loadingRejections ? (
                <tr>
                  <td colSpan={5} className="muted">No rejection rows for current filters.</td>
                </tr>
              ) : null}
            </tbody>
          </table>

          {rejectionSummary.length > 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <h2 style={{ marginTop: 0 }}>Top rejection reasons</h2>
              <ul style={{ margin: 0 }}>
                {rejectionSummary.map(([reason, count]) => (
                  <li key={reason} className="mono">{reason}: {count}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}
