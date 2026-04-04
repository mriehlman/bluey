"use client";

import { useEffect, useMemo, useState } from "react";

type PatternRow = {
  id: string;
  outcomeType: string;
  conditions: string[];
  posteriorHitRate: number;
  edge: number;
  score: number;
  n: number;
  status: string;
};

type ModelVersionOption = {
  name: string;
  isActive: boolean;
  coverage?: { gradedDates: number; gradedPicks: number };
};

type SimDay = {
  date: string;
  picks: number;
  resolved: number;
  hits: number;
  misses: number;
  pending: number;
  hitRate: number | null;
};

type SimPick = {
  date: string;
  season: number;
  gameId: string;
  gameLabel: string;
  outcomeType: string;
  label: string;
  odds: number;
  confidence: number;
  posterior: number;
  meta: number | null;
  edge: number;
  mlInvolved: boolean;
  hit: boolean | null;
};

type SimResponse = {
  season: number;
  from: string;
  to: string;
  modelVersion: string;
  selectedPatternCount: number;
  selectedPatternIds: string[];
  summary: {
    days: number;
    picks: number;
    resolved: number;
    hits: number;
    hitRate: number | null;
  };
  days: SimDay[];
  picks: SimPick[];
};

function currentSeason(): number {
  const d = new Date();
  return d.getMonth() >= 9 ? d.getFullYear() : d.getFullYear() - 1;
}

function pct(v: number | null | undefined): string {
  if (v == null) return "n/a";
  return `${(v * 100).toFixed(1)}%`;
}

export default function PatternSimulatorPage() {
  const [patterns, setPatterns] = useState<PatternRow[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [season, setSeason] = useState(String(currentSeason()));
  const [modelVersions, setModelVersions] = useState<ModelVersionOption[]>([]);
  const [modelVersion, setModelVersion] = useState<string>("active");
  const [loadingPatterns, setLoadingPatterns] = useState(true);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<SimResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pickDateFilter, setPickDateFilter] = useState<string>("all");

  useEffect(() => {
    setLoadingPatterns(true);
    fetch("/api/discovery-v2?status=deployed&limit=1000")
      .then((r) => r.json())
      .then((json: { patterns?: PatternRow[] }) => setPatterns(json.patterns ?? []))
      .catch(() => setPatterns([]))
      .finally(() => setLoadingPatterns(false));
  }, []);

  useEffect(() => {
    fetch("/api/model-versions")
      .then((r) => r.json())
      .then((json: { versions?: ModelVersionOption[] }) => {
        const versions = json.versions ?? [];
        setModelVersions(versions);
        const active = versions.find((v) => v.isActive);
        if (active) setModelVersion(active.name);
      })
      .catch(() => setModelVersions([]));
  }, []);

  const filteredPatterns = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return patterns;
    return patterns.filter((p) => {
      const haystack = `${p.outcomeType} ${p.conditions.join(" ")}`.toLowerCase();
      return haystack.includes(q);
    });
  }, [patterns, search]);

  const selectedPatterns = useMemo(
    () => patterns.filter((p) => selectedIds.has(p.id)),
    [patterns, selectedIds],
  );

  const visiblePicks = useMemo(() => {
    if (!result) return [];
    if (pickDateFilter === "all") return result.picks;
    return result.picks.filter((p) => p.date === pickDateFilter);
  }, [result, pickDateFilter]);

  const togglePattern = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const runSimulation = async () => {
    const seasonNum = Number(season);
    if (!Number.isFinite(seasonNum)) {
      setError("Season must be a valid year.");
      return;
    }
    if (selectedIds.size === 0) {
      setError("Pick at least one deployed pattern first.");
      return;
    }
    setError(null);
    setRunning(true);
    setResult(null);
    setPickDateFilter("all");
    try {
      const body: {
        season: number;
        patternIds: string[];
        modelVersion?: string;
      } = {
        season: seasonNum,
        patternIds: [...selectedIds],
      };
      if (modelVersion !== "active") body.modelVersion = modelVersion;

      const res = await fetch("/api/pattern-simulator", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error((json as { error?: string }).error ?? `HTTP ${res.status}`);
      }
      setResult(json as SimResponse);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to run simulation");
    } finally {
      setRunning(false);
    }
  };

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem", alignItems: "center", flexWrap: "wrap" }}>
        <h1 style={{ margin: 0, fontSize: "1.35rem", fontWeight: 800 }}>Pattern Picker Simulator</h1>
        <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap" }}>
          <label className="muted" style={{ fontSize: "0.82rem" }}>Season</label>
          <input
            value={season}
            onChange={(e) => setSeason(e.target.value)}
            style={{ width: 90, padding: "0.35rem 0.4rem" }}
          />
          <label className="muted" style={{ fontSize: "0.82rem" }}>Model</label>
          <select value={modelVersion} onChange={(e) => setModelVersion(e.target.value)} style={{ padding: "0.35rem 0.4rem" }}>
            <option value="active">active snapshot/live</option>
            <option value="live">live</option>
            {modelVersions.map((v) => (
              <option key={v.name} value={v.name}>
                {v.name}{v.isActive ? " (active)" : ""}
              </option>
            ))}
          </select>
          <button
            type="button"
            className="btn-today"
            disabled={running || selectedIds.size === 0}
            onClick={runSimulation}
            style={{ minWidth: 120 }}
          >
            {running ? "Running..." : "Run Sim"}
          </button>
        </div>
      </div>

      <div className="muted" style={{ marginTop: "0.35rem", fontSize: "0.82rem" }}>
        Select deployed PatternV2 signals, then run day-by-day simulation using current engine logic with only selected patterns enabled.
      </div>

      {error && (
        <div className="card" style={{ marginTop: "0.75rem", borderColor: "var(--error)", color: "var(--error)" }}>
          {error}
        </div>
      )}

      <div className="picker-layout" style={{ marginTop: "0.9rem" }}>
        <div className="card" style={{ padding: "0.75rem" }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: "0.5rem", alignItems: "center", marginBottom: "0.5rem" }}>
            <h3 style={{ margin: 0, fontSize: "0.95rem" }}>Deployed Patterns</h3>
            <input
              placeholder="Search outcome/condition..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{ width: "100%", maxWidth: 280, padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            />
          </div>
          <div className="muted" style={{ fontSize: "0.78rem", marginBottom: "0.5rem" }}>
            {loadingPatterns ? "Loading..." : `${filteredPatterns.length} shown / ${patterns.length} deployed`}
          </div>
          <div style={{ maxHeight: 520, overflowY: "auto", display: "flex", flexDirection: "column", gap: "0.45rem" }}>
            {filteredPatterns.map((p) => {
              const checked = selectedIds.has(p.id);
              return (
                <label key={p.id} style={{ display: "block", border: "1px solid var(--border)", borderRadius: 8, padding: "0.5rem", cursor: "pointer", background: checked ? "var(--accent-muted)" : "transparent" }}>
                  <div style={{ display: "flex", alignItems: "flex-start", gap: "0.45rem" }}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => togglePattern(p.id)}
                      style={{ marginTop: "0.1rem" }}
                    />
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontSize: "0.83rem", fontWeight: 700 }}>{p.outcomeType}</div>
                      <div className="muted" style={{ fontSize: "0.74rem", marginTop: "0.2rem" }}>
                        post {pct(p.posteriorHitRate)} | edge {pct(p.edge)} | n {p.n} | score {p.score.toFixed(3)}
                      </div>
                      <div className="muted" style={{ fontSize: "0.72rem", marginTop: "0.22rem", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                        {p.conditions.join(" + ")}
                      </div>
                    </div>
                  </div>
                </label>
              );
            })}
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          <div className="card" style={{ padding: "0.75rem" }}>
            <h3 style={{ margin: "0 0 0.4rem", fontSize: "0.95rem" }}>Selected Patterns ({selectedPatterns.length})</h3>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem", maxHeight: 120, overflowY: "auto" }}>
              {selectedPatterns.length === 0 ? (
                <span className="muted" style={{ fontSize: "0.8rem" }}>No patterns selected yet.</span>
              ) : (
                selectedPatterns.map((p) => (
                  <span key={p.id} className="badge" style={{ fontSize: "0.72rem" }}>
                    {p.outcomeType}
                  </span>
                ))
              )}
            </div>
          </div>

          <div className="card" style={{ padding: "0.75rem" }}>
            <h3 style={{ margin: "0 0 0.5rem", fontSize: "0.95rem" }}>Results</h3>
            {!result ? (
              <div className="muted" style={{ fontSize: "0.82rem" }}>
                Run a simulation to view day-by-day and per-pick results.
              </div>
            ) : (
              <>
                <div className="muted" style={{ fontSize: "0.78rem", marginBottom: "0.4rem" }}>
                  model: {result.modelVersion} | range: {result.from} to {result.to}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0,1fr))", gap: "0.45rem", marginBottom: "0.6rem" }}>
                  <div className="card" style={{ padding: "0.45rem" }}>
                    <div className="muted" style={{ fontSize: "0.7rem" }}>Days</div>
                    <div style={{ fontWeight: 800 }}>{result.summary.days}</div>
                  </div>
                  <div className="card" style={{ padding: "0.45rem" }}>
                    <div className="muted" style={{ fontSize: "0.7rem" }}>Picks</div>
                    <div style={{ fontWeight: 800 }}>{result.summary.picks}</div>
                  </div>
                  <div className="card" style={{ padding: "0.45rem" }}>
                    <div className="muted" style={{ fontSize: "0.7rem" }}>Resolved</div>
                    <div style={{ fontWeight: 800 }}>{result.summary.resolved}</div>
                  </div>
                  <div className="card" style={{ padding: "0.45rem" }}>
                    <div className="muted" style={{ fontSize: "0.7rem" }}>Hit Rate</div>
                    <div style={{ fontWeight: 800 }}>{pct(result.summary.hitRate)}</div>
                  </div>
                </div>

                <div style={{ marginBottom: "0.45rem", display: "flex", gap: "0.45rem", alignItems: "center", flexWrap: "wrap" }}>
                  <label className="muted" style={{ fontSize: "0.78rem" }}>Pick date filter</label>
                  <select value={pickDateFilter} onChange={(e) => setPickDateFilter(e.target.value)} style={{ padding: "0.3rem 0.4rem" }}>
                    <option value="all">all dates</option>
                    {result.days.map((d) => (
                      <option key={d.date} value={d.date}>{d.date}</option>
                    ))}
                  </select>
                </div>

                <div style={{ maxHeight: 220, overflowY: "auto", marginBottom: "0.6rem" }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Picks</th>
                        <th>Hits</th>
                        <th>Resolved</th>
                        <th>Hit Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.days.map((d) => (
                        <tr key={d.date}>
                          <td>{d.date}</td>
                          <td>{d.picks}</td>
                          <td>{d.hits}</td>
                          <td>{d.resolved}</td>
                          <td>{pct(d.hitRate)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div style={{ maxHeight: 280, overflowY: "auto" }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Game</th>
                        <th>Pick</th>
                        <th>Hit</th>
                        <th>Posterior</th>
                        <th>Meta</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visiblePicks.map((p, idx) => (
                        <tr key={`${p.gameId}-${p.outcomeType}-${idx}`}>
                          <td>{p.date}</td>
                          <td>{p.gameLabel}</td>
                          <td title={p.outcomeType}>{p.label}</td>
                          <td>
                            {p.hit == null ? (
                              <span className="muted">pending</span>
                            ) : p.hit ? (
                              <span style={{ color: "var(--success)", fontWeight: 700 }}>hit</span>
                            ) : (
                              <span style={{ color: "var(--error)", fontWeight: 700 }}>miss</span>
                            )}
                          </td>
                          <td>{pct(p.posterior)}</td>
                          <td>{pct(p.meta)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      <style jsx>{`
        .picker-layout {
          display: grid;
          grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
          gap: 0.75rem;
          align-items: start;
        }
        @media (max-width: 1100px) {
          .picker-layout {
            grid-template-columns: minmax(0, 1fr);
          }
        }
      `}</style>
    </>
  );
}
