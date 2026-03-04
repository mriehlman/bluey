"use client";

import { useEffect, useMemo, useState } from "react";

type SplitStats = {
  n: number;
  wins: number;
  rawHitRate: number;
  posteriorHitRate: number;
  edge: number;
  lift: number;
};

type PatternRow = {
  id: string;
  outcomeType: string;
  conditions: string[];
  discoverySource: string;
  trainStats: SplitStats;
  valStats: SplitStats;
  forwardStats: SplitStats;
  posteriorHitRate: number;
  edge: number;
  score: number;
  lift: number;
  n: number;
  status: string;
};

type ApiResponse = {
  status: string;
  outcomeType: string | null;
  outcomes: string[];
  count: number;
  patterns: PatternRow[];
};

export default function DiscoveryV2Page() {
  const [status, setStatus] = useState("deployed");
  const [outcomeType, setOutcomeType] = useState("");
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const run = async () => {
      setLoading(true);
      try {
        const qs = new URLSearchParams({ status, limit: "100" });
        if (outcomeType) qs.set("outcomeType", outcomeType);
        const res = await fetch(`/api/discovery-v2?${qs.toString()}`);
        const json = await res.json();
        setData(json);
      } finally {
        setLoading(false);
      }
    };
    run();
  }, [status, outcomeType]);

  const rows = useMemo(() => data?.patterns ?? [], [data]);

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1>Discovery v2</h1>
        <div style={{ display: "flex", gap: "0.5rem" }}>
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="deployed">deployed</option>
            <option value="validated">validated</option>
            <option value="candidate">candidate</option>
            <option value="retired">retired</option>
          </select>
          <select value={outcomeType} onChange={(e) => setOutcomeType(e.target.value)}>
            <option value="">All outcomes</option>
            {(data?.outcomes ?? []).map((o) => (
              <option key={o} value={o}>
                {o}
              </option>
            ))}
          </select>
        </div>
      </div>

      <p className="muted" style={{ marginBottom: "1rem" }}>
        Durable patterns discovered from quantized game features. Only deployed patterns are used for live matching.
      </p>

      {loading && <div className="card">Loading Discovery v2 patterns...</div>}
      {!loading && rows.length === 0 && <div className="card">No patterns found for this filter.</div>}

      {!loading && rows.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          {rows.map((p) => (
            <div key={p.id} className="card" style={{ fontSize: "0.9rem" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem" }}>
                <div>
                  <div style={{ marginBottom: "0.35rem" }}>
                    <strong>{p.outcomeType}</strong>
                    <span className="muted"> ({p.discoverySource})</span>
                  </div>
                  <div className="muted" style={{ marginBottom: "0.35rem" }}>
                    {p.conditions.join(" + ")}
                  </div>
                  <div>
                    <span className="badge badge-gray">n {p.n}</span>{" "}
                    <span className="badge badge-gray">
                      posterior {(p.posteriorHitRate * 100).toFixed(1)}%
                    </span>{" "}
                    <span className="badge badge-gray">edge {(p.edge * 100).toFixed(2)}%</span>{" "}
                    <span className="badge badge-gray">score {p.score.toFixed(3)}</span>
                  </div>
                </div>
                <div style={{ minWidth: "320px", fontSize: "0.82rem" }}>
                  <div>
                    <strong>Train:</strong> {p.trainStats.wins}/{p.trainStats.n} | post {(p.trainStats.posteriorHitRate * 100).toFixed(1)}% | edge {(p.trainStats.edge * 100).toFixed(2)}%
                  </div>
                  <div>
                    <strong>Validate:</strong> {p.valStats.wins}/{p.valStats.n} | post {(p.valStats.posteriorHitRate * 100).toFixed(1)}% | edge {(p.valStats.edge * 100).toFixed(2)}%
                  </div>
                  <div>
                    <strong>Forward:</strong> {p.forwardStats.wins}/{p.forwardStats.n} | post {(p.forwardStats.posteriorHitRate * 100).toFixed(1)}% | edge {(p.forwardStats.edge * 100).toFixed(2)}%
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </>
  );
}
