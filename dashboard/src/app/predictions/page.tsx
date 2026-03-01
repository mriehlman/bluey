"use client";

import { useEffect, useState, useCallback } from "react";

interface TeamInfo {
  id: number;
  code: string | null;
  name: string | null;
}

interface TeamContext {
  record: string;
  ppg: number;
  oppg: number;
  rankOff: number | null;
  rankDef: number | null;
  streak: number;
}

interface Prediction {
  conditions: string[];
  outcome: string;
  hitRate: number;
  hitCount: number;
  sampleSize: number;
  seasons: number;
  edge: number;
}

interface GamePrediction {
  id: string;
  homeTeam: TeamInfo;
  awayTeam: TeamInfo;
  tipoff: string | null;
  status: string | null;
  homeScore: number;
  awayScore: number;
  odds: {
    spreadHome: number | null;
    totalOver: number | null;
    mlHome: number | null;
    mlAway: number | null;
  } | null;
  context: {
    home: TeamContext | null;
    away: TeamContext | null;
  };
  predictions: Prediction[];
  predictionCount: number;
}

interface PredictionData {
  date: string;
  games: GamePrediction[];
  message?: string;
}

export default function PredictionsPage() {
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [data, setData] = useState<PredictionData | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedGame, setExpandedGame] = useState<string | null>(null);

  const fetchPredictions = useCallback(async (targetDate: string) => {
    setLoading(true);
    try {
      const res = await fetch(`/api/predictions?date=${targetDate}`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPredictions(date);
  }, [date, fetchPredictions]);

  const changeDate = (delta: number) => {
    const d = new Date(date);
    d.setDate(d.getDate() + delta);
    setDate(d.toISOString().slice(0, 10));
  };

  const fmtOdds = (n: number | null | undefined) => {
    if (n == null) return "—";
    return n > 0 ? `+${n}` : `${n}`;
  };

  const fmtTime = (iso: string | null) => {
    if (!iso) return "";
    const d = new Date(iso);
    return d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  };

  const streakLabel = (s: number) => {
    if (s > 0) return `W${s}`;
    if (s < 0) return `L${Math.abs(s)}`;
    return "—";
  };

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1>Predictions</h1>
        <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
          <button onClick={() => changeDate(-1)} style={{ padding: "0.5rem 1rem" }}>
            &larr; Prev
          </button>
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            style={{ padding: "0.5rem", fontSize: "1rem" }}
          />
          <button onClick={() => changeDate(1)} style={{ padding: "0.5rem 1rem" }}>
            Next &rarr;
          </button>
        </div>
      </div>

      <p className="muted" style={{ marginBottom: "1rem" }}>
        Pattern-based predictions for {date}. Shows games with matching condition patterns from historical data.
      </p>

      {loading && <p>Loading...</p>}

      {!loading && data?.games.length === 0 && (
        <div className="card">
          <p>No games found for {date}. Try syncing upcoming games or selecting a different date.</p>
        </div>
      )}

      {!loading && data && data.games.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          {data.games.map((game) => {
            const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeam.id}`;
            const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeam.id}`;
            const isExpanded = expandedGame === game.id;
            const isFinal = game.status?.includes("Final");

            return (
              <div key={game.id} className="card">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                  <div>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", marginBottom: "0.25rem" }}>
                      <strong style={{ fontSize: "1.1rem" }}>
                        {awayLabel} @ {homeLabel}
                      </strong>
                      {game.tipoff && !isFinal && (
                        <span className="badge badge-gray">{fmtTime(game.tipoff)}</span>
                      )}
                      {isFinal && (
                        <span className="badge badge-green">
                          {game.awayScore} - {game.homeScore}
                        </span>
                      )}
                      {game.predictionCount > 0 && (
                        <span className="badge" style={{ background: "var(--bg-elevated)", color: "var(--accent-cyan)", borderColor: "var(--accent-cyan)" }}>
                          {game.predictionCount} predictions
                        </span>
                      )}
                    </div>

                    {game.odds && (
                      <div className="muted" style={{ fontSize: "0.85rem", marginBottom: "0.5rem" }}>
                        Spread: {homeLabel} {fmtOdds(game.odds.spreadHome)} | 
                        O/U: {game.odds.totalOver ?? "—"} | 
                        ML: {homeLabel} {fmtOdds(game.odds.mlHome)} / {awayLabel} {fmtOdds(game.odds.mlAway)}
                      </div>
                    )}

                    {game.context.home && game.context.away && (
                      <div style={{ display: "flex", gap: "2rem", fontSize: "0.85rem" }}>
                        <div>
                          <span className="muted">{homeLabel}:</span>{" "}
                          {game.context.home.record}{" "}
                          <span className="muted">
                            (Off #{game.context.home.rankOff ?? "?"}, Def #{game.context.home.rankDef ?? "?"})
                          </span>{" "}
                          <span style={{ color: game.context.home.streak > 0 ? "var(--success)" : game.context.home.streak < 0 ? "var(--error)" : undefined }}>
                            {streakLabel(game.context.home.streak)}
                          </span>
                        </div>
                        <div>
                          <span className="muted">{awayLabel}:</span>{" "}
                          {game.context.away.record}{" "}
                          <span className="muted">
                            (Off #{game.context.away.rankOff ?? "?"}, Def #{game.context.away.rankDef ?? "?"})
                          </span>{" "}
                          <span style={{ color: game.context.away.streak > 0 ? "var(--success)" : game.context.away.streak < 0 ? "var(--error)" : undefined }}>
                            {streakLabel(game.context.away.streak)}
                          </span>
                        </div>
                      </div>
                    )}
                  </div>

                  <button
                    onClick={() => setExpandedGame(isExpanded ? null : game.id)}
                    style={{ background: "#6b7280", flexShrink: 0 }}
                  >
                    {isExpanded ? "Hide" : "Show"} Details
                  </button>
                </div>

                {isExpanded && (
                  <div style={{ marginTop: "1rem", borderTop: "1px solid var(--border)", paddingTop: "1rem" }}>
                    {game.predictions.length === 0 ? (
                      <p className="muted">No matching patterns found for this game.</p>
                    ) : (
                      <>
                        <h4 style={{ marginTop: 0, marginBottom: "0.75rem" }}>Matching Patterns</h4>
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          {game.predictions.map((pred, i) => (
                            <div
                              key={i}
                              style={{
                                background: "var(--bg-elevated)",
                                border: "1px solid var(--border)",
                                padding: "0.75rem",
                                fontSize: "0.85rem",
                              }}
                            >
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                                <div>
                                  <div style={{ marginBottom: "0.25rem" }}>
                                    <span className="muted">Conditions:</span>{" "}
                                    {pred.conditions.map((c) => c.replace(/:.*/, "")).join(" + ")}
                                  </div>
                                  <div>
                                    <span className="muted">Outcome:</span>{" "}
                                    <strong style={{ color: "var(--accent-orange)" }}>{pred.outcome.replace(/:.*/, "")}</strong>
                                  </div>
                                </div>
                                <div style={{ textAlign: "right" }}>
                                  <div style={{ color: pred.edge >= 5 ? "var(--success)" : pred.edge >= 0 ? "var(--accent-cyan)" : "var(--error)" }}>
                                    {(pred.hitRate * 100).toFixed(1)}%
                                    <span style={{ fontSize: "0.75rem", marginLeft: "0.25rem" }}>
                                      ({pred.edge >= 0 ? "+" : ""}{pred.edge.toFixed(1)}% edge)
                                    </span>
                                  </div>
                                  <div className="muted" style={{ fontSize: "0.75rem" }}>
                                    {pred.hitCount}/{pred.sampleSize} over {pred.seasons} seasons
                                  </div>
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </>
  );
}
