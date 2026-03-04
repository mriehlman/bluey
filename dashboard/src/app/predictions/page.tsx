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

interface PredictionResult {
  hit: boolean;
  explanation: string | null;
}

interface PlayerTarget {
  name: string;
  stat: string;
  statValue: number;
}

interface PropLine {
  line: number;
  market: string;
}

interface RecentPerformance {
  hits: number;
  total: number;
}

interface Prediction {
  conditions: string[];
  outcome: string;
  hitRate: number;
  hitCount: number;
  sampleSize: number;
  seasons: number;
  edge: number;
  playerTarget: PlayerTarget | null;
  propLine: PropLine | null;
  recent: RecentPerformance | null;
  isHighValue: boolean;
  result: PredictionResult | null;
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
  discoveryV2Matches?: {
    id: string;
    outcomeType: string;
    conditions: string[];
    posteriorHitRate: number;
    edge: number;
    score: number;
    n: number;
  }[];
  suggestedPlays?: {
    outcomeType: string;
    confidence: number;
    posteriorHitRate: number;
    edge: number;
    votes: number;
  }[];
}

interface PredictionData {
  date: string;
  games: GamePrediction[];
  message?: string;
}

function getLocalDateString(d: Date = new Date()) {
  return d.toLocaleDateString("en-CA"); // YYYY-MM-DD in local timezone
}

export default function PredictionsPage() {
  const [date, setDate] = useState(() => getLocalDateString());
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
    const [y, m, day] = date.split("-").map(Number);
    const d = new Date(y, m - 1, day + delta);
    setDate(getLocalDateString(d));
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

  const humanizeLabel = (key: string, includeSide = false) => {
    const sideMatch = key.match(/:([^:]+)$/);
    const side = sideMatch?.[1];
    const base = key.replace(/:.*$/, "");
    const labelMap: Record<string, string> = {
      TOP_5_OFF: "Top 5 Offense",
      TOP_10_OFF: "Top 10 Offense",
      BOTTOM_10_OFF: "Bottom 10 Offense",
      BOTTOM_5_OFF: "Bottom 5 Offense",
      TOP_5_DEF: "Top 5 Defense",
      TOP_10_DEF: "Top 10 Defense",
      BOTTOM_10_DEF: "Bottom 10 Defense",
      BOTTOM_5_DEF: "Bottom 5 Defense",
      BOTH_TOP_10_PACE: "Both Top 10 Pace",
      BOTH_BOTTOM_10_PACE: "Both Bottom 10 Pace",
      PACE_MISMATCH: "Pace Mismatch",
      ON_B2B: "On B2B",
      BOTH_ON_B2B: "Both on B2B",
      RESTED_3_PLUS: "Rested 3+",
      RESTED_4_PLUS: "Rested 4+",
      REST_ADVANTAGE: "Rest Advantage",
      WINNING_RECORD: "Winning Record",
      WIN_STREAK_3: "3+ Win Streak",
      WIN_STREAK_5: "5+ Win Streak",
      WIN_STREAK_7: "7+ Win Streak",
      LOSING_STREAK_3: "3+ Losing Streak",
      LOSING_STREAK_5: "5+ Losing Streak",
      LOSING_STREAK_7: "7+ Losing Streak",
      TOP_OFF_VS_BOTTOM_DEF: "Top Off vs Bottom Def",
      BOTH_TOP_10_OFF: "Both Top 10 Offense",
      SPREAD_UNDER_3: "Spread < 3",
      SPREAD_3_TO_7: "Spread 3–7",
      SPREAD_OVER_10: "Spread > 10",
      TOTAL_LINE_OVER_230: "Total > 230",
      TOTAL_LINE_OVER_235: "Total > 235",
      TOTAL_LINE_UNDER_210: "Total < 210",
      BIG_FAVORITE: "Big Favorite",
      NET_RATING_PLUS_5: "Net +5",
      NET_RATING_PLUS_10: "Net +10",
      NET_RATING_MINUS_5: "Net -5",
      HIGH_SCORING: "High Scoring",
      LOW_SCORING: "Low Scoring",
      BOTH_HIGH_SCORING: "Both High Scoring",
      BOTH_LOW_SCORING: "Both Low Scoring",
      STINGY_DEF: "Stingy Defense",
      POROUS_DEF: "Porous Defense",
      BOTH_STINGY_DEF: "Both Stingy Def",
      BOTH_POROUS_DEF: "Both Porous Def",
      WIN_PCT_OVER_700: "Win% > 70%",
      WIN_PCT_OVER_600: "Win% > 60%",
      WIN_PCT_UNDER_400: "Win% < 40%",
      HAS_TOP_10_SCORER: "Has Top 10 Scorer",
      HAS_TOP_10_REBOUNDER: "Has Top 10 Rebounder",
      HAS_TOP_5_SCORER: "Has Top 5 Scorer",
      HAS_TOP_5_REBOUNDER: "Has Top 5 Rebounder",
      HAS_TOP_10_PLAYMAKER: "Has Top 10 Playmaker",
      STAR_MATCHUP: "Star Matchup",
      TOP_5_SCORER_VS_BOTTOM_10_DEF: "Top 5 Scorer vs Bottom 10 Def",
      UNDERDOG_COVERED: "Underdog Covered",
      FAVORITE_COVERED: "Favorite Covered",
      HOME_COVERED: "Home Covered",
      AWAY_COVERED: "Away Covered",
      OVER_HIT: "Over Hit",
      UNDER_HIT: "Under Hit",
      HOME_WIN: "Home Win",
      AWAY_WIN: "Away Win",
      HOME_TOP_SCORER_25_PLUS: "Home Top Scorer 25+",
      HOME_TOP_SCORER_30_PLUS: "Home Top Scorer 30+",
      AWAY_TOP_SCORER_25_PLUS: "Away Top Scorer 25+",
      AWAY_TOP_SCORER_30_PLUS: "Away Top Scorer 30+",
      HOME_TOP_REBOUNDER_10_PLUS: "Home Top Rebounder 10+",
      HOME_TOP_REBOUNDER_12_PLUS: "Home Top Rebounder 12+",
      AWAY_TOP_REBOUNDER_10_PLUS: "Away Top Rebounder 10+",
      AWAY_TOP_REBOUNDER_12_PLUS: "Away Top Rebounder 12+",
      HOME_TOP_ASSIST_8_PLUS: "Home Top Playmaker 8+",
      HOME_TOP_ASSIST_10_PLUS: "Home Top Playmaker 10+",
      AWAY_TOP_ASSIST_8_PLUS: "Away Top Playmaker 8+",
      AWAY_TOP_ASSIST_10_PLUS: "Away Top Playmaker 10+",
      MARGIN_UNDER_5: "Margin < 5",
      MARGIN_UNDER_10: "Margin < 10",
      BLOWOUT_20_PLUS: "Blowout 20+",
    };
    const label = labelMap[base] ?? base.replace(/_/g, " ");
    if (includeSide && side === "home") return `${label} (Home)`;
    if (includeSide && side === "away") return `${label} (Away)`;
    return label;
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
          <button
            onClick={() => setDate(getLocalDateString())}
            className="btn-today"
          >
            Today
          </button>
        </div>
      </div>

      <p className="muted" style={{ marginBottom: "1rem" }}>
        Pattern-based predictions for {date}. Shows games with matching condition patterns from historical data.
      </p>

      {loading && (
        <div className="card" style={{ opacity: 0.8 }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <div className="spinner" />
            <span className="muted">Loading predictions...</span>
          </div>
        </div>
      )}

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
                    className="btn-today"
                    style={{ flexShrink: 0 }}
                  >
                    {isExpanded ? "Hide" : "Show"} Details
                  </button>
                </div>

                {isExpanded && (
                  <div style={{ marginTop: "1rem", borderTop: "1px solid var(--border)", paddingTop: "1rem" }}>
                    {(game.suggestedPlays?.length ?? 0) > 0 && (
                      <div style={{ marginBottom: "1rem" }}>
                        <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Discovery v2 Suggested Plays</h4>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                          {game.suggestedPlays?.map((play) => (
                            <span key={play.outcomeType} className="badge badge-gray">
                              {humanizeLabel(play.outcomeType)} | {(play.posteriorHitRate * 100).toFixed(1)}% | edge {(play.edge * 100).toFixed(2)}% | {play.votes} votes
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {game.predictions.length === 0 ? (
                      <p className="muted">No matching patterns found for this game.</p>
                    ) : (
                      <>
                        <h4 style={{ marginTop: 0, marginBottom: "0.75rem" }}>Matching Patterns</h4>
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          {game.predictions.map((pred, i) => {
                            const hasResult = pred.result !== null;
                            const isHit = pred.result?.hit === true;
                            const borderColor = hasResult
                              ? isHit
                                ? "var(--success)"
                                : "var(--error)"
                              : pred.isHighValue
                                ? "var(--accent-orange)"
                                : "var(--border)";

                            const statLabel = pred.playerTarget?.stat === "ppg" ? "ppg" 
                              : pred.playerTarget?.stat === "rpg" ? "rpg" 
                              : pred.playerTarget?.stat === "apg" ? "apg" : "";

                            return (
                              <div
                                key={i}
                                style={{
                                  background: pred.isHighValue && !hasResult ? "var(--accent-muted)" : "var(--bg-elevated)",
                                  border: `1px solid ${borderColor}`,
                                  borderLeftWidth: hasResult || pred.isHighValue ? "4px" : "1px",
                                  padding: "0.75rem",
                                  fontSize: "0.85rem",
                                }}
                              >
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                                  <div style={{ flex: 1 }}>
                                    {/* High value badge */}
                                    {pred.isHighValue && !hasResult && (
                                      <div style={{ marginBottom: "0.5rem" }}>
                                        <span className="badge" style={{ background: "var(--accent-orange)", color: "white", border: "none" }}>
                                          HIGH VALUE
                                        </span>
                                      </div>
                                    )}

                                    <div style={{ marginBottom: "0.25rem" }}>
                                      <span className="muted">Conditions:</span>{" "}
                                      {pred.conditions.map((c) => humanizeLabel(c, true)).join(" + ")}
                                    </div>
                                    <div>
                                      <span className="muted">Outcome:</span>{" "}
                                      <strong style={{ color: "var(--accent-orange)" }}>{humanizeLabel(pred.outcome)}</strong>
                                    </div>

                                    {/* Player target with prop line */}
                                    {pred.playerTarget && (
                                      <div style={{ marginTop: "0.5rem", padding: "0.5rem", background: "var(--bg-elevated)", borderRadius: "4px" }}>
                                        <strong style={{ color: "var(--accent-cyan)" }}>{pred.playerTarget.name}</strong>
                                        <span className="muted"> ({pred.playerTarget.statValue.toFixed(1)} {statLabel})</span>
                                        {pred.propLine && (
                                          <span style={{ marginLeft: "0.75rem" }}>
                                            Line: <strong>{pred.propLine.line}</strong> {pred.propLine.market}
                                          </span>
                                        )}
                                      </div>
                                    )}

                                    {/* Result for completed games */}
                                    {hasResult && (
                                      <div style={{ marginTop: "0.5rem" }}>
                                        <span
                                          className="badge"
                                          style={{
                                            background: isHit ? "var(--success)" : "var(--error)",
                                            color: "white",
                                            border: "none",
                                          }}
                                        >
                                          {isHit ? "HIT" : "MISS"}
                                        </span>
                                        {pred.result?.explanation && (
                                          <span style={{ marginLeft: "0.5rem", color: "var(--text-secondary)" }}>
                                            {pred.result.explanation}
                                          </span>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                  <div style={{ textAlign: "right", flexShrink: 0 }}>
                                    <div style={{ color: pred.edge >= 5 ? "var(--success)" : pred.edge >= 0 ? "var(--accent-cyan)" : "var(--error)" }}>
                                      {(pred.hitRate * 100).toFixed(1)}%
                                      <span style={{ fontSize: "0.75rem", marginLeft: "0.25rem" }}>
                                        ({pred.edge >= 0 ? "+" : ""}{pred.edge.toFixed(1)}% edge)
                                      </span>
                                    </div>
                                    <div className="muted" style={{ fontSize: "0.75rem" }}>
                                      {pred.hitCount}/{pred.sampleSize} over {pred.seasons} seasons
                                    </div>
                                    {/* Recent performance */}
                                    {pred.recent && pred.recent.total > 0 && (
                                      <div style={{ fontSize: "0.75rem", marginTop: "0.25rem" }}>
                                        <span style={{ 
                                          color: pred.recent.hits / pred.recent.total >= pred.hitRate 
                                            ? "var(--success)" 
                                            : pred.recent.hits / pred.recent.total >= pred.hitRate * 0.8 
                                              ? "var(--accent-cyan)" 
                                              : "var(--error)"
                                        }}>
                                          Last 30d: {pred.recent.hits}/{pred.recent.total}
                                        </span>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </>
                    )}

                    {(game.discoveryV2Matches?.length ?? 0) > 0 && (
                      <div style={{ marginTop: "1rem" }}>
                        <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Discovery v2 Deployed Matches</h4>
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          {game.discoveryV2Matches?.slice(0, 6).map((p) => (
                            <div key={p.id} style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", padding: "0.6rem", fontSize: "0.82rem" }}>
                              <div>
                                <strong>{humanizeLabel(p.outcomeType)}</strong> | posterior {(p.posteriorHitRate * 100).toFixed(1)}% | edge {(p.edge * 100).toFixed(2)}% | n={p.n}
                              </div>
                              <div className="muted">{p.conditions.join(" + ")}</div>
                            </div>
                          ))}
                        </div>
                      </div>
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
