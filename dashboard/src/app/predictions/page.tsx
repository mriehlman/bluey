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
  scope?: "target" | "outcome";
}

interface PlayerTarget {
  name: string;
  stat: string;
  statValue: number;
  rationale?: string;
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
  discoveryV2Matches?: {
    id: string;
    outcomeType: string;
    conditions: string[];
    posteriorHitRate: number;
    edge: number;
    score: number;
    n: number;
    playerTarget?: PlayerTarget | null;
    result?: PredictionResult | null;
  }[];
  suggestedPlays?: {
    outcomeType: string;
    displayLabel?: string;
    confidence: number;
    posteriorHitRate: number;
    edge: number;
    metaScore?: number | null;
    votes: number;
    marketPick?: {
      market: string;
      line: number;
      overPrice: number;
      impliedProb: number;
      estimatedProb: number;
      edge: number;
      ev: number;
      label: string;
    } | null;
    playerTarget?: PlayerTarget | null;
    result?: PredictionResult | null;
  }[];
}

interface PredictionData {
  date: string;
  season?: number;
  seasonToDate?: {
    throughDate: string;
    v2: { hits: number; total: number; hitRate: number | null };
  };
  wagerTracking?: {
    stakePerPick: number;
    bankrollStart: number;
    day: {
      date: string;
      bets: number;
      settledBets: number;
      pendingBets: number;
      wins: number;
      losses: number;
      totalStaked: number;
      settledStaked: number;
      netPnl: number;
      roi: number | null;
    };
    seasonToDate: {
      throughDate: string;
      bets: number;
      settledBets: number;
      pendingBets: number;
      wins: number;
      losses: number;
      totalStaked: number;
      settledStaked: number;
      netPnl: number;
      roi: number | null;
      bankrollCurrent: number;
    };
  } | null;
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
  const [evidenceOpenByGame, setEvidenceOpenByGame] = useState<Record<string, boolean>>({});

  const fetchPredictions = useCallback(async (targetDate: string) => {
    setLoading(true);
    try {
      const res = await fetch(`/api/predictions?date=${targetDate}`);
      const text = await res.text();
      let json: PredictionData | null = null;
      try {
        json = text ? (JSON.parse(text) as PredictionData) : null;
      } catch {
        throw new Error(`Predictions API returned invalid JSON (status ${res.status})`);
      }
      if (!res.ok) {
        const message = (json as { message?: string } | null)?.message ?? `HTTP ${res.status}`;
        throw new Error(`Predictions API failed: ${message}`);
      }
      setData(json);
    } catch (err) {
      console.error(err);
      setData((prev) => ({
        date: targetDate,
        games: prev?.games ?? [],
        message: err instanceof Error ? err.message : "Failed to load predictions",
      }));
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

  const computeGameHitSummary = (game: GamePrediction) => {
    const allResults = [
      ...(game.suggestedPlays ?? []).map((p) => p.result).filter((r): r is PredictionResult => r != null),
    ];
    const total = allResults.length;
    const hits = allResults.filter((r) => r.hit).length;
    return { hits, total };
  };

  const dayHitSummary = (() => {
    if (!data) return { hits: 0, total: 0 };
    return data.games.reduce(
      (acc, game) => {
        const s = computeGameHitSummary(game);
        acc.hits += s.hits;
        acc.total += s.total;
        return acc;
      },
      { hits: 0, total: 0 },
    );
  })();

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
        Discovery v2 suggested picks and market-backed signals for {date}.
      </p>

      {!loading && data && (
        <div className="card" style={{ marginBottom: "1rem" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "1rem", flexWrap: "wrap" }}>
            <strong>Day Summary</strong>
            <div>
              {dayHitSummary.total > 0 ? (
                <>
                  <strong>{dayHitSummary.hits}/{dayHitSummary.total}</strong> graded picks hit
                  <span className="muted" style={{ marginLeft: "0.5rem" }}>
                    ({((dayHitSummary.hits / dayHitSummary.total) * 100).toFixed(1)}%)
                  </span>
                </>
              ) : (
                <span className="muted">No graded results yet.</span>
              )}
            </div>
          </div>
          {data.seasonToDate && (
            <div style={{ marginTop: "0.5rem", fontSize: "0.9rem" }}>
              <div>
                <span className="muted">Season {data.season ?? "?"} to {data.seasonToDate.throughDate} (v2 deployed): </span>
                {data.seasonToDate.v2.total > 0 ? (
                  <>
                    <strong>{data.seasonToDate.v2.hits}/{data.seasonToDate.v2.total}</strong>
                    <span className="muted" style={{ marginLeft: "0.5rem" }}>
                      ({((data.seasonToDate.v2.hitRate ?? 0) * 100).toFixed(1)}%)
                    </span>
                  </>
                ) : (
                  <span className="muted">No graded picks yet</span>
                )}
              </div>
            </div>
          )}
          {data.wagerTracking && (
            <div style={{ marginTop: "0.75rem", fontSize: "0.9rem", borderTop: "1px solid var(--border)", paddingTop: "0.6rem" }}>
              <div>
                <span className="muted">Wager tracker ({data.wagerTracking.day.date}, ${data.wagerTracking.stakePerPick.toFixed(2)} flat): </span>
                <strong>{data.wagerTracking.day.settledBets}</strong>
                <span className="muted"> settled, </span>
                <strong>{data.wagerTracking.day.pendingBets}</strong>
                <span className="muted"> pending, W-L </span>
                <strong>{data.wagerTracking.day.wins}-{data.wagerTracking.day.losses}</strong>
                <span className="muted"> | P&L </span>
                <strong style={{ color: data.wagerTracking.day.netPnl >= 0 ? "var(--success)" : "var(--error)" }}>
                  {data.wagerTracking.day.netPnl >= 0 ? "+" : ""}${data.wagerTracking.day.netPnl.toFixed(2)}
                </strong>
                <span className="muted"> | ROI </span>
                <strong>
                  {data.wagerTracking.day.roi != null
                    ? `${(data.wagerTracking.day.roi * 100).toFixed(1)}%`
                    : "n/a"}
                </strong>
              </div>
              <div>
                <span className="muted">Season to {data.wagerTracking.seasonToDate.throughDate} bankroll: </span>
                <strong>${data.wagerTracking.seasonToDate.bankrollCurrent.toFixed(2)}</strong>
                <span className="muted"> (start ${data.wagerTracking.bankrollStart.toFixed(2)}) | P&L </span>
                <strong style={{ color: data.wagerTracking.seasonToDate.netPnl >= 0 ? "var(--success)" : "var(--error)" }}>
                  {data.wagerTracking.seasonToDate.netPnl >= 0 ? "+" : ""}${data.wagerTracking.seasonToDate.netPnl.toFixed(2)}
                </strong>
                <span className="muted"> | ROI </span>
                <strong>
                  {data.wagerTracking.seasonToDate.roi != null
                    ? `${(data.wagerTracking.seasonToDate.roi * 100).toFixed(1)}%`
                    : "n/a"}
                </strong>
                <span className="muted"> | W-L </span>
                <strong>
                  {data.wagerTracking.seasonToDate.wins}-{data.wagerTracking.seasonToDate.losses}
                </strong>
              </div>
            </div>
          )}
        </div>
      )}

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
            const gameHitSummary = computeGameHitSummary(game);
            const recommendedPlay = game.suggestedPlays?.[0] ?? null;
            const collapsedDiscoveryV2 = (() => {
              const rows = game.discoveryV2Matches ?? [];
              const grouped = new Map<
                string,
                (typeof rows)[number] & { support: number }
              >();
              for (const row of rows) {
                const targetKey = row.playerTarget?.id != null ? `player:${row.playerTarget.id}` : "player:none";
                const key = `${row.outcomeType}|${targetKey}`;
                const existing = grouped.get(key);
                if (!existing) {
                  grouped.set(key, { ...row, support: 1 });
                } else {
                  existing.support += 1;
                  if (row.score > existing.score) {
                    grouped.set(key, { ...row, support: existing.support });
                  }
                }
              }
              return [...grouped.values()]
                .sort((a, b) => b.score - a.score || b.edge - a.edge)
                .slice(0, 6);
            })();

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
                      {gameHitSummary.total > 0 && (
                        <span
                          className="badge"
                          style={{
                            background: "var(--bg-elevated)",
                            color: gameHitSummary.hits / gameHitSummary.total >= 0.5 ? "var(--success)" : "var(--error)",
                            borderColor: gameHitSummary.hits / gameHitSummary.total >= 0.5 ? "var(--success)" : "var(--error)",
                          }}
                        >
                          Hits: {gameHitSummary.hits}/{gameHitSummary.total}
                        </span>
                      )}
                      {recommendedPlay && (
                        <span
                          className="badge"
                          style={{
                            background: "var(--bg-elevated)",
                            color: "var(--accent-orange)",
                            borderColor: "var(--accent-orange)",
                            maxWidth: "520px",
                            whiteSpace: "nowrap",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                          }}
                          title={`Recommended: ${recommendedPlay.displayLabel ?? humanizeLabel(recommendedPlay.outcomeType)} (${(recommendedPlay.posteriorHitRate * 100).toFixed(1)}%, edge ${(recommendedPlay.edge * 100).toFixed(2)}%, meta ${recommendedPlay.metaScore != null ? (recommendedPlay.metaScore * 100).toFixed(1) : "n/a"}%, ${recommendedPlay.votes} votes)`}
                        >
                          Recommended: {recommendedPlay.displayLabel ?? humanizeLabel(recommendedPlay.outcomeType)} ({(recommendedPlay.posteriorHitRate * 100).toFixed(1)}% | edge {(recommendedPlay.edge * 100).toFixed(2)}% | meta {recommendedPlay.metaScore != null ? (recommendedPlay.metaScore * 100).toFixed(1) : "n/a"}% | {recommendedPlay.votes}v)
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
                        <div className="muted" style={{ marginBottom: "0.5rem", fontSize: "0.82rem" }}>
                          Suggested action: start with{" "}
                          <strong style={{ color: "var(--accent-cyan)" }}>
                            {humanizeLabel(game.suggestedPlays?.[0]?.outcomeType ?? "")}
                          </strong>
                          {" "}and use vote count + edge as confidence.
                        </div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                          {game.suggestedPlays?.map((play) => (
                            <div
                              key={play.outcomeType}
                              style={{
                                background: "var(--bg-elevated)",
                                border: "1px solid var(--border)",
                                borderLeft: `4px solid ${play.result ? (play.result.hit ? "var(--success)" : "var(--error)") : "var(--accent-cyan)"}`,
                                borderRadius: "8px",
                                padding: "0.5rem 0.6rem",
                                fontSize: "0.82rem",
                              }}
                            >
                              <div>
                                <strong>{play.displayLabel ?? humanizeLabel(play.outcomeType)}</strong> | {(play.posteriorHitRate * 100).toFixed(1)}% | edge {(play.edge * 100).toFixed(2)}% | meta {play.metaScore != null ? (play.metaScore * 100).toFixed(1) : "n/a"}% | {play.votes} votes
                              </div>
                              {play.marketPick && (
                                <div className="muted" style={{ marginTop: "0.25rem" }}>
                                  Market EV: {(play.marketPick.ev * 100).toFixed(1)}% | est {(play.marketPick.estimatedProb * 100).toFixed(1)}% vs implied {(play.marketPick.impliedProb * 100).toFixed(1)}%
                                </div>
                              )}
                              {!play.marketPick && (
                                <div className="muted" style={{ marginTop: "0.25rem" }}>
                                  No market-backed line available for this game yet.
                                </div>
                              )}
                              {play.playerTarget && (
                                <div className="muted" style={{ marginTop: "0.25rem" }}>
                                  Target: <strong style={{ color: "var(--accent-cyan)" }}>{play.playerTarget.name}</strong>{" "}
                                  ({play.playerTarget.statValue.toFixed(1)} {play.playerTarget.stat})
                                  {play.playerTarget.rationale ? ` - ${play.playerTarget.rationale}` : ""}
                                </div>
                              )}
                              {play.result && (
                                <div style={{ marginTop: "0.3rem" }}>
                                  <span
                                    className="badge"
                                    style={{
                                      background: play.result.hit ? "var(--success)" : "var(--error)",
                                      color: "white",
                                      border: "none",
                                    }}
                                  >
                                    {play.result.scope === "target"
                                      ? play.result.hit
                                        ? "TARGET HIT"
                                        : "TARGET MISS"
                                      : play.result.hit
                                        ? "OUTCOME HIT"
                                        : "OUTCOME MISS"}
                                  </span>
                                  {play.result.explanation && (
                                    <span style={{ marginLeft: "0.5rem", color: "var(--text-secondary)" }}>
                                      {play.result.explanation}
                                    </span>
                                  )}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {(game.discoveryV2Matches?.length ?? 0) === 0 ? (
                      <p className="muted">No matching patterns found for this game.</p>
                    ) : null}

                    {collapsedDiscoveryV2.length > 0 && (
                      <div style={{ marginTop: "1rem" }}>
                        <button
                          className="btn-today"
                          onClick={() =>
                            setEvidenceOpenByGame((prev) => ({
                              ...prev,
                              [game.id]: !prev[game.id],
                            }))
                          }
                        >
                          {evidenceOpenByGame[game.id] ? "Hide evidence" : "Show evidence"}
                        </button>
                        {evidenceOpenByGame[game.id] && (
                          <div style={{ marginTop: "0.75rem" }}>
                            <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Discovery v2 Deployed Matches</h4>
                            <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                              {collapsedDiscoveryV2.map((p) => (
                                <div key={p.id} style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", padding: "0.6rem", fontSize: "0.82rem" }}>
                                  <div>
                                    <strong>{humanizeLabel(p.outcomeType)}</strong> | posterior {(p.posteriorHitRate * 100).toFixed(1)}% | edge {(p.edge * 100).toFixed(2)}% | n={p.n}
                                    {p.support > 1 ? ` | ${p.support} pattern variants` : ""}
                                  </div>
                                  <div className="muted">{p.conditions.join(" + ")}</div>
                                  {p.playerTarget && (
                                    <div className="muted" style={{ marginTop: "0.25rem" }}>
                                      Target: <strong style={{ color: "var(--accent-cyan)" }}>{p.playerTarget.name}</strong>{" "}
                                      ({p.playerTarget.statValue.toFixed(1)} {p.playerTarget.stat})
                                      {p.playerTarget.rationale ? ` - ${p.playerTarget.rationale}` : ""}
                                    </div>
                                  )}
                                  {p.result && (
                                    <div style={{ marginTop: "0.35rem" }}>
                                      <span
                                        className="badge"
                                        style={{
                                          background: p.result.hit ? "var(--success)" : "var(--error)",
                                          color: "white",
                                          border: "none",
                                        }}
                                      >
                                        {p.result.scope === "target"
                                          ? p.result.hit
                                            ? "TARGET HIT"
                                            : "TARGET MISS"
                                          : p.result.hit
                                            ? "OUTCOME HIT"
                                            : "OUTCOME MISS"}
                                      </span>
                                      {p.result.explanation && (
                                        <span style={{ marginLeft: "0.5rem", color: "var(--text-secondary)" }}>
                                          {p.result.explanation}
                                        </span>
                                      )}
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
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
