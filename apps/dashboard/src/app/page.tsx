"use client";

import { useEffect, useState, useCallback } from "react";
import { getProviders, signIn, useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { getTeamLogoUrl } from "@/lib/teamLogos";

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
  id?: number;
  name: string;
  stat: string;
  statValue: number;
  rationale?: string;
}

interface SuggestedPick {
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
  mlInvolved?: boolean;
  laneTag?: string;
  playerTarget?: PlayerTarget | null;
  result?: PredictionResult | null;
}

interface ModelPick {
  outcomeType: string;
  displayLabel: string;
  modelProbability: number;
  posteriorHitRate: number;
  metaScore: number | null;
  confidence: number;
  agreementCount: number;
  playerTarget?: PlayerTarget | null;
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
  result?: PredictionResult | null;
  laneTag?: string;
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
  suggestedPlays?: SuggestedPick[];
  suggestedBetPicks?: SuggestedPick[];
  modelPicks?: ModelPick[];
}

interface PredictionData {
  date: string;
  modelVersion?: string;
  gateMode?: "legacy" | "strict";
  season?: number;
  dayBetSummary?: { hits: number; total: number; hitRate: number | null };
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
  autoSynced?: boolean;
  message?: string;
}

interface ModelVersionOption {
  name: string;
  isActive: boolean;
  coverage?: {
    gradedDates: number;
    gradedPicks: number;
  };
}

function getLocalDateString(d: Date = new Date()) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseDateInput(value: string): Date {
  const m = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return new Date();
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  return new Date(year, month - 1, day);
}

function isPlayerPick(outcomeType: string): boolean {
  const base = outcomeType.replace(/:.*$/, "");
  return base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_");
}

function filterPicks<T extends { outcomeType: string }>(
  picks: T[] | undefined,
  filter: "game" | "player" | "all"
): T[] {
  if (!picks) return [];
  if (filter === "all") return picks;
  return picks.filter((p) => (filter === "player" ? isPlayerPick(p.outcomeType) : !isPlayerPick(p.outcomeType)));
}

function filterPicksByMl<T extends { mlInvolved?: boolean }>(
  picks: T[] | undefined,
  filter: "all" | "ml_only" | "no_ml",
): T[] {
  if (!picks) return [];
  if (filter === "all") return picks;
  return picks.filter((p) => (filter === "ml_only" ? !!p.mlInvolved : !p.mlInvolved));
}

function inferLaneTag(outcomeType: string): string {
  const base = (outcomeType ?? "").replace(/:.*$/, "");
  if (base.includes("WIN")) return "moneyline";
  if (base.includes("COVERED")) return "spread";
  if (base.includes("OVER") || base.includes("UNDER") || base.includes("TOTAL_")) return "total";
  if (base === "PLAYER_10_PLUS_REBOUNDS" || base.includes("REBOUNDER")) return "player_rebounds";
  if (base === "PLAYER_10_PLUS_ASSISTS" || base.includes("ASSIST") || base.includes("PLAYMAKER")) return "player_assists";
  if (base === "PLAYER_30_PLUS" || base === "PLAYER_40_PLUS" || base.includes("SCORER")) return "player_points";
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "other_prop";
  return "other";
}

function filterByLane<T extends { outcomeType: string; laneTag?: string }>(
  picks: T[] | undefined,
  lane: "all" | "moneyline" | "spread" | "total" | "player_points" | "player_rebounds" | "player_assists" | "other_prop" | "other",
): T[] {
  if (!picks) return [];
  if (lane === "all") return picks;
  return picks.filter((p) => (p.laneTag ?? inferLaneTag(p.outcomeType)) === lane);
}

export function PredictionsPage() {
  const [date, setDate] = useState(() => getLocalDateString());
  const [data, setData] = useState<PredictionData | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedGame, setExpandedGame] = useState<string | null>(null);
  const [evidenceOpenByGame, setEvidenceOpenByGame] = useState<Record<string, boolean>>({});
  const [parlayLegs, setParlayLegs] = useState<{ key: string; gameLabel: string; pickLabel: string; odds: number }[]>([]);
  const [parlayStake, setParlayStake] = useState("10");
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<{ ok: boolean; message: string; steps?: { step: string; ok: boolean; message?: string }[] } | null>(null);
  const [pickType, setPickType] = useState<"game" | "player" | "all">("game");
  const [mlFilter, setMlFilter] = useState<"all" | "ml_only" | "no_ml">("all");
  const [gateMode, setGateMode] = useState<"legacy" | "strict">("legacy");
  const [laneFilter, setLaneFilter] = useState<"all" | "moneyline" | "spread" | "total" | "player_points" | "player_rebounds" | "player_assists" | "other_prop" | "other">("all");
  const [calendarMonth, setCalendarMonth] = useState(() => {
    const d = new Date();
    return { year: d.getFullYear(), month: d.getMonth() };
  });
  const [perfectDates, setPerfectDates] = useState<Set<string>>(new Set());
  const [modelVersions, setModelVersions] = useState<ModelVersionOption[]>([]);
  const [liveCoverage, setLiveCoverage] = useState<{ gradedDates: number; gradedPicks: number }>({
    gradedDates: 0,
    gradedPicks: 0,
  });
  const [switchingModelVersion, setSwitchingModelVersion] = useState(false);

  const toggleParlayLeg = (key: string, gameLabel: string, pickLabel: string, americanOdds: number) => {
    setParlayLegs((prev) => {
      if (prev.some((l) => l.key === key)) return prev.filter((l) => l.key !== key);
      const decimal = americanOdds > 0 ? 1 + americanOdds / 100 : 1 + 100 / Math.abs(americanOdds);
      return [...prev, { key, gameLabel, pickLabel, odds: decimal }];
    });
  };

  const parlayDecimalOdds = parlayLegs.reduce((acc, l) => acc * l.odds, 1);
  const stakeNum = parseFloat(parlayStake) || 0;
  const parlayPayout = stakeNum * parlayDecimalOdds;

  const fetchPredictions = useCallback(async (targetDate: string, refreshLedger?: boolean) => {
    setLoading(true);
    try {
      const qp = new URLSearchParams();
      qp.set("date", targetDate);
      qp.set("gateMode", gateMode);
      if (refreshLedger) qp.set("refreshLedger", "1");
      const res = await fetch(`/api/predictions?${qp.toString()}`);
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
  }, [gateMode]);

  useEffect(() => {
    fetchPredictions(date);
    setParlayLegs([]);
    setSyncResult(null);
  }, [date, fetchPredictions]);

  useEffect(() => {
    fetch("/api/model-versions")
      .then((r) => r.json())
      .then((json: {
        versions?: Array<{
          name: string;
          isActive: boolean;
          coverage?: { gradedDates: number; gradedPicks: number };
        }>;
        liveCoverage?: { gradedDates: number; gradedPicks: number };
      }) => {
        setModelVersions(
          (json.versions ?? []).map((v) => ({
            name: v.name,
            isActive: v.isActive,
            coverage: v.coverage,
          })),
        );
        setLiveCoverage(json.liveCoverage ?? { gradedDates: 0, gradedPicks: 0 });
      })
      .catch(() => setModelVersions([]));
  }, []);

  useEffect(() => {
    const d = parseDateInput(date);
    setCalendarMonth((m) => {
      if (m.year === d.getFullYear() && m.month === d.getMonth()) return m;
      return { year: d.getFullYear(), month: d.getMonth() };
    });
  }, [date]);

  useEffect(() => {
    const monthStr = `${calendarMonth.year}-${String(calendarMonth.month + 1).padStart(2, "0")}`;
    const modelVersion = data?.modelVersion ?? "live";
    fetch(
      `/api/perfect-dates?month=${monthStr}&filter=${pickType}&mlFilter=${mlFilter}&lane=${laneFilter}&modelVersion=${encodeURIComponent(modelVersion)}&gateMode=${gateMode}`,
    )
      .then((r) => r.json())
      .then((json: { dates?: string[] }) => {
        setPerfectDates(new Set(json.dates ?? []));
      })
      .catch(() => setPerfectDates(new Set()));
  }, [calendarMonth.year, calendarMonth.month, pickType, mlFilter, laneFilter, gateMode, data?.modelVersion]);

  const runSync = useCallback(async () => {
    setSyncing(true);
    setSyncResult(null);
    try {
      const res = await fetch(`/api/sync?date=${date}`, { method: "POST" });
      const json = await res.json();
      setSyncResult({
        ok: json.ok,
        message: json.message ?? (res.ok ? "Sync complete" : "Sync failed"),
        steps: json.steps,
      });
      if (json.ok) {
        // Refresh with ledger update so picks get graded (hit/miss) and simulator stays in sync
        fetchPredictions(date, true);
        // Refresh perfect dates so calendar shows updated badges
        const monthStr = `${calendarMonth.year}-${String(calendarMonth.month + 1).padStart(2, "0")}`;
        const modelVersion = data?.modelVersion ?? "live";
        fetch(
          `/api/perfect-dates?month=${monthStr}&filter=${pickType}&mlFilter=${mlFilter}&lane=${laneFilter}&modelVersion=${encodeURIComponent(modelVersion)}&gateMode=${gateMode}`,
        )
          .then((r) => r.json())
          .then((j: { dates?: string[] }) => setPerfectDates(new Set(j.dates ?? [])))
          .catch(() => {});
      }
    } catch (err) {
      setSyncResult({ ok: false, message: err instanceof Error ? err.message : "Sync failed" });
    } finally {
      setSyncing(false);
    }
  }, [date, fetchPredictions, calendarMonth.year, calendarMonth.month, pickType, mlFilter, laneFilter, gateMode, data?.modelVersion]);

  const switchModelVersion = useCallback(async (next: string) => {
    setSwitchingModelVersion(true);
    try {
      const payload =
        next === "live"
          ? { action: "deactivate" }
          : { action: "activate", name: next };
      const res = await fetch("/api/model-versions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error((json as { error?: string }).error ?? "Failed to switch model version");
      }
      setModelVersions((prev) =>
        prev.map((v) => ({ ...v, isActive: next !== "live" && v.name === next })),
      );
      await fetchPredictions(date);
    } catch (err) {
      setSyncResult({
        ok: false,
        message: err instanceof Error ? err.message : "Failed to switch model version",
      });
    } finally {
      setSwitchingModelVersion(false);
    }
  }, [date, fetchPredictions]);

  const changeDate = (delta: number) => {
    setDate((prev) => {
      const current = parseDateInput(prev);
      const d = new Date(current.getFullYear(), current.getMonth(), current.getDate() + delta);
      return getLocalDateString(d);
    });
  };

  const fmtTime = (iso: string | null) => {
    if (!iso) return "";
    const d = new Date(iso);
    if (d.getHours() < 12) return "";
    return d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  };

  const streakLabel = (s: number) => {
    if (s > 0) return `W${s}`;
    if (s < 0) return `L${Math.abs(s)}`;
    return "—";
  };

  const computeGameHitSummary = (game: GamePrediction) => {
    const allResults = [
      ...(game.suggestedBetPicks ?? []).map((p) => p.result).filter((r): r is PredictionResult => r != null),
    ];
    const total = allResults.length;
    const hits = allResults.filter((r) => r.hit).length;
    return { hits, total };
  };


  const resolveLabel = (displayLabel: string | undefined, outcomeType: string, homeTeam: string, awayTeam: string) => {
    if (displayLabel) {
      return displayLabel
        .replace(/\bAway\b/g, awayTeam)
        .replace(/\bHome\b/g, homeTeam);
    }
    return contextLabel(outcomeType, homeTeam, awayTeam);
  };

  const contextLabel = (key: string, homeTeam: string, awayTeam: string) => {
    const base = key.replace(/:.*$/, "");
    const side = key.match(/:([^:]+)$/)?.[1];
    const team = side === "home" ? homeTeam : side === "away" ? awayTeam : null;
    const opp = side === "home" ? awayTeam : side === "away" ? homeTeam : null;

    switch (base) {
      case "HOME_WIN": return `${homeTeam} Win`;
      case "AWAY_WIN": return `${awayTeam} Win`;
      case "HOME_COVERED": return `${homeTeam} Cover`;
      case "AWAY_COVERED": return `${awayTeam} Cover`;
      case "OVER_HIT": return "Over";
      case "UNDER_HIT": return "Under";
      case "FAVORITE_COVERED": return "Fav Cover";
      case "UNDERDOG_COVERED": return "Dog Cover";
      case "HOME_TOP_SCORER_25_PLUS": return `${homeTeam} Top Scorer 25+`;
      case "HOME_TOP_SCORER_30_PLUS": return `${homeTeam} Top Scorer 30+`;
      case "AWAY_TOP_SCORER_25_PLUS": return `${awayTeam} Top Scorer 25+`;
      case "AWAY_TOP_SCORER_30_PLUS": return `${awayTeam} Top Scorer 30+`;
      case "HOME_TOP_REBOUNDER_10_PLUS": return `${homeTeam} Top Reb 10+`;
      case "HOME_TOP_REBOUNDER_12_PLUS": return `${homeTeam} Top Reb 12+`;
      case "AWAY_TOP_REBOUNDER_10_PLUS": return `${awayTeam} Top Reb 10+`;
      case "AWAY_TOP_REBOUNDER_12_PLUS": return `${awayTeam} Top Reb 12+`;
      case "HOME_TOP_ASSIST_8_PLUS": return `${homeTeam} Top Ast 8+`;
      case "HOME_TOP_ASSIST_10_PLUS": return `${homeTeam} Top Ast 10+`;
      case "AWAY_TOP_ASSIST_8_PLUS": return `${awayTeam} Top Ast 8+`;
      case "AWAY_TOP_ASSIST_10_PLUS": return `${awayTeam} Top Ast 10+`;
      case "MARGIN_UNDER_5": return "Within 5";
      case "MARGIN_UNDER_10": return "Within 10";
      case "BLOWOUT_20_PLUS": return "Blowout 20+";
      case "TOTAL_LINE_OVER_230": return "Total > 230";
      case "TOTAL_LINE_OVER_235": return "Total > 235";
      case "TOTAL_LINE_UNDER_210": return "Total < 210";
    }
    if (team) {
      const readable = humanizeLabel(key);
      return readable.replace(/ \((Home|Away)\)$/, "") + ` (${team})`;
    }
    return humanizeLabel(key);
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

  const filteredGames = data?.games.map((g) => ({
    ...g,
    suggestedBetPicks: filterByLane(filterPicksByMl(filterPicks(g.suggestedBetPicks, pickType), mlFilter), laneFilter),
    suggestedPlays: filterByLane(filterPicks(g.suggestedPlays, pickType), laneFilter),
    discoveryV2Matches: filterByLane(filterPicks(g.discoveryV2Matches, pickType), laneFilter),
  })) ?? [];
  const totalPicks = filteredGames.reduce((sum, g) => sum + (g.suggestedBetPicks?.length ?? 0), 0);

  const dayHitSummary = (() => {
    let hits = 0;
    let total = 0;
    for (const game of filteredGames) {
      for (const pick of game.suggestedBetPicks ?? []) {
        if (pick.result != null) {
          total++;
          if (pick.result.hit) hits++;
        }
      }
    }
    return { hits, total, hitRate: total > 0 ? hits / total : null };
  })();

  return (
    <>
      <div style={{ marginBottom: "1rem" }}>
        {!loading && data ? (
          <h1 style={{ margin: 0, fontSize: "1.4rem", fontWeight: 800 }}>
            {data.games.length} Games, {totalPicks} Picks
            <span className="muted" style={{ marginLeft: "0.5rem", fontSize: "0.85rem", fontWeight: 400 }}>
              model: {data.modelVersion ?? "live"}
            </span>
            {pickType !== "all" && <span className="muted" style={{ fontSize: "0.9rem", fontWeight: 400 }}> ({pickType})</span>}
            {mlFilter !== "all" && (
              <span className="muted" style={{ fontSize: "0.9rem", fontWeight: 400 }}>
                {" "}({mlFilter === "ml_only" ? "ml only" : "no ml"})
              </span>
            )}
            {gateMode === "strict" && (
              <span className="muted" style={{ fontSize: "0.9rem", fontWeight: 400 }}>
                {" "}(strict gates)
              </span>
            )}
            {laneFilter !== "all" && (
              <span className="muted" style={{ fontSize: "0.9rem", fontWeight: 400 }}>
                {" "}(lane: {laneFilter})
              </span>
            )}
            {dayHitSummary.total > 0 && (
              <span style={{
                color: dayHitSummary.hits === dayHitSummary.total ? "#a855f7" : (dayHitSummary.hitRate ?? 0) >= 0.5 ? "var(--success)" : "var(--error)",
                marginLeft: "0.5rem",
                fontSize: "1.1rem",
              }}>
                {dayHitSummary.hits}/{dayHitSummary.total} ({((dayHitSummary.hitRate ?? 0) * 100).toFixed(0)}%)
                {dayHitSummary.hits === dayHitSummary.total && " PERFECT"}
              </span>
            )}
            {data.seasonToDate && data.seasonToDate.v2.total > 0 && (
              <span className="muted" style={{ marginLeft: "0.5rem", fontSize: "0.85rem", fontWeight: 400 }}>
                Season {data.seasonToDate.v2.hits}/{data.seasonToDate.v2.total} ({((data.seasonToDate.v2.hitRate ?? 0) * 100).toFixed(1)}%)
              </span>
            )}
          </h1>
        ) : (
          <h1 style={{ margin: 0, fontSize: "1.4rem", fontWeight: 800, opacity: 0.4 }}>&nbsp;</h1>
        )}
        {!loading && data && totalPicks === 0 && perfectDates.has(date) && (
          <div className="muted" style={{ marginTop: "0.35rem", fontSize: "0.78rem" }}>
            This date is marked perfect for graded ledger picks under current filters, but no picks are currently visible.
          </div>
        )}
      </div>

      <div className="predictions-layout">
      {/* Games column */}
      <div className="predictions-main">
      {loading && (
        <div className="card" style={{ opacity: 0.8 }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <div className="spinner" />
            <span className="muted">Loading predictions...</span>
          </div>
        </div>
      )}

      {!loading && data?.autoSynced && (data?.games?.length ?? 0) > 0 && (
        <div style={{ background: "var(--bg-elevated)", border: "1px solid var(--accent-cyan)", borderRadius: "8px", padding: "0.5rem 1rem", marginBottom: "1rem", fontSize: "0.82rem", color: "var(--accent-cyan)" }}>
          Data was auto-synced for {date}
        </div>
      )}
      {!loading && data?.games.length === 0 && (
        <div className="card">
          <p>No games found for {date}{data?.autoSynced ? " (auto-sync was attempted)" : ""}. Try syncing upcoming games or selecting a different date.</p>
        </div>
      )}

      {!loading && data && filteredGames.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: "0.75rem", alignItems: "start" }}>
          {[...filteredGames].sort((a, b) => (b.suggestedBetPicks?.length ?? 0) - (a.suggestedBetPicks?.length ?? 0)).map((game) => {
            const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeam.id}`;
            const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeam.id}`;
            const isExpanded = expandedGame === game.id;
            const isFinal = game.status?.includes("Final");
            const gameHitSummary = computeGameHitSummary(game);
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

            const awayLogo = getTeamLogoUrl(game.awayTeam.code);
            const homeLogo = getTeamLogoUrl(game.homeTeam.code);
            const hasPicks = (game.suggestedBetPicks?.length ?? 0) > 0;

            return (
              <div
                key={game.id}
                className="card"
                style={{ padding: 0, overflow: "hidden", cursor: "pointer" }}
                onClick={() => setExpandedGame(isExpanded ? null : game.id)}
              >
                {/* Matchup row */}
                <div style={{ display: "flex", alignItems: "center", padding: "0.75rem 1.25rem", gap: "1rem" }}>
                  {/* Away team */}
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", flex: 1, minWidth: 0 }}>
                    {awayLogo && <img src={awayLogo} alt={awayLabel} style={{ width: 40, height: 40, objectFit: "contain", flexShrink: 0 }} />}
                    <div>
                      <div style={{ fontWeight: 700, fontSize: "0.95rem" }}>{awayLabel}</div>
                      {game.context.away && (
                        <div className="muted" style={{ fontSize: "0.75rem" }}>
                          {game.context.away.record}{" "}
                          <span style={{ color: game.context.away.streak > 0 ? "var(--success)" : game.context.away.streak < 0 ? "var(--error)" : undefined }}>
                            {streakLabel(game.context.away.streak)}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Center score / status */}
                  <div style={{ textAlign: "center", flexShrink: 0, minWidth: "5rem" }}>
                    {isFinal ? (
                      <div>
                        <span style={{ fontWeight: 800, fontSize: "1.15rem", fontVariantNumeric: "tabular-nums" }}>
                          {game.awayScore} - {game.homeScore}
                        </span>
                        <span className="badge badge-green" style={{ fontSize: "0.65rem", marginLeft: "0.4rem", verticalAlign: "middle" }}>F</span>
                      </div>
                    ) : (
                      <div className="muted" style={{ fontSize: "0.9rem" }}>
                        {fmtTime(game.tipoff) || "vs"}
                      </div>
                    )}
                    {gameHitSummary.total > 0 && (
                      <div style={{ marginTop: "0.2rem" }}>
                        <span
                          className="badge"
                          style={{
                            background: "var(--bg-elevated)",
                            color: gameHitSummary.hits / gameHitSummary.total >= 0.5 ? "var(--success)" : "var(--error)",
                            borderColor: gameHitSummary.hits / gameHitSummary.total >= 0.5 ? "var(--success)" : "var(--error)",
                            fontSize: "0.7rem",
                          }}
                        >
                          {gameHitSummary.hits}/{gameHitSummary.total} hits
                        </span>
                      </div>
                    )}
                  </div>

                  {/* Home team */}
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", flex: 1, minWidth: 0, justifyContent: "flex-end" }}>
                    <div style={{ textAlign: "right" }}>
                      <div style={{ fontWeight: 700, fontSize: "0.95rem" }}>{homeLabel}</div>
                      {game.context.home && (
                        <div className="muted" style={{ fontSize: "0.75rem" }}>
                          {game.context.home.record}{" "}
                          <span style={{ color: game.context.home.streak > 0 ? "var(--success)" : game.context.home.streak < 0 ? "var(--error)" : undefined }}>
                            {streakLabel(game.context.home.streak)}
                          </span>
                        </div>
                      )}
                    </div>
                    {homeLogo && <img src={homeLogo} alt={homeLabel} style={{ width: 40, height: 40, objectFit: "contain", flexShrink: 0 }} />}
                  </div>
                </div>

                {/* Picks strip */}
                {hasPicks && (
                  <div style={{
                    borderTop: "1px solid var(--border)",
                    padding: "0.6rem 1.5rem",
                    display: "flex",
                    flexWrap: "wrap",
                    gap: "0.4rem",
                  }}>
                    {game.suggestedBetPicks!.map((play, i) => {
                      const pickColor = play.result
                        ? (play.result.hit ? "var(--success)" : "var(--error)")
                        : "var(--accent-orange)";
                      const legKey = `${game.id}|${play.outcomeType}|${i}`;
                      const inParlay = parlayLegs.some((l) => l.key === legKey);
                      const label = resolveLabel(play.displayLabel, play.outcomeType, homeLabel, awayLabel);
                      const matchupLabel = `${awayLabel} @ ${homeLabel}`;
                      const americanOdds = play.marketPick?.overPrice ?? -110;
                      return (
                        <span
                          key={legKey}
                          className="badge"
                          style={{
                            background: inParlay ? pickColor : "var(--bg-elevated)",
                            color: inParlay ? "white" : pickColor,
                            borderColor: pickColor,
                            fontSize: "0.78rem",
                            cursor: "pointer",
                          }}
                          title={`${(play.posteriorHitRate * 100).toFixed(1)}% | edge ${(play.edge * 100).toFixed(1)}% | meta ${play.metaScore != null ? (play.metaScore * 100).toFixed(1) : "n/a"}% — click to add to parlay`}
                          onClick={(e) => { e.stopPropagation(); toggleParlayLeg(legKey, matchupLabel, label, americanOdds); }}
                        >
                          {label}
                          <span style={{ opacity: 0.7, marginLeft: "0.3rem" }}>
                            {play.metaScore != null ? `${(play.metaScore * 100).toFixed(0)}%` : ""}
                          </span>
                          {play.result && (
                            <span style={{ marginLeft: "0.3rem" }}>
                              {play.result.hit ? "✓" : "✗"}
                            </span>
                          )}
                        </span>
                      );
                    })}
                  </div>
                )}

                {isExpanded && (
                  <div style={{ borderTop: "1px solid var(--border)", padding: "1rem 1.5rem" }} onClick={(e) => e.stopPropagation()}>
                    {(game.suggestedBetPicks?.length ?? 0) > 0 && (
                      <div style={{ marginBottom: "1rem" }}>
                        <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Bettable Picks</h4>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                          {game.suggestedBetPicks?.map((play) => (
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
                                <strong>{resolveLabel(play.displayLabel, play.outcomeType, homeLabel, awayLabel)}</strong> | {(play.posteriorHitRate * 100).toFixed(1)}% | edge {(play.edge * 100).toFixed(2)}% | meta {play.metaScore != null ? (play.metaScore * 100).toFixed(1) : "n/a"}% | {play.votes} votes
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

                    {(game.modelPicks?.length ?? 0) > 0 && (
                      <div style={{ marginBottom: "1rem" }}>
                        <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Model Picks <span className="muted" style={{ fontWeight: 400, fontSize: "0.75rem" }}>(accuracy-focused, no market requirement)</span></h4>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                          {game.modelPicks!.map((mp) => (
                            <div
                              key={mp.outcomeType}
                              style={{
                                background: "var(--bg-elevated)",
                                border: "1px solid var(--border)",
                                borderLeft: `4px solid ${mp.result ? (mp.result.hit ? "var(--success)" : "var(--error)") : "var(--accent-purple, #a78bfa)"}`,
                                borderRadius: "8px",
                                padding: "0.5rem 0.6rem",
                                fontSize: "0.82rem",
                              }}
                            >
                              <div>
                                <strong>{resolveLabel(mp.displayLabel, mp.outcomeType, homeLabel, awayLabel)}</strong> | prob {(mp.modelProbability * 100).toFixed(1)}% | {mp.agreementCount} patterns agree
                                {mp.metaScore != null ? ` | meta ${(mp.metaScore * 100).toFixed(1)}%` : ""}
                                {mp.marketPick ? " | has market" : ""}
                              </div>
                              {mp.playerTarget && (
                                <div className="muted" style={{ marginTop: "0.25rem" }}>
                                  Target: <strong style={{ color: "var(--accent-cyan)" }}>{mp.playerTarget.name}</strong>{" "}
                                  ({mp.playerTarget.statValue.toFixed(1)} {mp.playerTarget.stat})
                                </div>
                              )}
                              {mp.result && (
                                <div style={{ marginTop: "0.3rem" }}>
                                  <span
                                    className="badge"
                                    style={{
                                      background: mp.result.hit ? "var(--success)" : "var(--error)",
                                      color: "white",
                                      border: "none",
                                    }}
                                  >
                                    {mp.result.hit ? "HIT" : "MISS"}
                                  </span>
                                  {mp.result.explanation && (
                                    <span style={{ marginLeft: "0.5rem", color: "var(--text-secondary)" }}>
                                      {mp.result.explanation}
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
                                    <strong>{contextLabel(p.outcomeType, homeLabel, awayLabel)}</strong> | posterior {(p.posteriorHitRate * 100).toFixed(1)}% | edge {(p.edge * 100).toFixed(2)}% | n={p.n}
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
      </div>

      {/* Sidebar: Date + Parlay */}
      <div className="predictions-sidebar">
      {/* Date picker + Sync card */}
      <div className="card" style={{ padding: "1rem" }}>
        <h3 style={{ margin: "0 0 0.75rem", fontSize: "0.95rem" }}>Date & Sync</h3>
        {/* Calendar */}
        <div style={{ marginBottom: "0.75rem" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.5rem" }}>
            <button
              type="button"
              onClick={() => setCalendarMonth((m) => {
                const d = new Date(m.year, m.month - 1);
                return { year: d.getFullYear(), month: d.getMonth() };
              })}
              className="btn-today"
              style={{ padding: "0.25rem 0.5rem", fontSize: "0.8rem" }}
            >
              &larr;
            </button>
            <span style={{ fontSize: "0.9rem", fontWeight: 600 }}>
              {new Date(calendarMonth.year, calendarMonth.month).toLocaleString("default", { month: "long", year: "numeric" })}
            </span>
            <button
              type="button"
              onClick={() => setCalendarMonth((m) => {
                const d = new Date(m.year, m.month + 1);
                return { year: d.getFullYear(), month: d.getMonth() };
              })}
              className="btn-today"
              style={{ padding: "0.25rem 0.5rem", fontSize: "0.8rem" }}
            >
              &rarr;
            </button>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 2, fontSize: "0.7rem", textAlign: "center" }}>
            {["S", "M", "T", "W", "T", "F", "S"].map((d, i) => (
              <div key={i} className="muted" style={{ fontWeight: 600 }}>{d}</div>
            ))}
            {(() => {
              const first = new Date(calendarMonth.year, calendarMonth.month, 1);
              const last = new Date(calendarMonth.year, calendarMonth.month + 1, 0);
              const startPad = first.getDay();
              const days: (number | null)[] = Array(startPad).fill(null);
              for (let d = 1; d <= last.getDate(); d++) days.push(d);
              const today = new Date();
              const todayStr = getLocalDateString(today);
              return days.map((d, i) => {
                if (d === null) return <div key={i} />;
                const dateStr = `${calendarMonth.year}-${String(calendarMonth.month + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
                const isSelected = dateStr === date;
                const isToday = dateStr === todayStr;
                const isPerfect = perfectDates.has(dateStr);
                return (
                  <button
                    key={i}
                    type="button"
                    onClick={() => setDate(dateStr)}
                    title={isPerfect ? "Perfect – all picks hit" : undefined}
                    style={{
                      padding: "0.35rem",
                      border: "1px solid",
                      borderColor: isSelected ? "var(--accent)" : isPerfect ? "#a855f7" : "var(--border)",
                      borderRadius: 4,
                      background: isSelected
                        ? "var(--accent-muted)"
                        : isPerfect
                          ? "rgba(168, 85, 247, 0.12)"
                          : isToday
                            ? "var(--bg-elevated)"
                            : "transparent",
                      color: isSelected ? "var(--accent)" : isPerfect ? "#a855f7" : "inherit",
                      boxShadow: isSelected ? "0 0 0 1px var(--accent)" : "none",
                      fontWeight: isSelected ? 700 : isToday ? 700 : isPerfect ? 600 : 400,
                      cursor: "pointer",
                      fontSize: "0.75rem",
                    }}
                  >
                    {d}
                  </button>
                );
              });
            })()}
          </div>
        </div>
        <button type="button" onClick={() => setDate(getLocalDateString())} className="btn-today" style={{ width: "100%", marginBottom: "0.5rem", fontSize: "0.8rem" }}>
          Today
        </button>
        <div style={{ marginBottom: "0.5rem" }}>
          <div className="muted" style={{ fontSize: "0.8rem", marginBottom: "0.25rem" }}>
            Model version <span style={{ opacity: 0.75 }}>(season coverage)</span>
          </div>
          <select
            value={data?.modelVersion ?? "live"}
            onChange={(e) => switchModelVersion(e.target.value)}
            disabled={switchingModelVersion}
            style={{ width: "100%", padding: "0.35rem 0.4rem", fontSize: "0.78rem" }}
          >
            <option value="live">
              live (no snapshot) [season: {liveCoverage.gradedDates}d/{liveCoverage.gradedPicks}p]
            </option>
            {modelVersions.map((v) => (
              <option key={v.name} value={v.name}>
                {v.name}
                {v.isActive ? " (active)" : ""}
                {` [season: ${v.coverage?.gradedDates ?? 0}d/${v.coverage?.gradedPicks ?? 0}p]`}
              </option>
            ))}
          </select>
          <div className="muted" style={{ fontSize: "0.72rem", marginTop: "0.25rem" }}>
            Coverage is graded season totals, not picks for selected date.
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.5rem" }}>
          <span className="muted" style={{ fontSize: "0.8rem" }}>Picks:</span>
          {(["game", "player", "all"] as const).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setPickType(t)}
              style={{
                flex: 1,
                padding: "0.3rem 0.4rem",
                fontSize: "0.75rem",
                border: "1px solid var(--border)",
                borderRadius: 4,
                background: pickType === t ? "var(--accent-muted)" : "transparent",
                cursor: "pointer",
              }}
            >
              {t === "game" ? "Game" : t === "player" ? "Player" : "All"}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.5rem" }}>
          <span className="muted" style={{ fontSize: "0.8rem" }}>ML vote:</span>
          {(["all", "ml_only", "no_ml"] as const).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setMlFilter(t)}
              style={{
                flex: 1,
                padding: "0.3rem 0.4rem",
                fontSize: "0.72rem",
                border: "1px solid var(--border)",
                borderRadius: 4,
                background: mlFilter === t ? "var(--accent-muted)" : "transparent",
                cursor: "pointer",
              }}
            >
              {t === "all" ? "All" : t === "ml_only" ? "ML" : "No ML"}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.5rem" }}>
          <span className="muted" style={{ fontSize: "0.8rem" }}>Gates:</span>
          {(["legacy", "strict"] as const).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setGateMode(t)}
              style={{
                flex: 1,
                padding: "0.3rem 0.4rem",
                fontSize: "0.72rem",
                border: "1px solid var(--border)",
                borderRadius: 4,
                background: gateMode === t ? "var(--accent-muted)" : "transparent",
                cursor: "pointer",
              }}
            >
              {t === "legacy" ? "Legacy" : "Strict"}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.5rem" }}>
          <span className="muted" style={{ fontSize: "0.8rem" }}>Lane:</span>
          <select
            value={laneFilter}
            onChange={(e) => setLaneFilter(e.target.value as typeof laneFilter)}
            style={{ width: "100%", padding: "0.3rem 0.4rem", fontSize: "0.72rem" }}
          >
            <option value="all">All lanes</option>
            <option value="moneyline">moneyline</option>
            <option value="spread">spread</option>
            <option value="total">total</option>
            <option value="player_points">player_points</option>
            <option value="player_rebounds">player_rebounds</option>
            <option value="player_assists">player_assists</option>
            <option value="other_prop">other_prop</option>
            <option value="other">other</option>
          </select>
        </div>
        {mlFilter !== "all" && pickType === "game" && (
          <div className="muted" style={{ fontSize: "0.72rem", marginBottom: "0.5rem" }}>
            ML vote applies to player-point picks. Switch Picks to Player or All.
          </div>
        )}
        <button
          type="button"
          onClick={runSync}
          disabled={syncing}
          className="btn-today"
          style={{ width: "100%", minHeight: "2rem" }}
          title="Sync games, odds, injuries, and lineups for this date"
        >
          {syncing ? "…" : "Sync"}
        </button>
        {syncResult && (
          <div title={syncResult.steps?.map((s) => `${s.step}: ${s.ok ? "ok" : s.message ?? "fail"}`).join("\n")} style={{ fontSize: "0.75rem", color: syncResult.ok ? "var(--success)" : "var(--error)", marginTop: "0.5rem" }}>
            {syncResult.message}
            {syncResult.steps && ` (${syncResult.steps.filter((s) => s.ok).length}/${syncResult.steps.length})`}
          </div>
        )}
      </div>

      {/* Parlay builder */}
      <div className="card" style={{ padding: "1rem", flex: 1 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.75rem", gap: "0.4rem" }}>
          <h3 style={{ margin: 0, fontSize: "1rem" }}>Parlay Builder</h3>
          <div style={{ display: "flex", gap: "0.3rem" }}>
            {data && (
              <button
                className="btn-today"
                style={{ fontSize: "0.7rem", padding: "0.2rem 0.5rem" }}
                onClick={() => {
                  const legs: typeof parlayLegs = [];
                  for (const game of filteredGames) {
                    const hl = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeam.id}`;
                    const al = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeam.id}`;
                    const ml = `${al} @ ${hl}`;
                    for (const [i, play] of (game.suggestedBetPicks ?? []).entries()) {
                      const key = `${game.id}|${play.outcomeType}|${i}`;
                      const label = resolveLabel(play.displayLabel, play.outcomeType, hl, al);
                      const american = play.marketPick?.overPrice ?? -110;
                      const decimal = american > 0 ? 1 + american / 100 : 1 + 100 / Math.abs(american);
                      legs.push({ key, gameLabel: ml, pickLabel: label, odds: decimal });
                    }
                  }
                  setParlayLegs(legs);
                }}
              >
                Add All
              </button>
            )}
            <button
              className="btn-today"
              style={{ fontSize: "0.7rem", padding: "0.2rem 0.5rem" }}
              onClick={() => setParlayLegs([])}
              disabled={parlayLegs.length === 0}
            >
              Clear All
            </button>
          </div>
        </div>
        {parlayLegs.length === 0 ? (
          <p className="muted" style={{ fontSize: "0.82rem", margin: 0 }}>Click picks to add legs</p>
        ) : (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: "0.4rem", marginBottom: "0.75rem" }}>
              {parlayLegs.map((leg) => (
                <div
                  key={leg.key}
                  style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: "0.8rem", padding: "0.3rem 0.4rem", background: "var(--bg-elevated)", borderRadius: 6 }}
                >
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{leg.pickLabel}</div>
                    <div className="muted" style={{ fontSize: "0.7rem" }}>{leg.gameLabel}</div>
                  </div>
                  <button
                    onClick={() => setParlayLegs((prev) => prev.filter((l) => l.key !== leg.key))}
                    style={{ background: "none", border: "none", color: "var(--error)", cursor: "pointer", padding: "0 0.3rem", fontSize: "1rem", lineHeight: 1 }}
                  >
                    &times;
                  </button>
                </div>
              ))}
            </div>

            <div style={{ borderTop: "1px solid var(--border)", paddingTop: "0.6rem" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.5rem" }}>
                <label className="muted" style={{ fontSize: "0.8rem", flexShrink: 0 }}>Stake $</label>
                <input
                  type="number"
                  value={parlayStake}
                  onChange={(e) => setParlayStake(e.target.value)}
                  style={{ width: "100%", padding: "0.35rem 0.5rem", fontSize: "0.9rem" }}
                  min="0"
                  step="5"
                />
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.82rem" }}>
                <span className="muted">Legs</span>
                <span>{parlayLegs.length}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.82rem" }}>
                <span className="muted">Combined odds</span>
                <span>{parlayDecimalOdds.toFixed(2)}x</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "1rem", fontWeight: 800, marginTop: "0.4rem" }}>
                <span>Payout</span>
                <span style={{ color: "var(--success)" }}>${parlayPayout.toFixed(2)}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.82rem", marginTop: "0.15rem" }}>
                <span className="muted">Profit</span>
                <span style={{ color: "var(--success)" }}>${(parlayPayout - stakeNum).toFixed(2)}</span>
              </div>
            </div>

          </>
        )}
      </div>
      </div>
      <style jsx>{`
        .predictions-layout {
          display: grid;
          grid-template-columns: minmax(0, 1fr) 300px;
          gap: 1rem;
          align-items: start;
        }

        .predictions-main {
          min-width: 0;
        }

        .predictions-sidebar {
          width: 300px;
          min-width: 0;
          display: flex;
          flex-direction: column;
          gap: 1rem;
          align-self: start;
          position: sticky;
          top: 1rem;
        }

        @media (max-width: 1100px) {
          .predictions-layout {
            grid-template-columns: minmax(0, 1fr);
          }

          .predictions-sidebar {
            width: 100%;
            position: static;
            top: auto;
          }
        }
      `}</style>
      </div>
    </>
  );
}

export default function LandingPage() {
  const router = useRouter();
  const { data: session, status } = useSession();
  const [providers, setProviders] = useState<Record<string, { id: string; name: string }> | null>(null);

  useEffect(() => {
    let mounted = true;
    getProviders()
      .then((result) => {
        if (mounted) {
          setProviders(result as Record<string, { id: string; name: string }> | null);
        }
      })
      .catch(() => {
        if (mounted) {
          setProviders({});
        }
      });
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    if (status === "authenticated" && session?.user) {
      router.replace("/predictions");
    }
  }, [router, session, status]);

  const googleAvailable = !!providers?.google;
  const appleAvailable = !!providers?.apple;
  const devAvailable = !!providers?.["dev-login"];
  const hasProvider = googleAvailable || appleAvailable || devAvailable;
  const callbackUrl =
    typeof window !== "undefined"
      ? `${window.location.origin}/predictions`
      : "/predictions";

  return (
    <div className="auth-landing">
      <div className="auth-card">
        <div className="auth-eyebrow">Bluey Intelligence</div>
        <h1>NBA prediction workspace</h1>
        <p>
          Private dashboard for model-driven picks, simulator workflows, and saved user context.
        </p>

        {status === "loading" || status === "authenticated" ? (
          <p className="muted">Checking session...</p>
        ) : (
          <div className="auth-actions">
            {googleAvailable ? (
              <button
                type="button"
                className="auth-primary-btn"
                onClick={() => signIn("google", { callbackUrl })}
              >
                Continue with Google
              </button>
            ) : null}
            {appleAvailable ? (
              <button
                type="button"
                className="auth-secondary-btn"
                onClick={() => signIn("apple", { callbackUrl })}
              >
                Continue with Apple
              </button>
            ) : null}
            {devAvailable ? (
              <button
                type="button"
                className="auth-secondary-btn"
                onClick={() => signIn("dev-login", { callbackUrl })}
              >
                Continue in Dev Mode
              </button>
            ) : null}
            {!hasProvider ? (
              <p className="muted" style={{ margin: "0.25rem 0 0", fontSize: "0.82rem" }}>
                No auth provider configured yet. Add OAuth credentials or enable dev auth in `apps/dashboard/.env`.
              </p>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
