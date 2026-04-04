"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
import { signIn, useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { getTeamLogoUrl } from "@/lib/teamLogos";

interface WeekDaySummary {
  date: string;
  games: number;
  picks: number;
  hits: number;
  settled: number;
}

interface WeekSummaryData {
  weekOf: string;
  sunday: string;
  saturday: string;
  days: WeekDaySummary[];
}

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] as const;

function getSundayOfWeek(dateStr: string): string {
  const m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return dateStr;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  d.setDate(d.getDate() - d.getDay());
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function formatMonthDay(dateStr: string): { month: string; day: number } {
  const m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return { month: "", day: 0 };
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return { month: months[Number(m[2]) - 1], day: Number(m[3]) };
}

function addDaysStr(dateStr: string, delta: number): string {
  const m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return dateStr;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  d.setDate(d.getDate() + delta);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function weekdayShortFromDate(dateStr: string): string {
  const m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return "";
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return DAY_NAMES[d.getDay()];
}

function useVisibleDayCount(): number {
  const [n, setN] = useState(7);
  useEffect(() => {
    const mq7 = window.matchMedia("(min-width: 920px)");
    const mq5 = window.matchMedia("(min-width: 560px)");
    const update = () => {
      setN(mq7.matches ? 7 : mq5.matches ? 5 : 3);
    };
    update();
    mq7.addEventListener("change", update);
    mq5.addEventListener("change", update);
    return () => {
      mq7.removeEventListener("change", update);
      mq5.removeEventListener("change", update);
    };
  }, []);
  return n;
}

const MAX_FUTURE_DAYS = 2;

function getMaxDate(todayStr: string): string {
  const m = todayStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return todayStr;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  d.setDate(d.getDate() + MAX_FUTURE_DAYS);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function WeekNavigator({
  selectedDate,
  onSelectDate,
  todayStr,
  gateMode,
}: {
  selectedDate: string;
  onSelectDate: (date: string) => void;
  todayStr: string;
  gateMode: string;
}) {
  const visibleCount = useVisibleDayCount();
  const [weekSummaries, setWeekSummaries] = useState<WeekSummaryData[]>([]);
  const [weekLoading, setWeekLoading] = useState(false);

  const maxDate = useMemo(() => getMaxDate(todayStr), [todayStr]);

  const offsetBefore = Math.floor((visibleCount - 1) / 2);
  const windowDates = useMemo(
    () => Array.from({ length: visibleCount }, (_, i) => addDaysStr(todayStr, i - offsetBefore)),
    [todayStr, visibleCount, offsetBefore],
  );

  const sundaysToFetch = useMemo(() => {
    const set = new Set<string>();
    for (const ds of windowDates) {
      set.add(getSundayOfWeek(ds));
    }
    return [...set].sort();
  }, [windowDates]);

  useEffect(() => {
    let cancelled = false;
    setWeekLoading(true);
    Promise.all(
      sundaysToFetch.map((sunday) =>
        fetch(`/api/predictions/week-summary?weekOf=${sunday}&gateMode=${gateMode}`, { cache: "no-store" }).then((r) =>
          r.json(),
        ),
      ),
    )
      .then((results: WeekSummaryData[]) => {
        if (!cancelled) setWeekSummaries(results);
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) setWeekLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [sundaysToFetch, gateMode]);

  const dayByDate = useMemo(() => {
    const m = new Map<string, WeekDaySummary>();
    for (const w of weekSummaries) {
      for (const d of w.days) {
        m.set(d.date, d);
      }
    }
    return m;
  }, [weekSummaries]);

  const visibleDays = useMemo(
    () =>
      windowDates.map((dateStr) => {
        const found = dayByDate.get(dateStr);
        if (found) return found;
        return { date: dateStr, games: 0, picks: 0, hits: 0, settled: 0 };
      }),
    [windowDates, dayByDate],
  );

  const rangeLabel = (() => {
    if (visibleDays.length === 0) return "";
    const first = formatMonthDay(visibleDays[0].date);
    const last = formatMonthDay(visibleDays[visibleDays.length - 1].date);
    if (first.month === last.month) {
      return `${first.month} ${first.day} – ${last.day}`;
    }
    return `${first.month} ${first.day} – ${last.month} ${last.day}`;
  })();

  const prevDay = addDaysStr(selectedDate, -1);
  const nextDay = addDaysStr(selectedDate, 1);

  return (
    <div className="week-navigator">
      <button
        type="button"
        className="week-nav-arrow"
        onClick={() => onSelectDate(prevDay)}
        title="Previous day"
      >
        ‹
      </button>

      <div className="week-days">
        <div className="week-label">{rangeLabel}</div>
        <div
          className="week-day-cards"
          style={{ gridTemplateColumns: `repeat(${visibleCount}, minmax(0, 1fr))` }}
        >
          {visibleDays.map((day) => {
            const isSelected = day.date === selectedDate;
            const isToday = day.date === todayStr;
            const isPast = day.date < todayStr;
            const isDisabled = day.date > maxDate;
            const { day: dayNum } = formatMonthDay(day.date);
            const dayName = weekdayShortFromDate(day.date);
            return (
              <button
                key={day.date}
                type="button"
                className={[
                  "week-day-card",
                  isSelected && "week-day-selected",
                  isToday && "week-day-today",
                  isDisabled && "week-day-disabled",
                ].filter(Boolean).join(" ")}
                onClick={() => !isDisabled && onSelectDate(day.date)}
                disabled={isDisabled}
              >
                <span className="week-day-name">{dayName}</span>
                <span className="week-day-num">{dayNum}</span>
                <span className="week-day-games">
                  {isDisabled ? "—" : weekLoading ? "\u00A0" : day.games > 0 ? `${day.games} game${day.games !== 1 ? "s" : ""}` : "—"}
                </span>
                <span
                  className="week-day-hits"
                  style={{
                    color: !isDisabled && !weekLoading && isPast && day.picks > 0 && day.settled > 0
                      ? (day.hits / day.settled >= 0.5 ? "var(--success)" : "var(--error)")
                      : "var(--text-muted)",
                    visibility: isDisabled || weekLoading || day.games === 0 || (!isPast || day.picks === 0) ? "hidden" : "visible",
                  }}
                >
                  {isPast && day.picks > 0 && day.settled > 0 ? `${day.hits}/${day.settled}` : isPast && day.picks > 0 ? `${day.picks} bets` : "\u00A0"}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      <button
        type="button"
        className="week-nav-arrow"
        onClick={() => {
          if (nextDay <= maxDate) onSelectDate(nextDay);
        }}
        disabled={nextDay > maxDate}
        title="Next day"
      >
        ›
      </button>
    </div>
  );
}

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
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(d);
  const year = parts.find((p) => p.type === "year")!.value;
  const month = parts.find((p) => p.type === "month")!.value;
  const day = parts.find((p) => p.type === "day")!.value;
  return `${year}-${month}-${day}`;
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

function isPlayerProp(outcomeType: string): boolean {
  const base = (outcomeType ?? "").replace(/:.*$/, "");
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return true;
  if (base.includes("WIN") || base.includes("COVERED") || base.startsWith("OVER_") || base.startsWith("UNDER_") || base.startsWith("TOTAL_")) return false;
  return false;
}

function filterByMinVotes<T extends { votes?: number; outcomeType: string }>(
  picks: T[] | undefined,
  gameMinVotes: number,
  playerMinVotes: number,
): T[] {
  if (!picks) return [];
  if (gameMinVotes <= 1 && playerMinVotes <= 1) return picks;
  return picks.filter((p) => {
    const min = isPlayerProp(p.outcomeType) ? playerMinVotes : gameMinVotes;
    return (p.votes ?? 1) >= min;
  });
}

export function PredictionsPage() {
  const [date, setDate] = useState(() => getLocalDateString());
  const [data, setData] = useState<PredictionData | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedGame, setExpandedGame] = useState<string | null>(null);
  const [evidenceOpenByGame, setEvidenceOpenByGame] = useState<Record<string, boolean>>({});
  const [parlayLegs, setParlayLegs] = useState<{ key: string; gameLabel: string; pickLabel: string; odds: number; american: number }[]>([]);
  const [parlayStake, setParlayStake] = useState("10");
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<{ ok: boolean; message: string; steps?: { step: string; ok: boolean; message?: string }[] } | null>(null);
  const [pickType, setPickType] = useState<"game" | "player" | "all">("game");
  const [mlFilter, setMlFilter] = useState<"all" | "ml_only" | "no_ml">("all");
  const [gateMode, setGateMode] = useState<"legacy" | "strict">("legacy");
  const [laneFilter, setLaneFilter] = useState<"all" | "moneyline" | "spread" | "total" | "player_points" | "player_rebounds" | "player_assists" | "other_prop" | "other">("all");
  const [gameMinVotes, setGameMinVotes] = useState(1);
  const [playerMinVotes, setPlayerMinVotes] = useState(1);
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
      return [...prev, { key, gameLabel, pickLabel, odds: decimal, american: americanOdds }];
    });
  };

  const parlayDecimalOdds = parlayLegs.reduce((acc, l) => acc * l.odds, 1);
  const stakeNum = parseFloat(parlayStake) || 0;
  const parlayPayout = stakeNum * parlayDecimalOdds;
  const parlayAmericanOdds = parlayDecimalOdds <= 1
    ? "+0"
    : parlayDecimalOdds >= 2
      ? `+${Math.round((parlayDecimalOdds - 1) * 100).toLocaleString()}`
      : `${Math.round(-100 / (parlayDecimalOdds - 1))}`;

  const fetchPredictions = useCallback(async (targetDate: string, refreshLedger?: boolean) => {
    setLoading(true);
    try {
      const qp = new URLSearchParams();
      qp.set("date", targetDate);
      qp.set("gateMode", gateMode);
      if (refreshLedger) qp.set("refreshLedger", "1");
      const res = await fetch(`/api/predictions?${qp.toString()}`, { cache: "no-store" });
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
    const max = getMaxDate(getLocalDateString());
    if (date > max) {
      setData(null);
      setLoading(false);
      return;
    }
    fetchPredictions(date);
    setParlayLegs([]);
    setSyncResult(null);
  }, [date, fetchPredictions]);

  useEffect(() => {
    fetch("/api/model-versions", { cache: "no-store" })
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
        fetchPredictions(date, true);
      }
    } catch (err) {
      setSyncResult({ ok: false, message: err instanceof Error ? err.message : "Sync failed" });
    } finally {
      setSyncing(false);
    }
  }, [date, fetchPredictions]);

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
    suggestedBetPicks: filterByMinVotes(filterByLane(filterPicksByMl(filterPicks(g.suggestedBetPicks, pickType), mlFilter), laneFilter), gameMinVotes, playerMinVotes),
    suggestedPlays: filterByMinVotes(filterByLane(filterPicks(g.suggestedPlays, pickType), laneFilter), gameMinVotes, playerMinVotes),
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

  const todayStr = getLocalDateString();
  const maxDate = getMaxDate(todayStr);
  const isBeyondLimit = date > maxDate;

  return (
    <>
      <WeekNavigator
        selectedDate={date}
        onSelectDate={setDate}
        todayStr={todayStr}
        gateMode={gateMode}
      />

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1rem", position: "relative" }}>
        <div>
          {!loading && data ? (
            <h1 style={{ margin: 0, fontSize: "1.4rem", fontWeight: 800 }}>
              {data.games.length} Games, {totalPicks} Picks
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
                  color: (dayHitSummary.hitRate ?? 0) >= 0.5 ? "var(--success)" : "var(--error)",
                  marginLeft: "0.5rem",
                  fontSize: "1.1rem",
                }}>
                  {dayHitSummary.hits}/{dayHitSummary.total} ({((dayHitSummary.hitRate ?? 0) * 100).toFixed(0)}%)
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
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", flexShrink: 0 }}>
          <button
            type="button"
            onClick={() => setDate(getLocalDateString())}
            className="btn-today"
            style={{ fontSize: "0.72rem", padding: "0.3rem 0.6rem" }}
          >
            Today
          </button>
          <button
            type="button"
            onClick={runSync}
            disabled={syncing}
            className="btn-today"
            style={{ fontSize: "0.72rem", padding: "0.3rem 0.6rem" }}
            title="Sync games, odds, injuries, and lineups for this date"
          >
            {syncing ? "…" : "Sync"}
          </button>
          <button
            type="button"
            className="filters-gear-btn"
            onClick={() => setFiltersOpen((v) => !v)}
            title="Filters & settings"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
              <circle cx="12" cy="12" r="3" />
            </svg>
          </button>
        </div>
        {syncResult && (
          <div
            title={syncResult.steps?.map((s) => `${s.step}: ${s.ok ? "ok" : s.message ?? "fail"}`).join("\n")}
            style={{ position: "absolute", right: 0, top: "100%", fontSize: "0.75rem", color: syncResult.ok ? "var(--success)" : "var(--error)", whiteSpace: "nowrap" }}
          >
            {syncResult.message}
            {syncResult.steps && ` (${syncResult.steps.filter((s) => s.ok).length}/${syncResult.steps.length})`}
          </div>
        )}
      </div>

      {/* Filters modal */}
      {filtersOpen && (
        <>
          <div className="filters-backdrop" onClick={() => setFiltersOpen(false)} />
          <div className="filters-modal">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1rem" }}>
              <h3 style={{ margin: 0, fontSize: "1rem" }}>Filters & Settings</h3>
              <button
                type="button"
                onClick={() => setFiltersOpen(false)}
                style={{ background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", padding: "0.2rem", fontSize: "1.2rem", lineHeight: 1 }}
              >
                &times;
              </button>
            </div>
            <div style={{ marginBottom: "0.75rem" }}>
              <div className="muted" style={{ fontSize: "0.8rem", marginBottom: "0.25rem" }}>
                Model version <span style={{ opacity: 0.75 }}>(season coverage)</span>
              </div>
              <select
                value={data?.modelVersion ?? "live"}
                onChange={(e) => switchModelVersion(e.target.value)}
                disabled={switchingModelVersion}
                style={{ width: "100%", padding: "0.4rem 0.5rem", fontSize: "0.82rem" }}
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
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.75rem" }}>
              <span className="muted" style={{ fontSize: "0.8rem", minWidth: "3.5rem" }}>Picks:</span>
              {(["game", "player", "all"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setPickType(t)}
                  style={{
                    flex: 1,
                    padding: "0.35rem 0.4rem",
                    fontSize: "0.78rem",
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
            <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.75rem" }}>
              <span className="muted" style={{ fontSize: "0.8rem", minWidth: "3.5rem" }}>ML vote:</span>
              {(["all", "ml_only", "no_ml"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setMlFilter(t)}
                  style={{
                    flex: 1,
                    padding: "0.35rem 0.4rem",
                    fontSize: "0.78rem",
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
            <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.75rem" }}>
              <span className="muted" style={{ fontSize: "0.8rem", minWidth: "3.5rem" }}>Gates:</span>
              {(["legacy", "strict"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setGateMode(t)}
                  style={{
                    flex: 1,
                    padding: "0.35rem 0.4rem",
                    fontSize: "0.78rem",
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
            <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.75rem" }}>
              <span className="muted" style={{ fontSize: "0.8rem", minWidth: "3.5rem" }}>Lane:</span>
              <select
                value={laneFilter}
                onChange={(e) => setLaneFilter(e.target.value as typeof laneFilter)}
                style={{ width: "100%", padding: "0.4rem 0.5rem", fontSize: "0.78rem" }}
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
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.75rem", marginBottom: "0.5rem" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
                <span className="muted" style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>Game votes:</span>
                <select
                  value={gameMinVotes}
                  onChange={(e) => setGameMinVotes(Number(e.target.value))}
                  style={{ width: "100%", padding: "0.4rem 0.5rem", fontSize: "0.78rem" }}
                >
                  <option value={1}>1+</option>
                  <option value={2}>2+</option>
                  <option value={3}>3+</option>
                  <option value={4}>4+</option>
                  <option value={5}>5+</option>
                </select>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
                <span className="muted" style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>Player votes:</span>
                <select
                  value={playerMinVotes}
                  onChange={(e) => setPlayerMinVotes(Number(e.target.value))}
                  style={{ width: "100%", padding: "0.4rem 0.5rem", fontSize: "0.78rem" }}
                >
                  <option value={1}>1+</option>
                  <option value={2}>2+</option>
                  <option value={3}>3+</option>
                  <option value={4}>4+</option>
                  <option value={5}>5+</option>
                </select>
              </div>
            </div>
            {mlFilter !== "all" && pickType === "game" && (
              <div className="muted" style={{ fontSize: "0.72rem" }}>
                ML vote applies to player-point picks. Switch Picks to Player or All.
              </div>
            )}
          </div>
        </>
      )}

      <div>
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
      {isBeyondLimit && (
        <div className="card" style={{ textAlign: "center", padding: "3rem 1.5rem" }}>
          <p style={{ fontSize: "1.1rem", fontWeight: 600, margin: "0 0 0.5rem" }}>Too far ahead</p>
          <p className="muted" style={{ margin: 0 }}>Predictions are only available up to {MAX_FUTURE_DAYS} days from today. Select a closer date to view games and picks.</p>
        </div>
      )}

      {!isBeyondLimit && !loading && data?.games.length === 0 && (
        <div className="card">
          <p>No games found for {date}{data?.autoSynced ? " (auto-sync was attempted)" : ""}. Try syncing upcoming games or selecting a different date.</p>
        </div>
      )}

      {!isBeyondLimit && !loading && data && filteredGames.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(380px, 1fr))", gap: "0.75rem", alignItems: "start" }}>
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
    </>
  );
}

export default function LandingPage() {
  const router = useRouter();
  const { data: session, status } = useSession();
  const [providers, setProviders] = useState<Record<string, { id: string; name: string }>>({});
  const [providersStatus, setProvidersStatus] = useState<"loading" | "ready" | "error">("loading");

  useEffect(() => {
    let mounted = true;
    const url = `${window.location.origin}/api/auth/providers`;
    fetch(url)
      .then(async (res) => {
        const data = res.ok ? ((await res.json()) as Record<string, { id: string; name: string }>) : null;
        if (!mounted) return;
        if (!res.ok) {
          setProvidersStatus("error");
          return;
        }
        setProviders(data && typeof data === "object" ? data : {});
        setProvidersStatus("ready");
      })
      .catch(() => {
        if (mounted) setProvidersStatus("error");
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

  const googleAvailable = !!providers.google;
  const appleAvailable = !!providers.apple;
  const devAvailable = !!providers["dev-login"];
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
            {providersStatus === "loading" ? (
              <p className="muted">Loading sign-in options…</p>
            ) : providersStatus === "error" ? (
              <p className="muted" style={{ margin: "0.25rem 0 0", fontSize: "0.82rem" }}>
                Could not reach the auth service. Confirm <code>NEXTAUTH_URL</code> matches this site,{" "}
                <code>NEXTAUTH_SECRET</code> is set on the server, and the deployment succeeded.
              </p>
            ) : (
              <>
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
                    No auth provider configured. In <code>apps/dashboard/.env</code> (or Vercel env), add{" "}
                    <code>GOOGLE_CLIENT_ID</code> / <code>GOOGLE_CLIENT_SECRET</code> (or Apple), or set{" "}
                    <code>DEV_AUTH_BYPASS=true</code>. On Vercel production, Dev Mode also requires{" "}
                    <code>DEV_AUTH_ALLOW_PRODUCTION=true</code> (not for public sites).
                  </p>
                ) : null}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
