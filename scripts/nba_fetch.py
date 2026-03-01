#!/usr/bin/env python3
"""NBA data fetcher using nba_api. Outputs JSON to stdout or saves to files.

Captures ALL available data from the API for future-proofing.
Supports concurrent fetching with --concurrency flag.
"""

import sys
import json
import time
import os
import threading
import tempfile
import shutil
import signal
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global shutdown_requested
    if shutdown_requested:
        print("\n\nForce quit - exiting immediately", file=sys.stderr)
        os._exit(1)
    shutdown_requested = True
    print("\n\nShutdown requested - finishing current tasks... (Ctrl+C again to force quit)", file=sys.stderr)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

from nba_api.stats.endpoints import (
    scoreboardv3,
    boxscoretraditionalv3,
    leaguegamefinder,
)
from nba_api.stats.static import teams

# Rate limit: max requests per second (shared across all threads)
MAX_REQUESTS_PER_SECOND = 2.0
RATE_LIMIT_SECONDS = 1.0 / MAX_REQUESTS_PER_SECOND

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 3.0
RETRY_BACKOFF_MULTIPLIER = 2.0
RETRY_MAX_DELAY = 60.0  # Cap at 1 minute

import random

def retry_on_error(max_retries: int = MAX_RETRIES, delay: float = RETRY_DELAY_SECONDS):
    """
    Decorator that retries a function on exception with exponential backoff + jitter.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Add jitter (±30%) to prevent thundering herd
                        jitter = current_delay * random.uniform(-0.3, 0.3)
                        sleep_time = min(current_delay + jitter, RETRY_MAX_DELAY)
                        print(f"  Retry {attempt + 1}/{max_retries} in {sleep_time:.1f}s: {type(e).__name__}", file=sys.stderr)
                        time.sleep(sleep_time)
                        current_delay = min(current_delay * RETRY_BACKOFF_MULTIPLIER, RETRY_MAX_DELAY)
                    else:
                        raise last_exception

            raise last_exception
        return wrapper
    return decorator

# Thread-safe rate limiter
class RateLimiter:
    """Thread-safe rate limiter using token bucket algorithm."""
    def __init__(self, rate_per_second: float = MAX_REQUESTS_PER_SECOND):
        self.rate = rate_per_second
        self.lock = threading.Lock()
        self.last_time = time.time()
        self.tokens = rate_per_second  # Start with full bucket
        
    def acquire(self):
        """Block until a request can be made."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time
            self.last_time = now
            
            # Add tokens based on elapsed time
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            
            if self.tokens < 1:
                # Need to wait
                wait_time = (1 - self.tokens) / self.rate
                time.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

# Global rate limiter instance
rate_limiter = RateLimiter()


def ensure_data_dir(subdir: str = "") -> Path:
    """Ensure data/raw directory exists."""
    base = Path(__file__).parent.parent / "data" / "raw"
    if subdir:
        base = base / subdir
    base.mkdir(parents=True, exist_ok=True)
    return base


def save_json(data: dict | list, filepath: Path):
    """
    Save JSON to file atomically.
    Writes to a temp file first, then renames to prevent partial writes.
    """
    filepath = Path(filepath)
    
    # Write to temp file in same directory (for atomic rename)
    temp_fd, temp_path = tempfile.mkstemp(
        suffix=".tmp",
        prefix=filepath.stem + "_",
        dir=filepath.parent
    )
    
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        
        # Atomic rename (on same filesystem)
        shutil.move(temp_path, filepath)
        print(f"Saved: {filepath}", file=sys.stderr)
        
    except Exception as e:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except:
            pass
        raise e


def safe_value(val):
    """Convert numpy/pandas types to JSON-serializable Python types."""
    if val is None:
        return None
    if hasattr(val, 'item'):  # numpy types
        return val.item()
    if isinstance(val, float) and (val != val):  # NaN check
        return None
    return val


def row_to_dict(row):
    """Convert a pandas row to a dict with safe values."""
    return {k: safe_value(v) for k, v in row.to_dict().items()}


def fetch_games_for_date(date_str: str) -> list:
    """Fetch all games for a specific date (YYYY-MM-DD)."""
    sb = scoreboardv3.ScoreboardV3(game_date=date_str)
    dfs = sb.get_data_frames()
    
    # V3 structure: DF[1] = games, DF[2] = teams (2 rows per game: home and away)
    games_df = dfs[1]
    teams_df = dfs[2]
    
    result = []
    for _, game_row in games_df.iterrows():
        game_id = game_row["gameId"]
        status = game_row.get("gameStatusText", "")
        
        # Get teams for this game (should be 2 rows)
        game_teams = teams_df[teams_df["gameId"] == game_id]
        if len(game_teams) < 2:
            continue
        
        # First team is away, second is home in V3
        away_team = game_teams.iloc[0]
        home_team = game_teams.iloc[1]
        
        result.append({
            "gameId": game_id,
            "date": date_str,
            "homeTeamId": int(home_team["teamId"]),
            "awayTeamId": int(away_team["teamId"]),
            "homeScore": int(home_team.get("score", 0) or 0),
            "awayScore": int(away_team.get("score", 0) or 0),
            "status": status,
            "period": int(game_row.get("period", 4) or 4),
        })
    return result


def fetch_games_for_season(season: str) -> list:
    """Fetch all games for a season (e.g., '2024-25')."""
    result = []
    
    for team in teams.get_teams():
        time.sleep(RATE_LIMIT_SECONDS)
        finder = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=team["id"],
            season_nullable=season,
            season_type_nullable="Regular Season",
        )
        games_df = finder.get_data_frames()[0]
        
        for _, row in games_df.iterrows():
            game_id = row["GAME_ID"]
            if not any(g["gameId"] == game_id for g in result):
                matchup = row["MATCHUP"]
                is_home = " vs. " in matchup
                
                result.append({
                    "gameId": game_id,
                    "date": row["GAME_DATE"],
                    "teamId": int(row["TEAM_ID"]),
                    "isHome": is_home,
                    "points": int(row["PTS"] or 0),
                    "matchup": matchup,
                    "winLoss": row["WL"],
                })
        
        print(f"Fetched {team['abbreviation']}: {len(games_df)} games", file=sys.stderr)
    
    return result


@retry_on_error(max_retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
def fetch_full_game_data(game_id: str, game_meta: dict) -> dict:
    """
    Fetch ALL available data for a game - box scores plus team totals.
    Returns complete raw data for future-proofing.
    Retries automatically on connection errors.
    """
    box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    dfs = box.get_data_frames()
    
    # DF[0] = Player stats (25 rows typically)
    # DF[1] = Starters vs Bench breakdown (4 rows: 2 teams x 2 groups)
    # DF[2] = Team totals (2 rows: home and away)
    
    player_stats = []
    team_starters_bench = []
    team_totals = []
    
    # Player stats - capture EVERYTHING
    if len(dfs) > 0:
        for _, row in dfs[0].iterrows():
            player_stats.append(row_to_dict(row))
    
    # Starters/Bench breakdown
    if len(dfs) > 1:
        for _, row in dfs[1].iterrows():
            team_starters_bench.append(row_to_dict(row))
    
    # Team totals
    if len(dfs) > 2:
        for _, row in dfs[2].iterrows():
            team_totals.append(row_to_dict(row))
    
    return {
        "gameId": game_id,
        "playerStats": player_stats,
        "teamStartersBench": team_starters_bench,
        "teamTotals": team_totals,
        **game_meta,  # Include game metadata (date, teams, scores, etc.)
    }


@retry_on_error(max_retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
def fetch_scoreboard_full(date_str: str) -> dict:
    """
    Fetch complete scoreboard data for a date - ALL DataFrames.
    Retries automatically on connection errors.
    """
    sb = scoreboardv3.ScoreboardV3(game_date=date_str)
    dfs = sb.get_data_frames()
    
    # DF[0] = League info
    # DF[1] = Games (detailed)
    # DF[2] = Teams (wins/losses/score per team)
    # DF[3] = Game leaders
    # DF[4] = Season leaders
    # DF[5] = Broadcast info
    
    result = {
        "date": date_str,
        "leagueInfo": [row_to_dict(r) for _, r in dfs[0].iterrows()] if len(dfs) > 0 else [],
        "games": [row_to_dict(r) for _, r in dfs[1].iterrows()] if len(dfs) > 1 else [],
        "teams": [row_to_dict(r) for _, r in dfs[2].iterrows()] if len(dfs) > 2 else [],
        "gameLeaders": [row_to_dict(r) for _, r in dfs[3].iterrows()] if len(dfs) > 3 else [],
        "seasonLeaders": [row_to_dict(r) for _, r in dfs[4].iterrows()] if len(dfs) > 4 else [],
        "broadcasts": [row_to_dict(r) for _, r in dfs[5].iterrows()] if len(dfs) > 5 else [],
    }
    
    return result


@retry_on_error(max_retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
def fetch_box_score(game_id: str) -> dict:
    """Fetch box score for a specific game (legacy format for compatibility)."""
    box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    dfs = box.get_data_frames()
    
    player_stats = []
    if len(dfs) > 0:
        players_df = dfs[0]
        for _, row in players_df.iterrows():
            minutes_raw = row.get("minutes", "")
            
            # Parse minutes to numeric
            minutes_played = 0
            minutes_str = str(minutes_raw) if minutes_raw else ""
            
            # Handle ISO duration format PT##M##.##S
            if minutes_str.startswith("PT"):
                try:
                    mins = 0
                    secs = 0
                    part = minutes_str[2:]  # Remove PT
                    if "M" in part:
                        mins_str, rest = part.split("M")
                        mins = int(mins_str)
                        part = rest
                    if "S" in part:
                        secs_str = part.replace("S", "")
                        if secs_str:
                            secs = float(secs_str)
                    minutes_played = mins + int(secs / 60)
                except:
                    pass
            elif ":" in minutes_str:
                try:
                    parts = minutes_str.split(":")
                    minutes_played = int(parts[0])
                except:
                    pass
            
            player_stats.append({
                "playerId": int(row["personId"]),
                "firstName": row.get("firstName", ""),
                "familyName": row.get("familyName", ""),
                "teamId": int(row["teamId"]),
                "position": row.get("position", "") or "",
                "starter": False,  # V3 doesn't have starter field directly
                "comment": row.get("comment", "") or "",
                "jerseyNum": row.get("jerseyNum", "") or "",
                "minutes": minutes_str,
                "minutesPlayed": minutes_played,
                "points": int(row.get("points", 0) or 0),
                "rebounds": int(row.get("reboundsTotal", 0) or 0),
                "assists": int(row.get("assists", 0) or 0),
                "steals": int(row.get("steals", 0) or 0),
                "blocks": int(row.get("blocks", 0) or 0),
                "turnovers": int(row.get("turnovers", 0) or 0),
                "fgm": int(row.get("fieldGoalsMade", 0) or 0),
                "fga": int(row.get("fieldGoalsAttempted", 0) or 0),
                "fgPct": float(row.get("fieldGoalsPercentage", 0) or 0),
                "fg3m": int(row.get("threePointersMade", 0) or 0),
                "fg3a": int(row.get("threePointersAttempted", 0) or 0),
                "fg3Pct": float(row.get("threePointersPercentage", 0) or 0),
                "ftm": int(row.get("freeThrowsMade", 0) or 0),
                "fta": int(row.get("freeThrowsAttempted", 0) or 0),
                "ftPct": float(row.get("freeThrowsPercentage", 0) or 0),
                "oreb": int(row.get("reboundsOffensive", 0) or 0),
                "dreb": int(row.get("reboundsDefensive", 0) or 0),
                "pf": int(row.get("foulsPersonal", 0) or 0),
                "plusMinus": float(row.get("plusMinusPoints", 0) or 0),
            })
    
    return {"gameId": game_id, "playerStats": player_stats}


def fetch_box_scores_for_date(date_str: str, save_raw: bool = False) -> list:
    """Fetch box scores for all games on a date."""
    games = fetch_games_for_date(date_str)
    results = []
    
    if save_raw:
        data_dir = ensure_data_dir("boxscores")
    
    for i, game in enumerate(games):
        time.sleep(RATE_LIMIT_SECONDS)
        game_id = game["gameId"]
        print(f"Fetching box score {i+1}/{len(games)}: {game_id}", file=sys.stderr)
        
        try:
            box = fetch_box_score(game_id)
            box["homeTeamId"] = game["homeTeamId"]
            box["awayTeamId"] = game["awayTeamId"]
            box["homeScore"] = game["homeScore"]
            box["awayScore"] = game["awayScore"]
            box["date"] = date_str
            box["status"] = game.get("status", "")
            box["period"] = game.get("period", 4)
            results.append(box)
            
            if save_raw:
                filepath = data_dir / f"{game_id}.json"
                save_json(box, filepath)
                
        except Exception as e:
            print(f"Error fetching {game_id}: {e}", file=sys.stderr)
    
    return results


def process_single_game(game_info: dict, teams_data: list, scoreboard: dict, 
                        data_dir: Path, season: str, date_str: str) -> bool:
    """
    Process a single game - fetch box score and save to file.
    Returns True if game was processed, False if skipped or error.
    """
    game_id = game_info["gameId"]
    filepath = data_dir / f"{game_id}.json"
    
    # Skip if already saved
    if filepath.exists():
        return False
    
    # Build game metadata from scoreboard
    game_teams = [t for t in teams_data if t["gameId"] == game_id]
    home_team = None
    away_team = None
    
    # In scoreboard, first entry is usually away, second is home
    if len(game_teams) >= 2:
        away_team = game_teams[0]
        home_team = game_teams[1]
    
    game_meta = {
        "date": date_str,
        "season": season,
        "homeTeamId": int(home_team["teamId"]) if home_team else None,
        "awayTeamId": int(away_team["teamId"]) if away_team else None,
        "homeScore": int(home_team.get("score", 0) or 0) if home_team else 0,
        "awayScore": int(away_team.get("score", 0) or 0) if away_team else 0,
        "homeWins": int(home_team.get("wins", 0) or 0) if home_team else 0,
        "homeLosses": int(home_team.get("losses", 0) or 0) if home_team else 0,
        "awayWins": int(away_team.get("wins", 0) or 0) if away_team else 0,
        "awayLosses": int(away_team.get("losses", 0) or 0) if away_team else 0,
        "homeTricode": home_team.get("teamTricode", "") if home_team else "",
        "awayTricode": away_team.get("teamTricode", "") if away_team else "",
        "status": game_info.get("gameStatusText", ""),
        "gameStatus": game_info.get("gameStatus", 0),
        "period": int(game_info.get("period", 4) or 4),
        "regulationPeriods": int(game_info.get("regulationPeriods", 4) or 4),
        "gameCode": game_info.get("gameCode", ""),
        "gameTimeUTC": game_info.get("gameTimeUTC", ""),
        "gameEt": game_info.get("gameEt", ""),
        "isNeutral": game_info.get("isNeutral", False),
        "seriesText": game_info.get("seriesText", ""),
        "seriesGameNumber": game_info.get("seriesGameNumber", ""),
        "poRoundDesc": game_info.get("poRoundDesc", ""),
        "gameSubtype": game_info.get("gameSubtype", ""),
    }
    
    try:
        rate_limiter.acquire()
        full_game = fetch_full_game_data(game_id, game_meta)
        
        # Add broadcasts and leaders from scoreboard
        full_game["broadcasts"] = [b for b in scoreboard.get("broadcasts", []) if b.get("gameId") == game_id]
        full_game["gameLeaders"] = [l for l in scoreboard.get("gameLeaders", []) if l.get("gameId") == game_id]
        full_game["seasonLeaders"] = [l for l in scoreboard.get("seasonLeaders", []) if l.get("gameId") == game_id]
        
        save_json(full_game, filepath)
        return True
        
    except Exception as e:
        print(f"  Error fetching {game_id}: {e}", file=sys.stderr)
        return False


def load_skip_list(data_dir: Path) -> set:
    """Load list of game IDs to skip (persistently failing)."""
    skip_file = data_dir / "_skip_games.json"
    if skip_file.exists():
        try:
            with open(skip_file) as f:
                data = json.load(f)
                return set(data.get("games", []))
        except:
            pass
    return set()


def add_to_skip_list(data_dir: Path, game_id: str, reason: str):
    """Add a game to the skip list."""
    skip_file = data_dir / "_skip_games.json"
    skip_data = {"games": [], "reasons": {}}
    if skip_file.exists():
        try:
            with open(skip_file) as f:
                skip_data = json.load(f)
        except:
            pass
    
    if game_id not in skip_data["games"]:
        skip_data["games"].append(game_id)
        skip_data["reasons"][game_id] = reason
        with open(skip_file, "w") as f:
            json.dump(skip_data, f, indent=2)
        print(f"  Added {game_id} to skip list: {reason}", file=sys.stderr)


def process_single_date(date_str: str, season: str, data_dir: Path,
                        game_concurrency: int = 3) -> tuple[int, int, bool]:
    """
    Process all games for a single date.
    Returns (games_processed, games_failed, all_complete).
    all_complete is True only if ALL games were successfully saved.
    """
    # Check for shutdown before starting
    if shutdown_requested:
        return (0, 0, False)
    
    try:
        # Check if we already have scoreboard data locally
        scoreboard_file = data_dir / f"_scoreboard_{date_str}.json"
        if scoreboard_file.exists():
            # Use local data - no API call needed!
            with open(scoreboard_file) as f:
                scoreboard = json.load(f)
            games = scoreboard.get("games", [])
            teams_data = scoreboard.get("teams", {})
            if not games:
                return (0, 0, True)  # No games, already complete
            print(f"[{date_str}] Using cached scoreboard ({len(games)} games)", file=sys.stderr)
        else:
            # Fetch from API
            rate_limiter.acquire()
            scoreboard = fetch_scoreboard_full(date_str)
            games = scoreboard["games"]
            teams_data = scoreboard["teams"]

            if not games:
                print(f"No games on {date_str}", file=sys.stderr)
                return (0, 0, True)

            print(f"[{date_str}] Found {len(games)} games", file=sys.stderr)
            save_json(scoreboard, scoreboard_file)

        # Load skip list for persistently failing games
        skip_list = load_skip_list(data_dir)
        
        # Count games that already exist vs need fetching
        games_to_fetch = []
        games_already_exist = 0
        games_skipped = 0
        for game_info in games:
            game_id = game_info["gameId"]
            filepath = data_dir / f"{game_id}.json"
            if filepath.exists():
                games_already_exist += 1
            elif game_id in skip_list:
                games_skipped += 1
            else:
                games_to_fetch.append(game_info)

        if not games_to_fetch:
            status = f"All {games_already_exist} games already exist"
            if games_skipped:
                status += f" ({games_skipped} skipped)"
            print(f"  [{date_str}] {status}", file=sys.stderr)
            return (0, 0, True)

        # Load/update failure tracker
        fail_tracker_file = data_dir / "_fail_tracker.json"
        fail_tracker = {}
        if fail_tracker_file.exists():
            try:
                with open(fail_tracker_file) as f:
                    fail_tracker = json.load(f)
            except:
                pass
        
        # Process games concurrently
        games_processed = 0
        games_failed = 0
        failed_game_ids = []
        with ThreadPoolExecutor(max_workers=game_concurrency) as executor:
            futures = {
                executor.submit(
                    process_single_game, game_info, teams_data,
                    scoreboard, data_dir, season, date_str
                ): game_info["gameId"]
                for game_info in games_to_fetch
            }

            for future in as_completed(futures):
                game_id = futures[future]
                try:
                    if future.result():
                        games_processed += 1
                        print(f"  [{date_str}] Saved {game_id}", file=sys.stderr)
                        # Clear from fail tracker on success
                        if game_id in fail_tracker:
                            del fail_tracker[game_id]
                    else:
                        games_failed += 1
                        failed_game_ids.append(game_id)
                except Exception as e:
                    games_failed += 1
                    failed_game_ids.append(game_id)
                    print(f"  [{date_str}] Error {game_id}: {e}", file=sys.stderr)
        
        # Update failure tracker and add to skip list if too many failures
        for game_id in failed_game_ids:
            fail_tracker[game_id] = fail_tracker.get(game_id, 0) + 1
            if fail_tracker[game_id] >= 3:
                add_to_skip_list(data_dir, game_id, f"Failed {fail_tracker[game_id]} times")
                games_skipped += 1
                games_failed -= 1  # Don't count as failed if skipped
        
        # Save fail tracker
        with open(fail_tracker_file, "w") as f:
            json.dump(fail_tracker, f, indent=2)

        # Consider date complete if:
        # - No new failures, OR
        # - All failures are now in skip list
        total_accounted = games_already_exist + games_processed + games_skipped
        expected = len(games)
        
        all_complete = (games_failed == 0) or (total_accounted >= expected)

        if not all_complete:
            print(f"  [{date_str}] Incomplete: {total_accounted}/{expected} games ({games_failed} failed)", file=sys.stderr)

        return (games_processed, games_failed, all_complete)

    except Exception as e:
        print(f"Error on {date_str}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return (0, 1, False)


def backfill_season(season: str, start_date: str = None, end_date: str = None, 
                    concurrency: int = 5, game_concurrency: int = 3):
    """
    Backfill an entire season with CONCURRENT fetching.
    Captures ALL available data from the API.
    
    Season format: "2024-25" means Oct 2024 - Apr 2025
    
    Args:
        season: Season string like "2024-25"
        start_date: Optional start date override
        end_date: Optional end date override
        concurrency: Number of concurrent date workers (default 5)
        game_concurrency: Number of concurrent game workers per date (default 3)
    """
    # Parse season to date range
    # NBA regular season typically: late Oct to mid-April
    # Playoffs: April to June
    start_year = int(season.split("-")[0])
    
    if not start_date:
        start_date = f"{start_year}-10-01"
    if not end_date:
        end_date = f"{start_year + 1}-06-30"
    
    # Generate all dates
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    all_dates = []
    while current <= end:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    # Also check: for dates where we have scoreboard files, mark as processed
    # This handles the case where games exist but date wasn't marked complete
    data_dir_check = ensure_data_dir(f"seasons/{season}")
    existing_scoreboards = set()
    for f in data_dir_check.glob("_scoreboard_*.json"):
        date_str = f.name.replace("_scoreboard_", "").replace(".json", "")
        existing_scoreboards.add(date_str)
    
    # Create season directory
    data_dir = ensure_data_dir(f"seasons/{season}")
    
    # Check for existing progress
    progress_file = data_dir / "_progress.json"
    processed_dates = set()
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
            processed_dates = set(progress.get("dates", []))
            print(f"Resuming from {len(processed_dates)} already processed dates", file=sys.stderr)

    # Also consider dates with existing scoreboards as "have data" 
    # (we still process them to fill missing games, but we know about them)
    dates_with_data = processed_dates | existing_scoreboards
    if len(existing_scoreboards - processed_dates) > 0:
        print(f"Found {len(existing_scoreboards - processed_dates)} dates with scoreboards not in progress", file=sys.stderr)

    # Filter to dates that need processing
    # Skip dates that are in progress OR have scoreboards (we'll handle missing games separately)
    dates_to_process = [d for d in all_dates if d not in dates_with_data]
    print(f"Processing {len(dates_to_process)} new dates with {concurrency} concurrent workers", file=sys.stderr)
    
    # Also queue dates with scoreboards but incomplete games
    dates_needing_games = [d for d in all_dates if d in existing_scoreboards and d not in processed_dates]
    if dates_needing_games:
        print(f"Also checking {len(dates_needing_games)} dates with incomplete games", file=sys.stderr)
        dates_to_process = dates_needing_games + dates_to_process  # Prioritize incomplete dates
    
    if not dates_to_process:
        print("All dates already processed!", file=sys.stderr)
        return {"datesProcessed": 0, "gamesProcessed": 0}
    
    # Adjust rate limiter for concurrency
    global rate_limiter
    # Allow more requests per second with concurrent workers, but stay conservative
    effective_rate = min(concurrency * 1.5, 10.0)  # Max 10 req/sec
    rate_limiter = RateLimiter(effective_rate)
    print(f"Rate limit: {effective_rate:.1f} requests/second", file=sys.stderr)
    
    # Thread-safe counters
    dates_processed = 0
    games_processed = 0
    counter_lock = threading.Lock()
    progress_lock = threading.Lock()
    
    def process_and_track(date_str: str) -> tuple[int, int, bool]:
        nonlocal dates_processed, games_processed

        games, failed, all_complete = process_single_date(date_str, season, data_dir, game_concurrency)

        with counter_lock:
            games_processed += games
            if all_complete:
                dates_processed += 1

        # Only mark date as processed if ALL games were saved
        if all_complete:
            with progress_lock:
                processed_dates.add(date_str)
                with open(progress_file, "w") as f:
                    json.dump({
                        "season": season,
                        "dates": sorted(list(processed_dates)),
                        "lastDate": date_str,
                        "gamesProcessed": games_processed,
                    }, f, indent=2)

        return (games, failed, all_complete)
    
    # Process dates concurrently
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_and_track, d): d for d in dates_to_process}
        
        completed = 0
        total_failed = 0
        for future in as_completed(futures):
            # Check for shutdown request
            if shutdown_requested:
                print("\nShutdown requested - cancelling remaining tasks...", file=sys.stderr)
                for f in futures:
                    f.cancel()
                break
            
            date_str = futures[future]
            completed += 1
            try:
                games, failed, all_complete = future.result()
                total_failed += failed
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = len(dates_to_process) - completed
                eta = remaining / rate if rate > 0 else 0
                status = "✓" if all_complete else f"partial ({failed} failed)"
                print(f"Progress: {completed}/{len(dates_to_process)} dates "
                      f"({rate:.1f} dates/sec, ETA: {eta/60:.1f} min) [{status}]", file=sys.stderr)
            except Exception as e:
                total_failed += 1
                print(f"Error processing {date_str}: {e}", file=sys.stderr)
    
    elapsed = time.time() - start_time
    print(f"\n=== Complete ===", file=sys.stderr)
    print(f"Processed {dates_processed} complete dates, {games_processed} games in {elapsed:.1f}s", file=sys.stderr)
    if total_failed > 0:
        print(f"Warning: {total_failed} games failed - run again to retry", file=sys.stderr)
        print(f"Tip: Use 'py scripts/verify_data.py audit {season}' to check data integrity", file=sys.stderr)
    
    return {
        "datesProcessed": dates_processed, 
        "gamesProcessed": games_processed,
        "gamesFailed": total_failed,
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: nba_fetch.py <command> [args]", file=sys.stderr)
        print("Commands:", file=sys.stderr)
        print("  games-date <YYYY-MM-DD>         Fetch games for a date", file=sys.stderr)
        print("  games-season <YYYY-YY>          Fetch games for a season", file=sys.stderr)
        print("  box-score <GAME_ID>             Fetch box score for a game", file=sys.stderr)
        print("  box-scores-date <YYYY-MM-DD>    Fetch all box scores for a date", file=sys.stderr)
        print("  scoreboard <YYYY-MM-DD>         Fetch full scoreboard for a date", file=sys.stderr)
        print("  backfill-season <YYYY-YY> [options]  Backfill season to local JSON", file=sys.stderr)
        print("", file=sys.stderr)
        print("backfill-season options:", file=sys.stderr)
        print("  --concurrency N      Number of concurrent date workers (default: 5)", file=sys.stderr)
        print("  --game-concurrency N Number of concurrent game workers per date (default: 3)", file=sys.stderr)
        print("  --start YYYY-MM-DD   Start date (default: Oct 1 of season start year)", file=sys.stderr)
        print("  --end YYYY-MM-DD     End date (default: Jun 30 of season end year)", file=sys.stderr)
        print("", file=sys.stderr)
        print("Examples:", file=sys.stderr)
        print("  python nba_fetch.py backfill-season 2024-25", file=sys.stderr)
        print("  python nba_fetch.py backfill-season 2023-24 --concurrency 10", file=sys.stderr)
        print("  python nba_fetch.py backfill-season 2022-23 --concurrency 8 --game-concurrency 4", file=sys.stderr)
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "games-date":
            date_str = sys.argv[2]
            result = fetch_games_for_date(date_str)
            print(json.dumps(result))
            
        elif command == "games-season":
            season = sys.argv[2]
            result = fetch_games_for_season(season)
            print(json.dumps(result))
            
        elif command == "box-score":
            game_id = sys.argv[2]
            result = fetch_box_score(game_id)
            print(json.dumps(result))
            
        elif command == "box-scores-date":
            date_str = sys.argv[2]
            save_raw = "--save" in sys.argv
            result = fetch_box_scores_for_date(date_str, save_raw=save_raw)
            print(json.dumps(result))
            
        elif command == "scoreboard":
            date_str = sys.argv[2]
            result = fetch_scoreboard_full(date_str)
            print(json.dumps(result, indent=2))
            
        elif command == "backfill-season":
            season = sys.argv[2]
            
            # Parse optional arguments
            start_date = None
            end_date = None
            concurrency = 5
            game_concurrency = 3
            
            i = 3
            while i < len(sys.argv):
                arg = sys.argv[i]
                if arg == "--concurrency" and i + 1 < len(sys.argv):
                    concurrency = int(sys.argv[i + 1])
                    i += 2
                elif arg == "--game-concurrency" and i + 1 < len(sys.argv):
                    game_concurrency = int(sys.argv[i + 1])
                    i += 2
                elif arg == "--start" and i + 1 < len(sys.argv):
                    start_date = sys.argv[i + 1]
                    i += 2
                elif arg == "--end" and i + 1 < len(sys.argv):
                    end_date = sys.argv[i + 1]
                    i += 2
                elif not start_date and "-" in arg and len(arg) == 10:
                    # Legacy positional: start_date
                    start_date = arg
                    i += 1
                elif start_date and not end_date and "-" in arg and len(arg) == 10:
                    # Legacy positional: end_date
                    end_date = arg
                    i += 1
                else:
                    i += 1
            
            result = backfill_season(season, start_date, end_date, concurrency, game_concurrency)
            print(json.dumps(result))
            
        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
