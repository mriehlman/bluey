#!/usr/bin/env python3
"""Data verification and integrity checker for NBA season data.

Audits the raw data directory to find:
- Corrupted JSON files
- Missing games (dates with incomplete data)
- Progress file mismatches
- Duplicate or orphan files

Usage:
  python verify_data.py audit 2024-25          # Audit a season
  python verify_data.py audit-all              # Audit all seasons
  python verify_data.py reconcile 2024-25      # Fix progress file to match reality
  python verify_data.py validate 2024-25       # Validate all JSON files
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict


def get_data_dir() -> Path:
    """Get the data/raw directory."""
    return Path(__file__).parent.parent / "data" / "raw"


def load_json_safe(filepath: Path) -> tuple[dict | list | None, str | None]:
    """Load JSON file, return (data, error)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data, None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"
    except Exception as e:
        return None, f"Read error: {e}"


def validate_game_file(filepath: Path) -> tuple[bool, list[str]]:
    """
    Validate a game JSON file has required fields.
    Returns (is_valid, list of issues).
    """
    issues = []
    data, error = load_json_safe(filepath)
    
    if error:
        return False, [error]
    
    if not isinstance(data, dict):
        return False, ["Not a dict"]
    
    required_fields = ["gameId", "date", "playerStats"]
    for field in required_fields:
        if field not in data:
            issues.append(f"Missing field: {field}")
    
    if "playerStats" in data:
        if not isinstance(data["playerStats"], list):
            issues.append("playerStats is not a list")
        elif len(data["playerStats"]) == 0:
            issues.append("playerStats is empty (may be valid for cancelled games)")
    
    return len(issues) == 0, issues


def validate_scoreboard_file(filepath: Path) -> tuple[bool, list[str]]:
    """Validate a scoreboard JSON file."""
    issues = []
    data, error = load_json_safe(filepath)
    
    if error:
        return False, [error]
    
    if not isinstance(data, dict):
        return False, ["Not a dict"]
    
    required_fields = ["date", "games"]
    for field in required_fields:
        if field not in data:
            issues.append(f"Missing field: {field}")
    
    return len(issues) == 0, issues


def audit_season(season: str, verbose: bool = True) -> dict:
    """
    Audit a season's data for completeness and integrity.
    
    Returns a report dict with:
    - dates_in_progress: dates marked as processed
    - dates_with_scoreboards: dates with scoreboard files
    - games_expected: games we should have (from scoreboards)
    - games_found: game files that exist
    - games_missing: games we should have but don't
    - games_orphan: game files without scoreboard reference
    - corrupted_files: files that failed validation
    - issues: list of all issues found
    """
    data_dir = get_data_dir() / "seasons" / season
    
    if not data_dir.exists():
        return {"error": f"Season directory not found: {data_dir}"}
    
    report = {
        "season": season,
        "dates_in_progress": [],
        "dates_with_scoreboards": [],
        "games_expected": [],
        "games_found": [],
        "games_missing": [],
        "games_orphan": [],
        "corrupted_files": [],
        "issues": [],
    }
    
    # Load progress file
    progress_file = data_dir / "_progress.json"
    if progress_file.exists():
        progress, error = load_json_safe(progress_file)
        if error:
            report["issues"].append(f"Progress file corrupted: {error}")
        else:
            report["dates_in_progress"] = progress.get("dates", [])
    
    # Scan all files
    scoreboard_files = {}
    game_files = {}
    
    for filepath in data_dir.glob("*.json"):
        filename = filepath.name
        
        if filename == "_progress.json":
            continue
        
        if filename.startswith("_scoreboard_"):
            # Parse date from filename: _scoreboard_YYYY-MM-DD.json
            date_str = filename.replace("_scoreboard_", "").replace(".json", "")
            is_valid, issues = validate_scoreboard_file(filepath)
            
            if not is_valid:
                report["corrupted_files"].append({
                    "file": str(filepath),
                    "issues": issues,
                })
            else:
                data, _ = load_json_safe(filepath)
                scoreboard_files[date_str] = data
                report["dates_with_scoreboards"].append(date_str)
        else:
            # Game file: {game_id}.json
            game_id = filename.replace(".json", "")
            is_valid, issues = validate_game_file(filepath)
            
            if not is_valid:
                report["corrupted_files"].append({
                    "file": str(filepath),
                    "issues": issues,
                })
            else:
                data, _ = load_json_safe(filepath)
                game_files[game_id] = data
                report["games_found"].append(game_id)
    
    # Cross-reference: find expected games from scoreboards
    for date_str, scoreboard in scoreboard_files.items():
        games = scoreboard.get("games", [])
        for game in games:
            game_id = game.get("gameId")
            if game_id:
                report["games_expected"].append(game_id)
                
                if game_id not in game_files:
                    report["games_missing"].append({
                        "gameId": game_id,
                        "date": date_str,
                    })
    
    # Find orphan games (not referenced in any scoreboard)
    expected_set = set(report["games_expected"])
    for game_id in report["games_found"]:
        if game_id not in expected_set:
            report["games_orphan"].append(game_id)
    
    # Check progress vs reality
    dates_with_data = set(report["dates_with_scoreboards"])
    dates_in_progress = set(report["dates_in_progress"])
    
    # Dates in progress but no scoreboard
    for date_str in dates_in_progress - dates_with_data:
        report["issues"].append(f"Date {date_str} in progress but no scoreboard file")
    
    # Dates with scoreboard but not in progress
    for date_str in dates_with_data - dates_in_progress:
        report["issues"].append(f"Date {date_str} has scoreboard but not in progress file")
    
    # Summary
    report["summary"] = {
        "total_dates_processed": len(report["dates_in_progress"]),
        "total_scoreboards": len(report["dates_with_scoreboards"]),
        "total_games_expected": len(report["games_expected"]),
        "total_games_found": len(report["games_found"]),
        "total_games_missing": len(report["games_missing"]),
        "total_games_orphan": len(report["games_orphan"]),
        "total_corrupted": len(report["corrupted_files"]),
        "total_issues": len(report["issues"]),
    }
    
    if verbose:
        print(f"\n=== Audit Report: {season} ===", file=sys.stderr)
        print(f"Dates processed:  {report['summary']['total_dates_processed']}", file=sys.stderr)
        print(f"Scoreboard files: {report['summary']['total_scoreboards']}", file=sys.stderr)
        print(f"Games expected:   {report['summary']['total_games_expected']}", file=sys.stderr)
        print(f"Games found:      {report['summary']['total_games_found']}", file=sys.stderr)
        print(f"Games missing:    {report['summary']['total_games_missing']}", file=sys.stderr)
        print(f"Orphan games:     {report['summary']['total_games_orphan']}", file=sys.stderr)
        print(f"Corrupted files:  {report['summary']['total_corrupted']}", file=sys.stderr)
        print(f"Issues:           {report['summary']['total_issues']}", file=sys.stderr)
        
        if report["corrupted_files"]:
            print(f"\nCorrupted files:", file=sys.stderr)
            for item in report["corrupted_files"][:10]:
                print(f"  {item['file']}: {item['issues']}", file=sys.stderr)
            if len(report["corrupted_files"]) > 10:
                print(f"  ... and {len(report['corrupted_files']) - 10} more", file=sys.stderr)
        
        if report["games_missing"]:
            print(f"\nMissing games:", file=sys.stderr)
            for item in report["games_missing"][:10]:
                print(f"  {item['date']}: {item['gameId']}", file=sys.stderr)
            if len(report["games_missing"]) > 10:
                print(f"  ... and {len(report['games_missing']) - 10} more", file=sys.stderr)
        
        if report["issues"]:
            print(f"\nIssues:", file=sys.stderr)
            for issue in report["issues"][:10]:
                print(f"  {issue}", file=sys.stderr)
            if len(report["issues"]) > 10:
                print(f"  ... and {len(report['issues']) - 10} more", file=sys.stderr)
    
    return report


def reconcile_progress(season: str, dry_run: bool = True) -> dict:
    """
    Reconcile progress file to match actual data on disk.
    
    This ensures the progress file accurately reflects what we have,
    so re-runs will correctly skip already-fetched data.
    """
    data_dir = get_data_dir() / "seasons" / season
    
    if not data_dir.exists():
        return {"error": f"Season directory not found: {data_dir}"}
    
    # Find all dates with complete data
    # A date is "complete" if:
    # 1. We have a scoreboard file for it
    # 2. We have all game files referenced in the scoreboard
    
    complete_dates = []
    incomplete_dates = []
    games_count = 0
    
    for filepath in sorted(data_dir.glob("_scoreboard_*.json")):
        date_str = filepath.name.replace("_scoreboard_", "").replace(".json", "")
        
        scoreboard, error = load_json_safe(filepath)
        if error:
            incomplete_dates.append({"date": date_str, "reason": "corrupted scoreboard"})
            continue
        
        games = scoreboard.get("games", [])
        missing = []
        
        for game in games:
            game_id = game.get("gameId")
            if game_id:
                game_file = data_dir / f"{game_id}.json"
                if not game_file.exists():
                    missing.append(game_id)
        
        if missing:
            incomplete_dates.append({
                "date": date_str,
                "reason": f"missing {len(missing)} games",
                "missing": missing,
            })
        else:
            complete_dates.append(date_str)
            games_count += len(games)
    
    print(f"\n=== Reconciliation: {season} ===", file=sys.stderr)
    print(f"Complete dates: {len(complete_dates)}", file=sys.stderr)
    print(f"Incomplete dates: {len(incomplete_dates)}", file=sys.stderr)
    print(f"Total games: {games_count}", file=sys.stderr)
    
    if incomplete_dates:
        print(f"\nIncomplete dates (will be re-fetched on next run):", file=sys.stderr)
        for item in incomplete_dates[:10]:
            print(f"  {item['date']}: {item['reason']}", file=sys.stderr)
        if len(incomplete_dates) > 10:
            print(f"  ... and {len(incomplete_dates) - 10} more", file=sys.stderr)
    
    # Build new progress file
    new_progress = {
        "season": season,
        "dates": sorted(complete_dates),
        "lastDate": complete_dates[-1] if complete_dates else None,
        "gamesProcessed": games_count,
        "reconciledAt": datetime.now().isoformat(),
    }
    
    progress_file = data_dir / "_progress.json"
    
    if dry_run:
        print(f"\nDry run - would write to {progress_file}", file=sys.stderr)
        print(f"New progress would have {len(complete_dates)} dates", file=sys.stderr)
    else:
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(new_progress, f, indent=2)
        print(f"\nWrote reconciled progress to {progress_file}", file=sys.stderr)
    
    return {
        "complete_dates": len(complete_dates),
        "incomplete_dates": len(incomplete_dates),
        "games_count": games_count,
        "incomplete_details": incomplete_dates,
    }


def audit_all_seasons(verbose: bool = True) -> dict:
    """Audit all seasons."""
    seasons_dir = get_data_dir() / "seasons"
    
    if not seasons_dir.exists():
        return {"error": "No seasons directory found"}
    
    reports = {}
    for season_dir in sorted(seasons_dir.iterdir()):
        if season_dir.is_dir():
            season = season_dir.name
            reports[season] = audit_season(season, verbose=verbose)
    
    return reports


def validate_all_json(season: str) -> dict:
    """Validate all JSON files in a season directory."""
    data_dir = get_data_dir() / "seasons" / season
    
    if not data_dir.exists():
        return {"error": f"Season directory not found: {data_dir}"}
    
    results = {
        "valid": 0,
        "invalid": 0,
        "errors": [],
    }
    
    for filepath in data_dir.glob("*.json"):
        data, error = load_json_safe(filepath)
        
        if error:
            results["invalid"] += 1
            results["errors"].append({
                "file": str(filepath),
                "error": error,
            })
        else:
            results["valid"] += 1
    
    print(f"\n=== JSON Validation: {season} ===", file=sys.stderr)
    print(f"Valid files:   {results['valid']}", file=sys.stderr)
    print(f"Invalid files: {results['invalid']}", file=sys.stderr)
    
    if results["errors"]:
        print(f"\nInvalid files:", file=sys.stderr)
        for item in results["errors"]:
            print(f"  {item['file']}: {item['error']}", file=sys.stderr)
    
    return results


def delete_corrupted(season: str, dry_run: bool = True) -> dict:
    """
    Find and delete corrupted JSON files so they can be re-fetched.
    
    A file is considered corrupted if:
    - It fails to parse as JSON
    - It's a game file missing required fields (gameId, date, playerStats)
    - It's a scoreboard file missing required fields (date, games)
    """
    data_dir = get_data_dir() / "seasons" / season
    
    if not data_dir.exists():
        return {"error": f"Season directory not found: {data_dir}"}
    
    corrupted = []
    
    for filepath in data_dir.glob("*.json"):
        filename = filepath.name
        
        if filename == "_progress.json":
            continue
        
        if filename.startswith("_scoreboard_"):
            is_valid, issues = validate_scoreboard_file(filepath)
        else:
            is_valid, issues = validate_game_file(filepath)
        
        if not is_valid:
            corrupted.append({
                "file": str(filepath),
                "path": filepath,
                "issues": issues,
            })
    
    print(f"\n=== Delete Corrupted: {season} ===", file=sys.stderr)
    print(f"Corrupted files found: {len(corrupted)}", file=sys.stderr)
    
    deleted = 0
    failed = 0
    
    if corrupted:
        if dry_run:
            print(f"\nDry run - would delete:", file=sys.stderr)
            for item in corrupted[:20]:
                print(f"  {item['file']}", file=sys.stderr)
            if len(corrupted) > 20:
                print(f"  ... and {len(corrupted) - 20} more", file=sys.stderr)
        else:
            print(f"\nDeleting corrupted files...", file=sys.stderr)
            for item in corrupted:
                try:
                    item["path"].unlink()
                    deleted += 1
                except Exception as e:
                    print(f"  Failed to delete {item['file']}: {e}", file=sys.stderr)
                    failed += 1
            print(f"Deleted: {deleted}, Failed: {failed}", file=sys.stderr)
    
    return {
        "season": season,
        "corrupted_count": len(corrupted),
        "deleted": deleted if not dry_run else 0,
        "failed": failed if not dry_run else 0,
        "dry_run": dry_run,
        "files": [item["file"] for item in corrupted],
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: verify_data.py <command> [args]", file=sys.stderr)
        print("Commands:", file=sys.stderr)
        print("  audit <season>       Audit a season's data integrity", file=sys.stderr)
        print("  audit-all            Audit all seasons", file=sys.stderr)
        print("  reconcile <season>   Fix progress file to match reality (dry run)", file=sys.stderr)
        print("  reconcile <season> --apply   Actually write the fixed progress", file=sys.stderr)
        print("  validate <season>    Validate all JSON files", file=sys.stderr)
        print("  delete-corrupted <season>          Find corrupted files (dry run)", file=sys.stderr)
        print("  delete-corrupted <season> --apply  Delete corrupted files", file=sys.stderr)
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "audit":
            season = sys.argv[2]
            report = audit_season(season)
            print(json.dumps(report, indent=2))
            
        elif command == "audit-all":
            reports = audit_all_seasons()
            print(json.dumps(reports, indent=2))
            
        elif command == "reconcile":
            season = sys.argv[2]
            dry_run = "--apply" not in sys.argv
            result = reconcile_progress(season, dry_run=dry_run)
            print(json.dumps(result, indent=2))
            
        elif command == "validate":
            season = sys.argv[2]
            result = validate_all_json(season)
            print(json.dumps(result, indent=2))
            
        elif command == "delete-corrupted":
            season = sys.argv[2]
            dry_run = "--apply" not in sys.argv
            result = delete_corrupted(season, dry_run=dry_run)
            print(json.dumps(result, indent=2))
            
        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
