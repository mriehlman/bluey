#!/usr/bin/env python3
"""Fetch NBA injury report snapshots via nbainjuries.

Usage:
  python scripts/nba_injuries_fetch.py report-date YYYY-MM-DD [HH:MM]
"""

import json
import sys
from datetime import datetime
import io
import contextlib


def usage() -> None:
    print("Usage: nba_injuries_fetch.py report-date YYYY-MM-DD [HH:MM]", file=sys.stderr)


def parse_datetime(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")


def main() -> None:
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)

    command = sys.argv[1]
    if command != "report-date":
        print(f"Unknown command: {command}", file=sys.stderr)
        usage()
        sys.exit(1)

    date_str = sys.argv[2]
    time_str = sys.argv[3] if len(sys.argv) >= 4 else "17:30"

    try:
        target_dt = parse_datetime(date_str, time_str)
    except ValueError:
        print(f"Invalid date/time: {date_str} {time_str}", file=sys.stderr)
        sys.exit(1)

    try:
        from nbainjuries import injury  # type: ignore
    except Exception as exc:
        print(
            "Failed to import nbainjuries. Install with: pip install nbainjuries "
            "(requires Python 3.10+ and Java in PATH).",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        sys.exit(2)

    try:
        # nbainjuries can print validation messages to stdout; suppress that noise
        # so this bridge always emits clean JSON for Node parsing.
        with contextlib.redirect_stdout(io.StringIO()):
            payload = injury.get_reportdata(target_dt)
        if isinstance(payload, str):
            payload = json.loads(payload)
        print(json.dumps(payload))
    except Exception as exc:
        print(f"Error fetching injury report for {date_str} {time_str}: {exc}", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
