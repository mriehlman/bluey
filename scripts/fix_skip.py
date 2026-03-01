import json
import sys

season = sys.argv[1] if len(sys.argv) > 1 else "2025-26"
base = f"data/raw/seasons/{season}"

ft_file = f"{base}/_fail_tracker.json"
skip_file = f"{base}/_skip_games.json"

try:
    ft = json.load(open(ft_file))
except:
    ft = {}

try:
    skip = json.load(open(skip_file))
except:
    skip = {"games": [], "reasons": {}}

added = 0
for game_id, count in ft.items():
    if game_id not in skip["games"]:
        skip["games"].append(game_id)
        skip["reasons"][game_id] = f"Failed {count} times - likely no box score data"
        added += 1

json.dump(skip, open(skip_file, "w"), indent=2)
print(f"Added {added} games to skip list")
print(f"Total skipped: {len(skip['games'])}")
print(f"Games: {skip['games']}")
