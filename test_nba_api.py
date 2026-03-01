from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2

print("=== Testing Python nba_api ===")
print()

print("1. Fetching scoreboard for 2025-02-25...")
sb = scoreboardv2.ScoreboardV2(game_date="2025-02-25")
games_df = sb.get_data_frames()[0]
print(f"   Games found: {len(games_df)}")

if len(games_df) > 0:
    game_id = games_df.iloc[0]["GAME_ID"]
    print(f"   First game ID: {game_id}")
    
    print()
    print(f"2. Fetching box score for game {game_id}...")
    box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    player_stats = box.get_data_frames()[0]
    print(f"   Player stats found: {len(player_stats)}")
    
    if len(player_stats) > 0:
        p = player_stats.iloc[0]
        print(f"   Sample: {p['PLAYER_NAME']} - PTS={p['PTS']}, REB={p['REB']}, AST={p['AST']}")
        print(f"   Shooting: FGM/FGA={p['FGM']}/{p['FGA']}, 3PM/3PA={p['FG3M']}/{p['FG3A']}")
        print(f"   Rebounds: OREB={p['OREB']}, DREB={p['DREB']}")

print()
print("=== API test successful! ===")
