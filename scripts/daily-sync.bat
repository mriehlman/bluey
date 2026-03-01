@echo off
cd /d "c:\Users\Michael Riehlman\Documents\Source\bluey"

echo [%date% %time%] Starting daily odds sync >> logs\sync.log

:: Sync game odds
call bun run sync:odds >> logs\sync.log 2>&1

:: Sync player props  
call bun run sync:player-props >> logs\sync.log 2>&1

echo [%date% %time%] Daily sync complete >> logs\sync.log
echo. >> logs\sync.log
