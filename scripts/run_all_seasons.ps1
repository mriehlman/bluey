# Run all seasons SEQUENTIALLY (one at a time) to avoid overwhelming the API
$seasons = @("2024-25", "2023-24", "2022-23", "2021-22", "2020-21", "2019-20")
$concurrency = 4
$maxRestartsPerSeason = 20

$startTime = Get-Date

foreach ($season in $seasons) {
    Write-Host "`n" -NoNewline
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host "  STARTING SEASON: $season" -ForegroundColor Cyan
    Write-Host ("=" * 60) -ForegroundColor Cyan
    
    $restarts = 0
    $seasonComplete = $false
    
    while (-not $seasonComplete -and $restarts -lt $maxRestartsPerSeason) {
        Write-Host "`n>>> [$season] Attempt $($restarts + 1)/$maxRestartsPerSeason <<<" -ForegroundColor Yellow
        
        $process = Start-Process -FilePath "py" -ArgumentList "scripts/nba_fetch.py", "backfill-season", $season, "--concurrency", $concurrency -NoNewWindow -PassThru -Wait
        
        if ($process.ExitCode -eq 0) {
            Write-Host ">>> [$season] Completed successfully! <<<" -ForegroundColor Green
            $seasonComplete = $true
        } else {
            $restarts++
            if ($restarts -lt $maxRestartsPerSeason) {
                Write-Host ">>> [$season] Failed (exit code $($process.ExitCode)), waiting 10s before retry... <<<" -ForegroundColor Red
                Start-Sleep -Seconds 10
            }
        }
    }
    
    if (-not $seasonComplete) {
        Write-Host ">>> [$season] Giving up after $maxRestartsPerSeason attempts <<<" -ForegroundColor Magenta
    }
}

$elapsed = (Get-Date) - $startTime
Write-Host "`n" -NoNewline
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  ALL SEASONS DONE! Total time: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
