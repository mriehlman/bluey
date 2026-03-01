param(
    [Parameter(Mandatory=$true)]
    [string]$Season,
    [int]$Concurrency = 4,
    [int]$MaxRestarts = 50
)

$restarts = 0
$startTime = Get-Date

while ($restarts -lt $MaxRestarts) {
    Write-Host "`n=== [$Season] Starting (attempt $($restarts + 1)/$MaxRestarts) ===" -ForegroundColor Cyan
    
    $process = Start-Process -FilePath "py" -ArgumentList "scripts/nba_fetch.py", "backfill-season", $Season, "--concurrency", $Concurrency -NoNewWindow -PassThru -Wait
    
    if ($process.ExitCode -eq 0) {
        Write-Host "=== [$Season] Completed successfully! ===" -ForegroundColor Green
        break
    } else {
        $restarts++
        Write-Host "=== [$Season] Exited with code $($process.ExitCode), restarting in 5s... ===" -ForegroundColor Yellow
        Start-Sleep -Seconds 5
    }
}

$elapsed = (Get-Date) - $startTime
Write-Host "`n=== [$Season] Done after $restarts restarts, elapsed: $($elapsed.ToString('hh\:mm\:ss')) ===" -ForegroundColor Magenta
