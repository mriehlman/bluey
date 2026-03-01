# Run this script as Administrator to create the scheduled task

$taskName = "BlueyDailyOddsSync"
$scriptPath = "c:\Users\Michael Riehlman\Documents\Source\bluey\scripts\daily-sync.bat"

# Create action
$action = New-ScheduledTaskAction -Execute $scriptPath

# Create trigger - runs daily at 10 AM (before most NBA games)
$trigger = New-ScheduledTaskTrigger -Daily -At "10:00AM"

# Create settings
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

# Register the task
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description "Syncs NBA odds and player props daily"

Write-Host "Scheduled task '$taskName' created successfully!"
Write-Host "It will run daily at 10:00 AM"
Write-Host ""
Write-Host "To run it manually: schtasks /run /tn $taskName"
Write-Host "To delete it: Unregister-ScheduledTask -TaskName $taskName"
