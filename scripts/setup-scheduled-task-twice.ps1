# Run this script as Administrator to create scheduled tasks for twice-daily sync

$taskNameAM = "BlueyOddsSyncMorning"
$taskNamePM = "BlueyOddsSyncEvening"
$scriptPath = "c:\Users\Michael Riehlman\Documents\Source\bluey\scripts\daily-sync.bat"

# Morning sync at 10 AM
$actionAM = New-ScheduledTaskAction -Execute $scriptPath
$triggerAM = New-ScheduledTaskTrigger -Daily -At "10:00AM"
$settingsAM = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd
Register-ScheduledTask -TaskName $taskNameAM -Action $actionAM -Trigger $triggerAM -Settings $settingsAM -Description "Morning NBA odds sync"

# Evening sync at 5 PM (before night games)
$actionPM = New-ScheduledTaskAction -Execute $scriptPath
$triggerPM = New-ScheduledTaskTrigger -Daily -At "5:00PM"
$settingsPM = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd
Register-ScheduledTask -TaskName $taskNamePM -Action $actionPM -Trigger $triggerPM -Settings $settingsPM -Description "Evening NBA odds sync"

Write-Host "Scheduled tasks created:"
Write-Host "  - $taskNameAM (10:00 AM)"
Write-Host "  - $taskNamePM (5:00 PM)"
Write-Host ""
Write-Host "Logs will be at: c:\Users\Michael Riehlman\Documents\Source\bluey\logs\sync.log"
