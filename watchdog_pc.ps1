# Nexus Core PC Watchdog (v3.7.6)
# Runs on WINDOWS (not inside WSL). Watches the laptop battery and the
# internet connection and alerts your phone via Telegram:
#   - battery <= 20% AND not charging  -> alert (repeats every 5 min)
#   - internet down for >= 5 minutes   -> alert when it comes BACK
#     (while the internet is down nothing can leave the laptop - that case
#     is covered by the healthchecks.io dead-man's switch in the bot)
#
# ONE-TIME SETUP:
#   1. Copy this file anywhere on Windows, e.g. C:\nexus\watchdog_pc.ps1
#   2. Test it: right-click -> "Run with PowerShell"
#   3. Auto-start at boot: press Win+R, type  shell:startup  and press Enter,
#      then create a shortcut in that folder with this target:
#        powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\nexus\watchdog_pc.ps1"
#
# Keys are read from the bot's .env inside WSL - never hardcoded here.

$ErrorActionPreference = 'SilentlyContinue'

# --- read TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from the WSL .env ---
$cfg = @{}
$envLines = wsl cat /opt/nexus_core/.env 2>$null
foreach ($line in $envLines) {
    if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*$') {
        $cfg[$matches[1]] = $matches[2].Trim('"').Trim("'")
    }
}
$token  = $cfg['TELEGRAM_BOT_TOKEN']
$chatId = $cfg['TELEGRAM_CHAT_ID']
if (-not $token -or -not $chatId) {
    Write-Host "ERROR: could not read TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from WSL .env"
    Write-Host "Tried: wsl cat /opt/nexus_core/.env"
    exit 1
}

$BATTERY_ALERT_PCT      = 20   # alert at/below this % while NOT charging
$INTERNET_DOWN_MINUTES  = 5    # report outages at least this long
$REPEAT_MINUTES         = 5    # re-alert interval while the problem lasts

function Send-Tg([string]$text) {
    try {
        Invoke-RestMethod -Uri "https://api.telegram.org/bot$token/sendMessage" `
            -Method Post -TimeoutSec 15 `
            -Body @{ chat_id = $chatId; text = $text } | Out-Null
    } catch { }
}

$lastBatteryAlert  = [DateTime]::MinValue
$lastInternetAlert = [DateTime]::MinValue
$internetDownSince = $null
$restoredPending   = $false

Write-Host "Nexus PC watchdog running (battery + internet). Close this window to stop."

while ($true) {
    $now = Get-Date

    # ---- battery ----
    $bat = Get-CimInstance -ClassName Win32_Battery
    if ($bat) {
        $pct = [int]$bat.EstimatedChargeRemaining
        $discharging = ($bat.BatteryStatus -eq 1)   # 1 = on battery, 2 = on AC
        if ($discharging -and $pct -le $BATTERY_ALERT_PCT -and `
            ($now - $lastBatteryAlert).TotalMinutes -ge $REPEAT_MINUTES) {
            $lastBatteryAlert = $now
            Send-Tg "LAPTOP BATTERY LOW: $pct% and NOT charging. The trading bot dies when this hits 0% - plug in the charger NOW."
        }
    }

    # ---- internet ----
    $online = Test-Connection -ComputerName 8.8.8.8 -Count 1 -Quiet
    if ($online) {
        if ($restoredPending -and $internetDownSince) {
            $mins = [int]($now - $internetDownSince).TotalMinutes
            Send-Tg "INTERNET IS BACK after $mins minute(s) down. The bot's supervisor should have restarted it - watch Telegram for the restart message."
        }
        $internetDownSince = $null
        $restoredPending   = $false
    } else {
        if (-not $internetDownSince) { $internetDownSince = $now }
        $downMin = ($now - $internetDownSince).TotalMinutes
        if ($downMin -ge $INTERNET_DOWN_MINUTES -and -not $restoredPending -and `
            ($now - $lastInternetAlert).TotalMinutes -ge $REPEAT_MINUTES) {
            $lastInternetAlert = $now
            $restoredPending = $true
            # Usually this can't send (no internet). It still tries - some
            # outages only block parts of the network.
            Send-Tg "INTERNET DOWN for $([int]$downMin) minutes - the bot cannot trade or report until it returns."
        }
    }

    Start-Sleep -Seconds 60
}
