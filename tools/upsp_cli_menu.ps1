param(
    [switch]$Smoke
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$CliPath = Join-Path $RepoRoot "tools\upsp_cli.py"

Set-Location $RepoRoot

$PythonExe = "python"
$PythonArgs = @()
$script:LastUpspCliExitCode = 0
if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3.10 -c "import sys" > $null 2>&1
    if ($LASTEXITCODE -eq 0) {
        $PythonExe = "py"
        $PythonArgs = @("-3.10")
    }
}

function Write-Header {
    Clear-Host
    Write-Host ""
    Write-Host "  UPSP CLI v1" -ForegroundColor Cyan
    Write-Host "  Repo: $RepoRoot" -ForegroundColor DarkGray
    Write-Host "  Python: $PythonExe $($PythonArgs -join ' ')" -ForegroundColor DarkGray
    Write-Host ""
}

function Write-JsonOutput {
    param([string]$Raw)

    if (-not $Raw) {
        Write-Host "(no output)" -ForegroundColor Yellow
        return
    }

    try {
        $obj = $Raw | ConvertFrom-Json
        $obj | ConvertTo-Json -Depth 32 | Write-Host
    } catch {
        Write-Host $Raw
    }
}

function Invoke-UpspCli {
    param(
        [string[]]$CliArgs,
        [switch]$PauseAfter
    )

    Write-Host ""
    Write-Host "> python tools/upsp_cli.py --json $($CliArgs -join ' ')" -ForegroundColor DarkGray
    $AllArgs = @()
    $AllArgs += $PythonArgs
    $AllArgs += $CliPath
    $AllArgs += "--json"
    $AllArgs += $CliArgs
    $raw = & $PythonExe @AllArgs 2>&1
    $code = $LASTEXITCODE
    Write-JsonOutput (($raw | Out-String).Trim())
    if ($code -eq 0) {
        Write-Host "exit code: $code" -ForegroundColor Green
    } else {
        Write-Host "exit code: $code" -ForegroundColor Yellow
    }
    if ($PauseAfter) {
        Write-Host ""
        Read-Host "Press Enter to return"
    }
    $script:LastUpspCliExitCode = $code
}

function Invoke-LiveSend {
    $message = Read-Host "Message for UPSP"
    if (-not $message.Trim()) {
        Write-Host "Empty message, canceled." -ForegroundColor Yellow
        Read-Host "Press Enter to return"
        return
    }
    Write-Host ""
    Write-Host "This will trigger a real Runtime/provider call." -ForegroundColor Yellow
    $confirm = Read-Host "Type LIVE to confirm"
    if ($confirm -ne "LIVE") {
        Write-Host "LIVE was not typed, canceled." -ForegroundColor Yellow
        Read-Host "Press Enter to return"
        return
    }
    Invoke-UpspCli -CliArgs @("send", "--live", "--message", $message) -PauseAfter
}

if ($Smoke) {
    Invoke-UpspCli -CliArgs @("doctor")
    exit $script:LastUpspCliExitCode
}

while ($true) {
    Write-Header
    Write-Host "  1. Doctor          local diagnostics, no provider call" -ForegroundColor White
    Write-Host "  2. Status          current state/flags" -ForegroundColor White
    Write-Host "  3. Recent rounds   latest 5 round files" -ForegroundColor White
    Write-Host "  4. Inspect latest  inspect latest round" -ForegroundColor White
    Write-Host "  5. Send live       real send, requires LIVE confirmation" -ForegroundColor Yellow
    Write-Host "  0. Exit" -ForegroundColor DarkGray
    Write-Host ""
    $choice = Read-Host "Choose"

    switch ($choice) {
        "1" { Invoke-UpspCli -CliArgs @("doctor") -PauseAfter }
        "2" { Invoke-UpspCli -CliArgs @("status") -PauseAfter }
        "3" { Invoke-UpspCli -CliArgs @("rounds", "list", "--limit", "5") -PauseAfter }
        "4" { Invoke-UpspCli -CliArgs @("rounds", "inspect", "--round", "latest") -PauseAfter }
        "5" { Invoke-LiveSend }
        "0" { break }
        default {
            Write-Host "Invalid choice." -ForegroundColor Yellow
            Start-Sleep -Seconds 1
        }
    }
}
