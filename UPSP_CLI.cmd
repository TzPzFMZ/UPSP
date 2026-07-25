@echo off
setlocal

set "REPO_DIR=%~dp0"
set "MENU_PS1=%REPO_DIR%tools\upsp_cli_menu.ps1"

if /I "%~1"=="--smoke" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%MENU_PS1%" -Smoke
  exit /b %ERRORLEVEL%
)

where wt.exe >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" wt.exe -w 0 nt --title "UPSP CLI" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%MENU_PS1%"
) else (
  start "" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%MENU_PS1%"
)

endlocal
