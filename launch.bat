@echo off
title EcoFlow Platform Governor
echo =============================================
echo   EcoFlow Platform Governor - Launching...
echo =============================================
echo.

cd /d "%~dp0build\bin"

if not exist "Echoflow.exe" (
    echo [ERROR] Echoflow.exe not found in build\bin\
    echo Please ensure the repository is intact.
    pause
    exit /b 1
)

echo Starting EcoFlow GUI...
start "" "Echoflow.exe"
