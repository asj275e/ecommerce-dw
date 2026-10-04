@echo off
rem ==========================================================================
rem  Run the data warehouse pipeline.  Double-click this file.
rem
rem  ASCII-only on purpose: cmd.exe parses .bat with the system ANSI codepage
rem  (936/GBK on this machine). The folder path contains Chinese characters,
rem  so we switch the console to UTF-8 (chcp 65001) before using it.
rem ==========================================================================

chcp 65001 >nul 2>&1
title Run dw_pipeline.py

set "BASE=%~dp0"
cd /d "%BASE%"

echo.
echo   ============================================
echo     Run: ODS - DWD - DWS - ADS warehouse build
echo   ============================================
echo.
echo   Working dir: %BASE%
echo.

python dw_pipeline.py
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
  echo   ============================================
  echo     DONE. Database written to dw_demo.db
  echo   ============================================
  echo.
  echo   Next: double-click  check_metrics.bat  to verify metrics
) else (
  echo   ============================================
  echo     FAILED with exit code %RC%
  echo   ============================================
  echo.
  echo   Please send the full error output above to the assistant.
)

echo.
pause
