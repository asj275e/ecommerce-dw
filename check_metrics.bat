@echo off
rem ==========================================================================
rem  Verify the metrics.  Double-click this file.
rem  Runs 19 cross-checks against dw_demo.db (independent recomputation).
rem
rem  ASCII-only on purpose (see run_pipeline.bat for the reason).
rem ==========================================================================

chcp 65001 >nul 2>&1
title Verify metrics - check_metrics.py

set "BASE=%~dp0"
cd /d "%BASE%"

echo.
echo   ============================================
echo     Verify metrics: 19 cross-checks
echo   ============================================
echo.

if not exist "%BASE%dw_demo.db" (
  echo   [ERROR] dw_demo.db not found.
  echo   Run run_pipeline.bat first to generate it.
  echo.
  pause
  exit /b 1
)

python check_metrics.py
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
  echo   ============================================
  echo     ALL CHECKS PASSED
  echo   ============================================
) else (
  echo   ============================================
  echo     SOME CHECKS FAILED
  echo   ============================================
  echo.
  echo   Please send the full output above to the assistant.
)

echo.
pause
