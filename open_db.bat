@echo off
rem ==========================================================================
rem  Double-click launcher: open dw_demo.db in DB Browser for SQLite.
rem
rem  This file is deliberately pure ASCII.
rem  Reason: cmd.exe parses .bat content byte-by-byte with the system ANSI
rem  codepage (936/GBK here). Multi-byte Chinese inside a .bat can contain
rem  0x5C (backslash) and break command parsing. All non-ASCII text lives in
rem  the .ps1 instead, which is saved as UTF-8 WITH BOM so PowerShell 5.1
rem  decodes it correctly.
rem ==========================================================================

setlocal
set "BASE=%~dp0"
set "PS1=%BASE%open_db.ps1"

if not exist "%PS1%" (
  echo.
  echo   [ERROR] open_db.ps1 not found in:
  echo   %BASE%
  echo.
  pause
  exit /b 1
)

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
endlocal
