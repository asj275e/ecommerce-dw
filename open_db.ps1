# -*- coding: utf-8 -*-
<#
  一键打开数仓数据库 dw_demo.db
  ================================

  用法：右键本文件 -> "使用 PowerShell 运行"
       或在 PowerShell 里执行：  .\打开数据库.ps1

  为什么要先结束 GamePPLite：
    那款软件会向所有新进程注入全局 IPC 覆盖层。它的共享内存/信号量
    创建在受限环境下会被拒绝，导致 DB Browser 在 Qt 还没加载时就崩溃退出。
    实测：结束它之后 DB Browser 立即正常启动。

  为什么用 PowerShell 而不是 .bat：
    cmd.exe 按系统 ANSI 代码页（中文系统是 GBK）逐字节解析 .bat 文件。
    UTF-8 编码的中文字节里若出现 0x5C（反斜杠），命令会被切断，中文注释
    也会被当成命令执行。PowerShell 按 UTF-8 读取 .ps1，没有这个问题。
#>

$ErrorActionPreference = 'Stop'

$Base    = Split-Path -Parent $MyInvocation.MyCommand.Path
$ToolDir = Join-Path $Base '_tools\DB Browser for SQLite'
$Exe     = Join-Path $ToolDir 'DB Browser for SQLite.exe'
$Db      = Join-Path $Base 'dw_demo.db'

Write-Host ''
Write-Host '  ============================================' -ForegroundColor Cyan
Write-Host '    准备打开数仓数据库' -ForegroundColor Cyan
Write-Host '  ============================================' -ForegroundColor Cyan
Write-Host ''

# --- 检查主程序 ---
if (-not (Test-Path $Exe)) {
    Write-Host '  [错误] 找不到 DB Browser 主程序：' -ForegroundColor Red
    Write-Host "  $Exe"
    Write-Host ''
    Write-Host '  请确认 _tools 目录完整。' -ForegroundColor Yellow
    Read-Host '  按回车键退出'
    exit 1
}

# --- 检查数据库，没有就生成 ---
if (-not (Test-Path $Db)) {
    Write-Host '  [提示] 尚未找到 dw_demo.db，正在生成...' -ForegroundColor Yellow
    Write-Host ''
    Push-Location $Base
    try { python dw_pipeline.py } finally { Pop-Location }
    if (-not (Test-Path $Db)) {
        Write-Host ''
        Write-Host '  [错误] 生成失败。请手动运行：python dw_pipeline.py' -ForegroundColor Red
        Read-Host '  按回车键退出'
        exit 1
    }
    Write-Host ''
    Write-Host '  生成完成。' -ForegroundColor Green
}

# --- 结束干扰进程 ---
Write-Host '  [1/2] 检查干扰进程 GamePPLite ...' -ForegroundColor Gray
$gp = Get-Process -Name 'GamePPLite' -ErrorAction SilentlyContinue
if ($gp) {
    Write-Host ("        发现 {0} 个进程，正在结束..." -f $gp.Count) -ForegroundColor Gray
    $gp | ForEach-Object {
        try { Stop-Process -Id $_.Id -Force -ErrorAction Stop } catch { }
    }
    Start-Sleep -Seconds 2
    $left = Get-Process -Name 'GamePPLite' -ErrorAction SilentlyContinue
    if ($left) {
        Write-Host '        部分进程未能结束，可能仍会影响启动。' -ForegroundColor Yellow
    } else {
        Write-Host '        已全部结束。' -ForegroundColor Green
    }
    Write-Host '        （这是"游戏加加"，需要时可自行重新打开，不影响本工具）' -ForegroundColor DarkGray
} else {
    Write-Host '        未运行，跳过。' -ForegroundColor Green
}

# --- 启动 ---
Write-Host '  [2/2] 启动 DB Browser 并载入 dw_demo.db ...' -ForegroundColor Gray
Start-Process -FilePath $Exe -WorkingDirectory $ToolDir -ArgumentList "`"$Db`""
Start-Sleep -Seconds 6

$p = Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -like '*DB Browser*' }
Write-Host ''
if ($p) {
    Write-Host '  完成，DB Browser 已打开。' -ForegroundColor Green
    Write-Host ''
    Write-Host '  用法提示：' -ForegroundColor Cyan
    Write-Host '    1. 左侧「数据库结构」查看表定义'
    Write-Host '    2. 切换到「执行 SQL」标签页写查询，Ctrl+Enter 执行'
    Write-Host '    3. 想快速验证指标是否正确，运行：python check_metrics.py'
    Write-Host ''
    Write-Host '  常用查询：' -ForegroundColor Cyan
    Write-Host '    SELECT * FROM ads_daily_overview ORDER BY dt;'
    Write-Host '    SELECT name FROM sqlite_master WHERE type=''table'';'
} else {
    Write-Host '  DB Browser 未能启动。' -ForegroundColor Red
    Write-Host ''
    Write-Host '  可能还有其他全局注入类软件在干扰。排查方法：' -ForegroundColor Yellow
    Write-Host '  打开任务管理器，找名字含 GamePP / Tencent / 360 / Huorong /' -ForegroundColor Yellow
    Write-Host '  Kingsoft / AntiCheat 的进程，逐个结束再试。' -ForegroundColor Yellow
}
Write-Host ''
Read-Host '  按回车键退出'
