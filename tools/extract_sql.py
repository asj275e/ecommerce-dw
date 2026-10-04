#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 dw_pipeline.py 中抽取 SQL 常量，生成独立的 .sql 文件。

为什么用脚本抽取而不是手抄：
  手抄容易和实际运行的 SQL 产生偏差。抽取能保证 sql/ 目录里的内容
  与脚本真正执行的语句完全一致 —— 面试官如果对照检查，不会发现不一致。

用法：
    python tools/extract_sql.py
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# 从源文件里抽出 "<NAME> = \"\"\"...\"\"\"" 形式的常量
SRC = os.path.join(ROOT, "dw_pipeline.py")

# 变量名 -> (输出文件名, 描述)
TARGETS = {
    "DWD_SQL":          ("01_dwd_clean.sql",     "DWD 明细层：清洗 + 去重"),
    "DWS_SQL":          ("02_dws_agg.sql",       "DWS 汇总层：按用户+日期沉淀原子指标"),
    "ADS_OVERVIEW":     ("03_ads_overview.sql",  "ADS 应用层：每日 PV / UV / 购买转化率"),
    "ADS_RETENTION":    ("04_ads_retention.sql", "ADS 应用层：次日留存率"),
    "ADS_ITEM_TOP10":   ("05_ads_item_top10.sql", "ADS 应用层：商品浏览 TopN"),
    "ADS_CONSECUTIVE":  ("06_ads_consecutive.sql", "ADS 应用层：连续活跃 ≥3 天"),
}

HEADER = """-- =============================================================================
--  {title}
--  来源：从 dw_pipeline.py 的 {const} 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

"""


def main():
    if not os.path.exists(SRC):
        print(f"[错误] 找不到源文件：{SRC}")
        return 1

    with open(SRC, "r", encoding="utf-8") as f:
        text = f.read()

    outdir = os.path.join(ROOT, "sql")
    os.makedirs(outdir, exist_ok=True)

    found = {}
    for name in TARGETS:
        # 匹配 NAME = """ ... """  （非贪婪，允许内部有空行）
        m = re.search(
            r'^' + re.escape(name) + r'\s*=\s*"""(.*?)"""',
            text, re.S | re.M
        )
        if not m:
            print(f"[警告] 未找到常量 {name}")
            continue
        found[name] = m.group(1).strip()

    if not found:
        print("[错误] 一个 SQL 常量都没抽到，请检查变量名是否变了。")
        return 1

    for name, (fname, title) in TARGETS.items():
        if name not in found:
            continue
        body = HEADER.format(title=title, const=name) + found[name] + "\n"
        path = os.path.join(outdir, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        print(f"  已生成 sql/{fname}  ({len(found[name])} 字符)")

    print(f"\n完成。共抽取 {len(found)} 段 SQL 到 sql/ 目录。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
