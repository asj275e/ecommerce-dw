"""
把 ADS 层的指标表导出成 CSV，供 Quick BI / Sugar BI 等云端 BI 工具上传使用。

为什么导出 CSV 而不是直接传数据库：
  · 云端 BI 支持"上传本地文件"作为数据源，CSV/Excel 是最通用的格式
  · ADS 层本来就是面向报表的成品层，数据量小（几十到几百行），适合上传
  · 数据库文件（9.4 GB）不可能上传，也没必要

用法：
    python export_for_bi.py                      # 默认读 dw_demo.db
    python export_for_bi.py --db real_full.db    # 指定数据库
"""

import argparse
import csv
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.join(HERE, "bi_exports")

# (输出文件名, SQL, 说明)
EXPORTS = [
    (
        "01_每日核心指标.csv",
        """SELECT dt AS 日期, pv AS PV, uv AS UV,
                  buy_uv AS 购买用户数,
                  ROUND(pv_to_buy * 100, 2) AS 购买转化率_百分比
           FROM ads_daily_overview ORDER BY dt""",
        "每日 PV / UV / 购买用户数 / 转化率",
    ),
    (
        "02_次日留存率.csv",
        """SELECT dt AS 日期, active_uv AS 活跃用户数,
                  retained_uv AS 次日回访数,
                  ROUND(retention_1d * 100, 2) AS 次日留存率_百分比
           FROM ads_user_retention ORDER BY dt""",
        "次日留存率",
    ),
    (
        "03_商品浏览Top10.csv",
        """SELECT dt AS 日期, rn AS 排名, item_id AS 商品ID, pv_cnt AS 浏览量
           FROM ads_item_top10 ORDER BY dt, rn""",
        "每日商品浏览 Top10",
    ),
    (
        "04_连续活跃天数分布.csv",
        """SELECT max_consecutive_days AS 最长连续活跃天数,
                  COUNT(*) AS 用户数
           FROM ads_user_consecutive
           GROUP BY max_consecutive_days
           ORDER BY max_consecutive_days""",
        "用户最长连续活跃天数的分布（按天数聚合）",
    ),
]


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="导出 ADS 层指标供 BI 工具使用")
    ap.add_argument("--db", default="dw_demo.db", help="SQLite 数据库（默认 dw_demo.db）")
    args = ap.parse_args()

    db = args.db
    if not os.path.isabs(db):
        db = os.path.join(HERE, db)
    if not os.path.exists(db):
        print(f"[错误] 找不到数据库：{db}")
        print("       先运行 python dw_pipeline.py 生成。")
        return 1

    os.makedirs(OUTDIR, exist_ok=True)
    conn = sqlite3.connect(db)

    print(f"数据源：{os.path.basename(db)}")
    print(f"输出到：{os.path.relpath(OUTDIR, HERE)}")
    print()

    total = 0
    for fname, sql, desc in EXPORTS:
        try:
            cur = conn.execute(sql)
        except sqlite3.Error as e:
            print(f"  [跳过] {fname}  ({e})")
            continue
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        path = os.path.join(OUTDIR, fname)
        # utf-8-sig 带 BOM，Excel 打开中文不乱码（云端 BI 也能正确识别）
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        total += len(rows)
        print(f"  [OK] {fname:<26} {len(rows):>4} 行  — {desc}")

    conn.close()
    print()
    print(f"完成，共导出 {total} 行。")
    print()
    print("下一步：登录 Quick BI，新建数据源时选择「上传本地文件」，")
    print("把 bi_exports 目录下的 CSV 逐个传上去，然后基于它们做仪表板。")
    print()
    print("提示：文件名带 utf-8 BOM，Excel 双击打开中文不会乱码。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
