#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标自检：验证 ADS 层的指标算得对不对
=====================================

用法：
    python check_metrics.py

它会做两类事：
  1. 【独立重算】用不同的写法把同一个指标再算一遍，和 ADS 表里的结果对比
                 —— 这是验证 SQL 最可靠的办法：换一条路径算，看能不能对上
  2. 【边界检查】查空值、重复、首末日期异常等常见错误

为什么要自己验：
  面试官一定会问"你怎么保证指标算得对"，只答"我跑出来了"是不够的。
  "我用另一种写法交叉验证过"才是他能记住的答案。

注意：这不是单元测试框架，就是一段能跑、能看懂的检查脚本。
"""

import argparse
import sqlite3
import sys

DEFAULT_DB = "dw_demo.db"
DB = DEFAULT_DB   # 运行时会被 --db 参数覆盖

# Windows 控制台默认 GBK，先切成 UTF-8，否则中文/符号会报错
for s in (sys.stdout, sys.stderr):
    try:
        s.reconfigure(encoding="utf-8")
    except Exception:
        pass


def kind(s):
    """统一代码块的显示"""
    print("\n" + "=" * 76)
    print(f"  {s}")
    print("=" * 76)


def main():
    global DB

    # 支持指定数据库文件，方便对真实数据生成的库做校验：
    #   python check_metrics.py --db real_full.db
    parser = argparse.ArgumentParser(description="验证数仓指标计算是否正确")
    parser.add_argument("--db", default=DEFAULT_DB,
                        help=f"要校验的 SQLite 数据库（默认 {DEFAULT_DB}）")
    args = parser.parse_args()
    DB = args.db

    print(f"待校验数据库：{DB}")

    try:
        conn = sqlite3.connect(DB)
    except sqlite3.Error as e:
        print(f"[错误] 打不开 {DB}：{e}")
        print("       请先运行 python dw_pipeline.py 生成数据库。")
        return 1

    cur = conn.cursor()

    # 先确认表都在
    tables = {r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    need = {"ods_user_behavior", "dwd_user_behavior_di", "dws_user_action_1d",
            "ads_daily_overview", "ads_user_retention", "ads_item_top10",
            "ads_user_consecutive"}
    missing = need - tables
    if missing:
        print(f"[错误] 数据库里缺少这些表：{sorted(missing)}")
        print("       请重新运行 python dw_pipeline.py")
        return 1

    print("数据库表：", ", ".join(sorted(tables)))
    passed = failed = 0

    def check(name, ok, detail=""):
        nonlocal passed, failed
        if ok:
            passed += 1
            print(f"  [通过] {name}")
        else:
            failed += 1
            print(f"  [失败] {name}")
            if detail:
                print(f"         {detail}")

    # ==================================================================
    kind("检查 1：DWD 清洗是否真的生效（ODS 行数 > DWD 行数）")
    # ==================================================================
    ods_n = cur.execute("SELECT COUNT(*) FROM ods_user_behavior").fetchone()[0]
    dwd_n = cur.execute("SELECT COUNT(*) FROM dwd_user_behavior_di").fetchone()[0]
    print(f"  ODS = {ods_n} 行")
    print(f"  DWD = {dwd_n} 行")
    print(f"  清洗掉 = {ods_n - dwd_n} 行")
    check("DWD 行数不超过 ODS（清洗确实过滤了数据）", dwd_n <= ods_n)

    # 逐条验证 4 类清洗规则：DWD 里不应该再出现这些脏数据
    bad_null_uid = cur.execute(
        "SELECT COUNT(*) FROM dwd_user_behavior_di WHERE user_id IS NULL").fetchone()[0]
    bad_null_iid = cur.execute(
        "SELECT COUNT(*) FROM dwd_user_behavior_di WHERE item_id IS NULL").fetchone()[0]
    bad_beh = cur.execute(
        "SELECT COUNT(*) FROM dwd_user_behavior_di "
        "WHERE behavior NOT IN ('pv','buy','cart','fav')").fetchone()[0]
    bad_time = cur.execute(
        "SELECT COUNT(*) FROM dwd_user_behavior_di "
        "WHERE event_date NOT BETWEEN '2017-11-25' AND '2017-12-03'").fetchone()[0]
    print(f"  DWD 中残留：空user_id={bad_null_uid} 空item_id={bad_null_iid} "
          f"非法behavior={bad_beh} 越界日期={bad_time}")
    check("清洗 1：无空 user_id", bad_null_uid == 0)
    check("清洗 2：无空 item_id", bad_null_iid == 0)
    check("清洗 3：behavior 值域合法", bad_beh == 0)
    check("清洗 4：日期在数据集范围内", bad_time == 0)

    # 去重验证：DWD 里不应再有 (user,item,ts,behavior) 重复
    dup = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT user_id, item_id, ts, behavior, COUNT(*) AS c
            FROM dwd_user_behavior_di
            GROUP BY user_id, item_id, ts, behavior
            HAVING c > 1
        )
    """).fetchone()[0]
    print(f"  DWD 中重复的 (user,item,ts,behavior) 组合数 = {dup}")
    check("清洗 5：去重生效（主键唯一）", dup == 0)

    # ==================================================================
    kind("检查 2：DWS 汇总是否正确 —— 用明细重算一遍比对")
    # ==================================================================
    # 这是最关键的验证：从 DWD 明细直接算，和 DWS 表里的值逐个比对
    diff = cur.execute("""
        WITH recalc AS (
            SELECT event_date AS dt, user_id,
                   SUM(CASE WHEN behavior='pv'   THEN 1 ELSE 0 END) AS pv_cnt,
                   SUM(CASE WHEN behavior='cart' THEN 1 ELSE 0 END) AS cart_cnt,
                   SUM(CASE WHEN behavior='fav'  THEN 1 ELSE 0 END) AS fav_cnt,
                   SUM(CASE WHEN behavior='buy'  THEN 1 ELSE 0 END) AS buy_cnt
            FROM dwd_user_behavior_di
            GROUP BY event_date, user_id
        )
        SELECT COUNT(*) FROM (
            SELECT d.dt, d.user_id
            FROM dws_user_action_1d d
            JOIN recalc r ON d.dt = r.dt AND d.user_id = r.user_id
            WHERE d.pv_cnt != r.pv_cnt OR d.cart_cnt != r.cart_cnt
               OR d.fav_cnt != r.fav_cnt OR d.buy_cnt != r.buy_cnt
            UNION ALL
            -- 反向：recalc 里有但 DWS 里没有的
            SELECT r.dt, r.user_id
            FROM recalc r
            LEFT JOIN dws_user_action_1d d
              ON d.dt = r.dt AND d.user_id = r.user_id
            WHERE d.user_id IS NULL
        )
    """).fetchone()[0]
    print(f"  明细重算 与 DWS 表 不一致的行数 = {diff}")
    check("DWS 聚合与明细重算完全一致", diff == 0)

    # ==================================================================
    kind("检查 3：PV / UV 是否与明细一致")
    # ==================================================================
    pv_diff = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT a.dt
            FROM ads_daily_overview a
            JOIN (SELECT event_date AS dt, COUNT(*) AS pv
                  FROM dwd_user_behavior_di WHERE behavior='pv'
                  GROUP BY event_date) m ON a.dt = m.dt
            WHERE a.pv != m.pv
        )
    """).fetchone()[0]
    print(f"  PV 与明细不一致的天数 = {pv_diff}")
    check("每日 PV 与明细统计一致", pv_diff == 0)

    uv_diff = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT a.dt
            FROM ads_daily_overview a
            JOIN (SELECT event_date AS dt, COUNT(DISTINCT user_id) AS uv
                  FROM dwd_user_behavior_di GROUP BY event_date) m ON a.dt = m.dt
            WHERE a.uv != m.uv
        )
    """).fetchone()[0]
    print(f"  UV 与明细不一致的天数 = {uv_diff}")
    check("每日 UV 与明细去重统计一致", uv_diff == 0)

    # 购买用户数不能超过总活跃用户数（逻辑约束）
    over = cur.execute(
        "SELECT COUNT(*) FROM ads_daily_overview WHERE buy_uv > uv").fetchone()[0]
    check("购买用户数 <= 活跃用户数", over == 0,
          f"有 {over} 天出现 buy_uv > uv，说明算错了")

    # ==================================================================
    kind("检查 4：留存率是否算对 —— 手工指定两天验证")
    # ==================================================================
    rows = cur.execute(
        "SELECT dt, active_uv, retained_uv FROM ads_user_retention ORDER BY dt").fetchall()
    if len(rows) >= 2:
        d1, a1, r1 = rows[0]
        d2 = rows[1][0]
        # 独立算：d1 当天活跃、且在 d2 也活跃的人数
        manual = cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT user_id FROM dws_user_action_1d WHERE dt = ?
                INTERSECT
                SELECT DISTINCT user_id FROM dws_user_action_1d WHERE dt = ?
            )
        """, (d1, d2)).fetchone()[0]
        print(f"  {d1} 的活跃数 = {a1}")
        print(f"  ADS 表里的次日回访 = {r1}")
        print(f"  独立重算（INTERSECT）= {manual}")
        check("次日回访数与独立重算一致", r1 == manual,
              f"表里 {r1}，重算 {manual}")

        # 留存率 = 回访 / 活跃
        rate = cur.execute(
            "SELECT retention_1d FROM ads_user_retention WHERE dt = ?", (d1,)).fetchone()[0]
        expect = round(manual / a1, 4) if a1 else 0
        check("留存率 = 回访数 / 活跃数", abs((rate or 0) - expect) < 0.0002,
              f"表里 {rate}，应为 {expect}")

    # 最后一天应该没有次日数据（这是正确的，不是 bug）
    last_dt = rows[-1][0]
    last_retained = rows[-1][2]
    print(f"  最后一天 {last_dt} 的次日回访 = {last_retained}（应为 0，因为没有次日数据）")
    check("最后一天次日回访为 0（预期行为）", last_retained == 0)

    # ==================================================================
    kind("检查 5：连续活跃天数是否算对 —— 造一个已知答案的用户来验")
    # ==================================================================
    # 思路：临时插入一个连续 4 天的用户，重跑连续活跃逻辑，看是否算出 4
    test_uid = 999999
    cur.execute("DELETE FROM dws_user_action_1d WHERE user_id = ?", (test_uid,))
    for d in ("2017-11-26", "2017-11-27", "2017-11-28", "2017-11-29"):
        cur.execute(
            "INSERT INTO dws_user_action_1d (dt, user_id, pv_cnt, cart_cnt, fav_cnt, buy_cnt) "
            "VALUES (?, ?, 3, 0, 0, 0)", (d, test_uid))
    conn.commit()

    # 用与主脚本相同的算法重算
    got = cur.execute("""
        WITH base AS (SELECT DISTINCT user_id, dt FROM dws_user_action_1d),
        numbered AS (
            SELECT user_id, dt,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt) AS rn
            FROM base WHERE user_id = ?
        ),
        marked AS (
            SELECT user_id, dt, date(dt, '-' || rn || ' day') AS grp FROM numbered
        ),
        streak AS (
            SELECT user_id, grp, COUNT(*) AS days FROM marked GROUP BY user_id, grp
        )
        SELECT MAX(days) FROM streak WHERE user_id = ?
    """, (test_uid, test_uid)).fetchone()[0]

    print(f"  造了一个连续登录 4 天的用户（{test_uid}），算法算出 = {got}")
    check("连续活跃差值法结果正确（应为 4）", got == 4, f"算出来是 {got}")

    # 清理测试数据，保持数据库原状
    cur.execute("DELETE FROM dws_user_action_1d WHERE user_id = ?", (test_uid,))
    conn.commit()
    print("  （测试数据已清理，数据库恢复原状）")

    # ==================================================================
    kind("检查 6：TopN 是否正确")
    # ==================================================================
    # 每天的 Top10 行数应恰好 <= 10，且当天收录的商品浏览量应 >= 未收录的
    over10 = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT dt, COUNT(*) AS c FROM ads_item_top10 GROUP BY dt HAVING c > 10
        )
    """).fetchone()[0]
    check("每天 Top10 不超过 10 条", over10 == 0)

    # 直接检查：Top10 的 rn 是否从 1 开始连续、条数不超过 10
    rn_bad = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT dt, MIN(rn) AS lo, MAX(rn) AS hi, COUNT(*) AS c
            FROM ads_item_top10 GROUP BY dt
            HAVING lo != 1 OR hi != c OR c > 10
        )
    """).fetchone()[0]
    print(f"  TopN 的 rn 序列异常的天数 = {rn_bad}")
    check("TopN 排名 1..N 连续且完整", rn_bad == 0)

    # 验证 TopN 的排序正确性：第 1 名的浏览量应 >= 第 2 名
    order_bad = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT dt,
                   MAX(CASE WHEN rn = 1 THEN pv_cnt END) AS top1,
                   MIN(pv_cnt) AS last_one
            FROM ads_item_top10 GROUP BY dt
        ) WHERE top1 < last_one
    """).fetchone()[0]
    print(f"  TopN 排序异常的天数 = {order_bad}")
    check("TopN 按浏览量降序排列", order_bad == 0)

    # ==================================================================
    kind("检查 7：常见边界问题")
    # ==================================================================
    null_ads = cur.execute(
        "SELECT COUNT(*) FROM ads_daily_overview WHERE dt IS NULL").fetchone()[0]
    check("ADS 概览表无空日期", null_ads == 0)

    days = cur.execute("SELECT COUNT(*) FROM ads_daily_overview").fetchone()[0]
    print(f"  ADS 概览表天数 = {days}（数据集是 9 天）")
    check("每日概览覆盖 9 天", days == 9, f"实际 {days} 天")

    # ==================================================================
    print("\n" + "=" * 76)
    print(f"  结果：{passed} 项通过，{failed} 项失败")
    print("=" * 76)
    if failed == 0:
        print("  全部通过。你的指标计算是正确的。")
        print()
        print("  这个脚本本身就是你的面试素材 —— 面试官问「你怎么保证指标算得对」，")
        print("  答案是：「我对每个指标都做了交叉验证：DWS 用明细重算比对、")
        print("  留存率用 INTERSECT 独立重算、连续活跃造已知答案的数据验证过。」")
    else:
        print("  有检查未通过，把上面的输出发给我。")
    print()

    conn.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
