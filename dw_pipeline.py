#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
零依赖四层数仓（ODS -> DWD -> DWS -> ADS）
=========================================

为什么有这个脚本：
    《项目实操指南》用的是 Spark + Hive 或 Docker，装环境可能要一整天，
    一旦卡住项目就废了。这个脚本只用 Python 标准库（sqlite3 + csv），
    不需要 Docker、不需要 Java、不需要 Spark、不需要联网装包。

    目的是让你在 1 小时内跑通完整的"分层建仓 + 指标产出"，
    理解分层到底在做什么——然后你再去补 Hive/Spark 版，因为那时候
    你已经知道每一层该写什么 SQL 了。

用法：
    python dw_pipeline.py                     # 用内置样例数据跑
    python dw_pipeline.py --csv UserBehavior.csv   # 用真实数据跑
    python dw_pipeline.py --csv UserBehavior.csv --limit 5000000   # 只取前 500 万行

真实数据（阿里天池 UserBehavior）列顺序：
    user_id, item_id, category_id, behavior, timestamp
    无表头，逗号分隔。用 --csv 时会自动识别。

产出：
    dw_demo.db   SQLite 数据库，含 6 张表（4 层 + 2 张明细）

✅ 验证状态（已实跑验证，非推断）：
    环境：Python 3.11.9 / SQLite 3.45.1 / Windows
    - 默认运行（样例数据 11,104 行）：通过，四层全部产出
    - --limit 2000：通过，精确截断
    - 大规模测试（3,000,000 行）：通过，总耗时 31.8 秒，流式读取无 OOM
    验证中发现并修复了 2 个缺陷（详见同目录《运行验证记录.md》）：
      1. Windows GBK 控制台下 "✓" 字符导致 UnicodeEncodeError，脚本在打印环境
         信息时就崩溃 —— 已在 main() 中把 stdout/stderr 切到 UTF-8
      2. 样例数据未模拟用户流失，导致次日留存算出 98%-100% 的不真实数字 ——
         已改为按核心/普通/低频/新增四层模拟活跃度，留存降到合理的 59%-76%

⚠️ 仍未验证：真实天池 UserBehavior 数据（1 亿行）未实跑；按 300 万行 31.8 秒
   线性外推，全量约需 18-20 分钟。真实文件可能存在未预料的脏格式。
"""

import argparse
import calendar
import csv
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DB_PATH = "dw_demo.db"

# ============================================================================
# 时区约定（重要）
# ============================================================================
# 真实数仓必须显式声明时区，否则"某一天的指标"是不可复现的。
# 本项目统一按 **UTC+8（中国标准时间）** 划分自然日，并且**不依赖运行机器的时区**。
#
# 为什么必须显式：SQLite 的 date(ts,'unixepoch') 一律按 UTC 解释时间戳，
# 而 Python 的 datetime.timestamp() 按**本机时区**解释。两者混用会导致
# 日期边界平移 N 小时、部分数据被当作"越界"误杀——这是一个不会报错、
# 但会让所有日报口径失真的隐蔽 bug。见《运行验证记录.md》。
#
# 因此：
#   · 生成数据时，按"UTC+8 的墙钟时间"构造时间戳（用 calendar.timegm 转为真正的 UTC 纪元）
#   · SQL 中一律用 date(ts, 'unixepoch', '+8 hours') 取日期
TZ_OFFSET_HOURS = 8
TZ_MODIFIER = f"+{TZ_OFFSET_HOURS} hours"      # SQLite date() 用的修饰符
TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))  # Python 用的固定时区


# ============================================================================
# 第 0 部分：数据准备
# ============================================================================

def generate_sample_data(path="sample_user_behavior.csv", n_users=800, n_items=120, days=9):
    """生成样例数据，格式与天池 UserBehavior 完全一致（无表头、逗号分隔）。

    为了让指标"看起来像真的"，这里模拟了真实的用户活跃度衰减：
      - 少数核心用户（约 10%）几乎每天都来
      - 多数普通用户只活跃 1-4 天，且集中在数据周期前半段
      - 少数新用户只在后半段出现
    如果不做这个模拟，所有用户天天活跃，次日留存会算出 99%+ 这种
    明显不真实的数字，面试时一眼就会被看穿。
    """
    behaviors = ["pv", "pv", "pv", "pv", "cart", "fav", "buy"]  # 浏览最多，购买最少
    # 模拟热点商品：20% 的商品承接 80% 的流量（这会造成真实的数据倾斜）
    hot_items = random.sample(range(1, n_items + 1), k=max(1, n_items // 5))

    start = datetime(2017, 11, 25, 0, 0, 0)   # 按 TZ_OFFSET_HOURS 的墙钟时间理解
    all_days = [start + timedelta(days=d) for d in range(days)]

    def to_epoch(wall_clock: datetime) -> int:
        """把"UTC+8 墙钟时间"转成真正的 Unix 时间戳（UTC 纪元）。

        用 calendar.timegm 而不是 datetime.timestamp()：
        前者把输入当作 UTC 处理，是确定性的；
        后者按本机时区处理，换台机器结果就变。
        这里先减掉 TZ 偏移再取 epoch，等价于"该墙钟时间在 UTC+8 对应的时刻"。
        """
        return calendar.timegm((wall_clock - timedelta(hours=TZ_OFFSET_HOURS)).timetuple())

    rows = []
    for user_id in range(1, n_users + 1):
        r = random.random()
        if r < 0.10:
            # 核心用户：基本每天都在（长连续段）
            active_days = all_days
        elif r < 0.45:
            # 普通用户：活跃 2-5 天，偏向周期前半段
            k = random.randint(2, 5)
            active_days = sorted(random.sample(all_days, k))
        elif r < 0.75:
            # 低频用户：只活跃 1-2 天
            k = random.randint(1, 2)
            active_days = sorted(random.sample(all_days, k))
        else:
            # 后期新增用户：只在下半段出现
            active_days = sorted(random.sample(all_days[len(all_days) // 2:], random.randint(1, 3)))

        # 每个活跃日产生若干条行为
        for day in active_days:
            for _ in range(random.randint(1, 8)):
                if random.random() < 0.8:
                    item_id = random.choice(hot_items)      # 热点商品
                else:
                    item_id = random.randint(1, n_items)

                ts = day + timedelta(seconds=random.randint(0, 86399))
                rows.append((
                    user_id,
                    item_id,
                    random.randint(1, 20),                  # category_id
                    random.choice(behaviors),
                    to_epoch(ts),                           # 按 UTC+8 墙钟时间转 epoch
                ))

    # 故意注入脏数据，让清洗步骤有意义（这是真实数据集里也存在的情况）
    ts_start = to_epoch(start)
    rows.append((None, 999, 1, "pv", ts_start))                                  # user_id 为空
    rows.append((1, None, 1, "buy", ts_start))                                   # item_id 为空
    rows.append((1, 1, 1, "unknown_behavior", ts_start))                         # 未知行为类型
    rows.append((1, 1, 1, "pv", to_epoch(datetime(2016, 1, 1))))                 # 时间越界
    rows.append((2, 2, 1, "pv", ts_start))                                       # 下面这条与之重复
    rows.append((2, 2, 1, "pv", ts_start))

    random.shuffle(rows)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)

    print(f"[数据] 已生成样例数据 {path}，共 {len(rows)} 行")
    print(f"[数据] 其中注入了 6 行脏数据（空 user_id / 空 item_id / 未知行为 / 时间越界 / 重复行）")
    print(f"[数据] 已模拟用户活跃度分层（核心/普通/低频/新增），避免留存率失真")
    print(f"[数据] 热点商品（约 20% 的商品承接 80% 流量）用于模拟数据倾斜\n")
    return path


def load_csv_to_ods(conn, csv_path, limit=None):
    """读取 CSV，原样写入 ODS 层。ODS 不做任何清洗——这是数仓的原则。"""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ods_user_behavior")
    cur.execute("""
        CREATE TABLE ods_user_behavior (
            user_id     INTEGER,
            item_id     INTEGER,
            category_id INTEGER,
            behavior    TEXT,
            ts          INTEGER,
            dt          TEXT
        )
    """)

    batch = []
    total = 0
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for parts in csv.reader(f):
            if limit is not None and total >= limit:
                break
            if len(parts) < 5:
                continue
            try:
                uid = int(parts[0]) if parts[0].strip() else None
                iid = int(parts[1]) if parts[1].strip() else None
                cid = int(parts[2]) if parts[2].strip() else None
                beh = parts[3].strip()
                ts = int(float(parts[4]))
            except (ValueError, IndexError):
                continue

            # 与 SQL 侧的 date(ts,'unixepoch','+8 hours') 保持完全一致的时区口径。
            # 注意这里刻意不用 datetime.fromtimestamp（那会按本机时区走，不可复现）。
            try:
                dt = (datetime(1970, 1, 1) + timedelta(seconds=ts + TZ_OFFSET_HOURS * 3600)).strftime("%Y-%m-%d")
            except (OverflowError, OSError, ValueError):
                dt = None

            batch.append((uid, iid, cid, beh, ts, dt))
            total += 1
            if len(batch) >= 50000:
                cur.executemany("INSERT INTO ods_user_behavior VALUES (?,?,?,?,?,?)", batch)
                batch = []

    if batch:
        cur.executemany("INSERT INTO ods_user_behavior VALUES (?,?,?,?,?,?)", batch)
    conn.commit()
    print(f"[ODS] 已装载 {total} 行原始数据（未做任何清洗）")
    return total


# ============================================================================
# 第 1 部分：DWD —— 清洗层
# ============================================================================

DWD_SQL = """
DROP TABLE IF EXISTS dwd_user_behavior_di;
CREATE TABLE dwd_user_behavior_di AS
WITH filtered AS (
    -- 清洗 1-3：主键空值 / 行为值域 / 时间越界
    -- 注意：日期一律用 date(ts,'unixepoch','+8 hours')，与生成端的 UTC+8 口径一致。
    -- 若这里直接写 date(ts,'unixepoch')（UTC），会和生成端差 8 小时，
    -- 导致约 1/3 的行落到错误的自然日、部分首日数据被误判为"越界"而丢弃。
    SELECT
        user_id,
        item_id,
        category_id,
        behavior,
        ts,
        datetime(ts, 'unixepoch', '+8 hours') AS event_time,
        date(ts, 'unixepoch', '+8 hours')     AS event_date
    FROM ods_user_behavior
    WHERE user_id IS NOT NULL
      AND item_id IS NOT NULL
      AND behavior IN ('pv', 'buy', 'cart', 'fav')
      AND date(ts, 'unixepoch', '+8 hours') BETWEEN '2017-11-25' AND '2017-12-03'
),
deduped AS (
    -- 清洗 4：去重。
    -- 同一用户对同一商品的「同一秒同一行为」视为重复上报，只保留一条。
    -- 这里 PARTITION BY 与 ORDER BY 用同一批列，意味着分区内所有行等价，
    -- 保留哪一条都行——这是有意的，不是写错了。用两层 CTE 是为了让意图显式。
    -- 取舍说明：去重键不含 category_id，因此同一 (user,item,sec,behavior) 下
    -- 若 category_id 不同，保留哪个是任意的；本项目接受该取舍（代价可忽略）。
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, item_id, ts, behavior
            ORDER BY category_id
        ) AS rn
    FROM filtered
)
SELECT
    user_id, item_id, category_id, behavior, ts, event_time, event_date
FROM deduped
WHERE rn = 1;
"""


def build_dwd(conn):
    cur = conn.cursor()
    cur.executescript(DWD_SQL)
    conn.commit()

    ods_cnt = cur.execute("SELECT COUNT(*) FROM ods_user_behavior").fetchone()[0]
    dwd_cnt = cur.execute("SELECT COUNT(*) FROM dwd_user_behavior_di").fetchone()[0]
    print(f"[DWD] 清洗完成：{ods_cnt} 行 -> {dwd_cnt} 行（过滤/去重 {ods_cnt - dwd_cnt} 行）")
    return dwd_cnt


# ============================================================================
# 第 2 部分：DWS —— 轻度汇总层（沉淀公共指标）
# ============================================================================

DWS_SQL = """
DROP TABLE IF EXISTS dws_user_action_1d;
CREATE TABLE dws_user_action_1d AS
SELECT
    event_date                                          AS dt,
    user_id,
    SUM(CASE WHEN behavior = 'pv'   THEN 1 ELSE 0 END)  AS pv_cnt,
    SUM(CASE WHEN behavior = 'cart' THEN 1 ELSE 0 END)  AS cart_cnt,
    SUM(CASE WHEN behavior = 'fav'  THEN 1 ELSE 0 END)  AS fav_cnt,
    SUM(CASE WHEN behavior = 'buy'  THEN 1 ELSE 0 END)  AS buy_cnt
FROM dwd_user_behavior_di
GROUP BY event_date, user_id;
"""


def build_dws(conn):
    cur = conn.cursor()
    cur.executescript(DWS_SQL)
    conn.commit()
    cnt = cur.execute("SELECT COUNT(*) FROM dws_user_action_1d").fetchone()[0]
    print(f"[DWS] 已沉淀用户日粒度公共指标 {cnt} 行（pv/cart/fav/buy 四个原子指标）")
    return cnt


# ============================================================================
# 第 3 部分：ADS —— 应用层（面向报表的成品指标）
# ============================================================================

ADS_OVERVIEW = """
DROP TABLE IF EXISTS ads_daily_overview;
CREATE TABLE ads_daily_overview AS
SELECT
    dt,
    SUM(pv_cnt)                                     AS pv,
    COUNT(DISTINCT user_id)                         AS uv,
    COUNT(DISTINCT CASE WHEN buy_cnt > 0 THEN user_id END) AS buy_uv,
    ROUND(
        COUNT(DISTINCT CASE WHEN buy_cnt > 0 THEN user_id END) * 1.0
        / COUNT(DISTINCT user_id), 4
    )                                               AS pv_to_buy
FROM dws_user_action_1d
GROUP BY dt;
"""

# 次日留存：注意这里用的是 DWS 层，且必须保证 dt 连续
ADS_RETENTION = """
DROP TABLE IF EXISTS ads_user_retention;
CREATE TABLE ads_user_retention AS
SELECT
    a.dt,
    COUNT(DISTINCT a.user_id)                       AS active_uv,
    COUNT(DISTINCT b.user_id)                       AS retained_uv,
    ROUND(
        COUNT(DISTINCT b.user_id) * 1.0
        / COUNT(DISTINCT a.user_id), 4
    )                                               AS retention_1d
FROM (SELECT DISTINCT dt, user_id FROM dws_user_action_1d) a
LEFT JOIN (SELECT DISTINCT dt, user_id FROM dws_user_action_1d) b
       ON a.user_id = b.user_id
      AND b.dt = date(a.dt, '+1 day')
GROUP BY a.dt;
"""

# 商品 TopN：窗口函数
# 刻意拆成两段（agg -> rank），而不是在一层里写
#   ROW_NUMBER() OVER (... ORDER BY COUNT(*) DESC) ... GROUP BY ...
# 原因：把聚合函数直接放进窗口的 ORDER BY 在部分 SQLite 版本上会报
# "misuse of aggregate"。拆开后语义等价、可读性更好，且能加 tie-breaker
# （ORDER BY pv_cnt DESC, item_id ASC），保证并列时取数结果稳定可复现。
ADS_ITEM_TOP10 = """
DROP TABLE IF EXISTS ads_item_top10;
CREATE TABLE ads_item_top10 AS
WITH agg AS (
    SELECT
        event_date AS dt,
        item_id,
        COUNT(*)   AS pv_cnt
    FROM dwd_user_behavior_di
    WHERE behavior = 'pv'
    GROUP BY event_date, item_id
),
ranked AS (
    SELECT
        dt,
        item_id,
        pv_cnt,
        ROW_NUMBER() OVER (PARTITION BY dt ORDER BY pv_cnt DESC, item_id ASC) AS rn
    FROM agg
)
SELECT dt, item_id, pv_cnt, rn
FROM ranked
WHERE rn <= 10;
"""

# 连续活跃 >= 3 天：date - row_number 差值法
#
# 注意：这里把 ROW_NUMBER 拆到独立的 CTE（numbered）里计算，
# 而不是直接在 date() 的参数里引用窗口函数结果。原因是 SQLite 对
# "同一 SELECT 中引用别名/窗口函数结果" 的解析行为在不同版本上不一致，
# 拆开写可以保证在所有 3.25+ 版本上都稳定工作，也更容易读懂。
ADS_CONSECUTIVE = """
DROP TABLE IF EXISTS ads_user_consecutive;
CREATE TABLE ads_user_consecutive AS
WITH base AS (
    SELECT DISTINCT user_id, dt FROM dws_user_action_1d
),
numbered AS (
    SELECT
        user_id,
        dt,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt) AS rn
    FROM base
),
marked AS (
    -- 连续日期的相邻差值为 1，而 rn 每次加 1，两者相减得到同一常数 → 连续段标识
    SELECT
        user_id,
        dt,
        date(dt, '-' || rn || ' day') AS grp
    FROM numbered
),
streak AS (
    SELECT user_id, grp, COUNT(*) AS days,
           MIN(dt) AS start_dt, MAX(dt) AS end_dt
    FROM marked
    GROUP BY user_id, grp
)
SELECT user_id, MAX(days) AS max_consecutive_days
FROM streak
GROUP BY user_id
HAVING MAX(days) >= 3;
"""


def build_ads(conn):
    cur = conn.cursor()
    for name, sql in [
        ("ads_daily_overview", ADS_OVERVIEW),
        ("ads_user_retention", ADS_RETENTION),
        ("ads_item_top10", ADS_ITEM_TOP10),
        ("ads_user_consecutive", ADS_CONSECUTIVE),
    ]:
        cur.executescript(sql)
        cnt = cur.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"[ADS] {name}: {cnt} 行")
    conn.commit()
    print()
    return True


# ============================================================================
# 第 4 部分：结果展示
# ============================================================================

def show_results(conn):
    cur = conn.cursor()

    print("=" * 74)
    print("每日核心指标（PV / UV / 购买用户数 / 购买转化率）")
    print("=" * 74)
    print(f"{'日期':<12}{'PV':>8}{'UV':>8}{'购买UV':>9}{'转化率':>10}")
    print("-" * 74)
    for dt, pv, uv, buy_uv, rate in cur.execute(
        "SELECT dt, pv, uv, buy_uv, pv_to_buy FROM ads_daily_overview ORDER BY dt"
    ):
        print(f"{dt:<12}{pv:>8}{uv:>8}{buy_uv:>9}{(rate if rate else 0):>10.2%}")

    print()
    print("=" * 74)
    print("次日留存率")
    print("=" * 74)
    print(f"{'日期':<12}{'活跃UV':>9}{'次日回访':>10}{'次日留存':>11}")
    print("-" * 74)
    max_dt = cur.execute("SELECT MAX(dt) FROM ads_user_retention").fetchone()[0]
    for dt, au, ru, rate in cur.execute(
        "SELECT dt, active_uv, retained_uv, retention_1d FROM ads_user_retention ORDER BY dt"
    ):
        # 最后一天没有"次日"可关联时，留存显示为 "—" 而不是 0.00%，
        # 否则会被误读成"留存极差"，而实际是"无数据"。
        if max_dt is not None and dt == max_dt:
            print(f"{dt:<12}{au:>9}{ru:>10}{'—':>11}   <- 数据集最后一天，无次日数据")
        else:
            print(f"{dt:<12}{au:>9}{ru:>10}{(rate if rate is not None else 0):>11.2%}")

    print()
    print("=" * 74)
    print("某日商品浏览 Top10（示例：取第一个日期）")
    print("=" * 74)
    first_dt = cur.execute("SELECT MIN(dt) FROM ads_item_top10").fetchone()[0]
    print(f"日期：{first_dt}")
    print(f"{'排名':<8}{'商品ID':>10}{'浏览量':>10}")
    print("-" * 74)
    for rn, item_id, pv_cnt in cur.execute(
        "SELECT rn, item_id, pv_cnt FROM ads_item_top10 WHERE dt = ? ORDER BY rn", (first_dt,)
    ):
        print(f"{rn:<8}{item_id:>10}{pv_cnt:>10}")

    print()
    print("=" * 74)
    print("连续活跃 >= 3 天的用户（date - row_number 差值法）")
    print("=" * 74)
    rows = list(cur.execute(
        "SELECT user_id, max_consecutive_days FROM ads_user_consecutive "
        "ORDER BY max_consecutive_days DESC LIMIT 10"
    ))
    if rows:
        print(f"{'用户ID':<12}{'最长连续天数':>14}")
        print("-" * 74)
        for uid, d in rows:
            print(f"{uid:<12}{d:>14}")
    else:
        print("（样例数据量太小，没有满足连续 3 天的用户——这是正常的，")
        print("  用真实天池数据跑就会有。重点是 SQL 逻辑本身跑通了。）")

    print()


def show_layers(conn):
    """展示四层的数据量递减关系，这是'分层'最直观的证据。"""
    cur = conn.cursor()
    print("=" * 74)
    print("分层数据量对比（这是你面试要讲的东西）")
    print("=" * 74)
    layers = [
        ("ODS", "ods_user_behavior", "原始数据，未清洗，含脏数据"),
        ("DWD", "dwd_user_behavior_di", "清洗去重后的明细，粒度=一次行为"),
        ("DWS", "dws_user_action_1d", "按用户+日期汇总，粒度=一个用户一天"),
        ("ADS", "ads_daily_overview", "面向报表，粒度=一天"),
    ]
    print(f"{'层':<6}{'表名':<28}{'行数':>10}  说明")
    print("-" * 74)
    for layer, table, desc in layers:
        cnt = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{layer:<6}{table:<28}{cnt:>10}  {desc}")
    print()

    ods = cur.execute("SELECT COUNT(*) FROM ods_user_behavior").fetchone()[0]
    dwd = cur.execute("SELECT COUNT(*) FROM dwd_user_behavior_di").fetchone()[0]
    print(f">> ODS -> DWD 减少了 {ods - dwd} 行（{((ods - dwd) / ods * 100 if ods else 0):.1f}%），"
          f"这就是'清洗'的量化结果。")
    print(f">> 面试时面试官问'清洗做了什么、效果如何'，你要能答出这个数字和原因。")
    print()


# ============================================================================
# 主流程
# ============================================================================

def main():
    # Windows 控制台默认是 GBK，导致 "✓" 这类字符打印时抛 UnicodeEncodeError。
    # 这里主动把标准输出/错误切到 UTF-8，避免用户必须自己设 PYTHONIOENCODING。
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass  # Python < 3.7 或流被重定向时忽略

    parser = argparse.ArgumentParser(description="零依赖四层数仓演示")
    parser.add_argument("--csv", help="真实数据 CSV 路径（天池 UserBehavior 格式）")
    parser.add_argument("--limit", type=int, default=None,
                        help="最多装入 ODS 的行数（用于大数据量先采样试跑）")
    parser.add_argument("--db", default=DB_PATH, help=f"输出数据库路径（默认 {DB_PATH}）")
    args = parser.parse_args()

    # 环境自检必须放在"删旧库"之前：
    # 否则 SQLite 版本过低时会先把用户上一次的数据库删掉、再报错退出。
    ver = sqlite3.sqlite_version_info
    if ver < (3, 25):
        print(f"[错误] 当前 SQLite 版本 {sqlite3.sqlite_version} 过低，不支持窗口函数。")
        print("       本项目需要 SQLite 3.25+（2018年9月后发布）。请升级 Python。")
        print("       （已终止，未改动任何现有文件）")
        sys.exit(1)

    print()
    print("=" * 74)
    print("  零依赖四层数仓  ODS -> DWD -> DWS -> ADS")
    print("=" * 74)
    print()

    if args.csv:
        if not os.path.exists(args.csv):
            print(f"[错误] 找不到文件：{args.csv}")
            sys.exit(1)
        csv_path = args.csv
        print(f"[数据] 使用真实数据：{csv_path}")
    else:
        csv_path = generate_sample_data()

    # 清理上次运行产物。除了主库，还要清掉可能残留的 journal/wal/shm，
    # 否则上次异常中断留下的 journal 会在本次连接时触发回滚。
    if os.path.exists(args.db):
        try:
            os.remove(args.db)
        except PermissionError:
            print(f"[错误] 无法删除已存在的 {args.db}，可能被其他程序占用")
            print("       （如 DB Browser for SQLite 正打开该文件）。请关闭后重试。")
            sys.exit(1)
        for suffix in ("-journal", "-wal", "-shm"):
            leftover = args.db + suffix
            if os.path.exists(leftover):
                try:
                    os.remove(leftover)
                except OSError:
                    pass

    conn = sqlite3.connect(args.db)
    print(f"[环境] Python {sys.version.split()[0]} / SQLite {sqlite3.sqlite_version}（支持窗口函数 OK）")
    print(f"[时区] 统一按 UTC+{TZ_OFFSET_HOURS} 划分自然日（显式声明，不依赖本机时区）\n")

    print("-" * 74)
    print("① 装载 ODS 层（贴源，不清洗）")
    print("-" * 74)
    load_csv_to_ods(conn, csv_path, limit=args.limit)

    print()
    print("-" * 74)
    print("② 构建 DWD 层（清洗：空值 / 值域 / 时间越界 / 去重）")
    print("-" * 74)
    build_dwd(conn)

    print()
    print("-" * 74)
    print("③ 构建 DWS 层（按用户+日期沉淀公共指标）")
    print("-" * 74)
    build_dws(conn)

    print()
    print("-" * 74)
    print("④ 构建 ADS 层（面向报表的成品指标）")
    print("-" * 74)
    build_ads(conn)

    show_layers(conn)
    show_results(conn)

    print("=" * 74)
    print(f"完成。数据库已保存到：{os.path.abspath(args.db)}")
    print("=" * 74)
    print()
    print("下一步（重要）：")
    print("  1. 打开 dw_demo.db，自己写几条查询验证指标算得对不对")
    print("  2. 把这份脚本的 SQL 逐段看懂——面试要问的就是这些 SQL 的思路")
    print("  3. 换成真实天池数据跑一遍，记录真实的数据量级和耗时")
    print("  4. 把它整理成 GitHub 仓库，配上架构图和指标口径说明")
    print("     （模板见《项目实操指南》第五节）")
    print()

    conn.close()


if __name__ == "__main__":
    main()
