"""
SQLite vs Spark 结果一致性对比

目的：
    用同一份数据，分别用 SQLite 和 Spark 跑同一套四层数仓逻辑，
    对比结果是否完全一致。

为什么这件事有价值：
    1. 验证 Spark 移植的正确性
    2. 「跨引擎结果一致」本身就是一个很强的交叉验证 ——
       两个完全不同的执行引擎算出同一个数，出错概率极低
    3. 能暴露两个引擎的语义差异（时区、整数除法、NULL 处理）

用法：
    python spark_compare.py                 # 默认 100 万行
    python spark_compare.py --limit 200000  # 指定行数
"""

import argparse
import importlib.util
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RAW_CSV = os.path.join(HERE, "UserBehavior.csv")

# ⚠️ Spark / Hadoop 在 Windows 上读不了中文路径
#    （和 Java 生态的编码问题同源）。
#    项目目录含中文（"数仓实习"），所以所有 Spark 读写文件都放到
#    D:\dsh\spark_work\ 这个纯 ASCII 路径下。
WORK_DIR = r"D:\dsh\spark_work"
TEST_CSV = os.path.join(WORK_DIR, "spark_test.csv")
# ⚠️ 文件名不能以 _ 或 . 开头！
#    Hadoop/Spark 默认把这类文件当隐藏/元数据文件跳过，
#    导致 "UNABLE_TO_INFER_SCHEMA: Unable to infer schema for CSV"。
#    这个坑排查起来很费时间，因为文件明明存在。
SPARK_DB = os.path.join(WORK_DIR, "spark_test.db")
RESULT_TXT = os.path.join(WORK_DIR, "compare_result.txt")


# ============================================================
# 载入 dw_pipeline，复用它的 SQL（保证和项目实际执行的完全一致）
# ============================================================
def load_pipeline_module():
    path = os.path.join(HERE, "dw_pipeline.py")
    spec = importlib.util.spec_from_file_location("dw_pipeline", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ============================================================
# Spark SQL 版本（对应 SQLite 的四层 SQL）
#
# 关键翻译点：
#   SQLite: date(ts,'unixepoch','+8 hours')
#   Spark : to_date(from_unixtime(ts))      ← 依赖会话时区 Asia/Shanghai
#
#   这是最容易错的地方：Spark 的 from_unixtime 默认按会话时区转换，
#   如果会话时区是 UTC，结果就和 SQLite 的 +8 hours 差 8 小时。
# ============================================================

SPARK_DWD_SQL = """
CREATE OR REPLACE TEMP VIEW dwd_user_behavior_di AS
WITH filtered AS (
    SELECT
        user_id,
        item_id,
        category_id,
        behavior,
        ts,
        from_unixtime(ts)              AS event_time,
        to_date(from_unixtime(ts))     AS event_date
    FROM ods_user_behavior
    WHERE user_id IS NOT NULL
      AND item_id IS NOT NULL
      AND behavior IN ('pv', 'buy', 'cart', 'fav')
      AND to_date(from_unixtime(ts)) BETWEEN DATE '2017-11-25' AND DATE '2017-12-03'
),
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, item_id, ts, behavior
            ORDER BY category_id
        ) AS rn
    FROM filtered
)
SELECT user_id, item_id, category_id, behavior, ts, event_time, event_date
FROM deduped
WHERE rn = 1
"""

SPARK_DWS_SQL = """
CREATE OR REPLACE TEMP VIEW dws_user_action_1d AS
SELECT
    event_date                                          AS dt,
    user_id,
    SUM(CASE WHEN behavior = 'pv'   THEN 1 ELSE 0 END)  AS pv_cnt,
    SUM(CASE WHEN behavior = 'cart' THEN 1 ELSE 0 END)  AS cart_cnt,
    SUM(CASE WHEN behavior = 'fav'  THEN 1 ELSE 0 END)  AS fav_cnt,
    SUM(CASE WHEN behavior = 'buy'  THEN 1 ELSE 0 END)  AS buy_cnt
FROM dwd_user_behavior_di
GROUP BY event_date, user_id
"""

SPARK_ADS_OVERVIEW_SQL = """
CREATE OR REPLACE TEMP VIEW ads_daily_overview AS
SELECT
    dt,
    SUM(pv_cnt)                                                  AS pv,
    COUNT(DISTINCT user_id)                                      AS uv,
    COUNT(DISTINCT CASE WHEN buy_cnt > 0 THEN user_id END)        AS buy_uv,
    ROUND(
        COUNT(DISTINCT CASE WHEN buy_cnt > 0 THEN user_id END) * 1.0
        / COUNT(DISTINCT user_id), 4
    )                                                             AS pv_to_buy
FROM dws_user_action_1d
GROUP BY dt
"""

SPARK_ADS_RETENTION_SQL = """
CREATE OR REPLACE TEMP VIEW ads_user_retention AS
SELECT
    a.dt,
    COUNT(DISTINCT a.user_id)                          AS active_uv,
    COUNT(DISTINCT b.user_id)                          AS retained_uv,
    ROUND(
        COUNT(DISTINCT b.user_id) * 1.0
        / COUNT(DISTINCT a.user_id), 4
    )                                                  AS retention_1d
FROM (SELECT DISTINCT dt, user_id FROM dws_user_action_1d) a
LEFT JOIN (SELECT DISTINCT dt, user_id FROM dws_user_action_1d) b
       ON a.user_id = b.user_id
      AND b.dt = date_add(a.dt, 1)
GROUP BY a.dt
"""


def prepare_test_csv(limit):
    """从原始 CSV 里取前 limit 行，写成测试文件（放到 ASCII 路径）"""
    os.makedirs(WORK_DIR, exist_ok=True)
    if os.path.exists(TEST_CSV):
        os.remove(TEST_CSV)
    n = 0
    print(f"[准备] 从 {os.path.basename(RAW_CSV)} 取前 {limit:,} 行 → {TEST_CSV}")
    with open(RAW_CSV, "r", encoding="utf-8", errors="replace", newline="") as fin:
        with open(TEST_CSV, "w", encoding="utf-8", newline="\n") as fout:
            for line in fin:
                if n >= limit:
                    break
                parts = line.rstrip("\r\n").split(",")
                if len(parts) < 5:
                    continue
                fout.write(",".join(parts[:5]) + "\n")
                n += 1
    print(f"[准备] 测试文件 {n:,} 行, {os.path.getsize(TEST_CSV)/1024/1024:.1f} MB")
    return n


def run_sqlite(pipeline):
    """用 SQLite 跑四层，返回每层行数和 ADS 结果"""
    print()
    print("=" * 74)
    print("  【引擎 A】SQLite")
    print("=" * 74)

    if os.path.exists(SPARK_DB):
        os.remove(SPARK_DB)
    conn = sqlite3.connect(SPARK_DB)
    conn.executescript("""
        CREATE TABLE ods_user_behavior (
            user_id INTEGER, item_id INTEGER, category_id INTEGER,
            behavior TEXT, ts INTEGER, dt TEXT
        );
    """)
    # 复用 pipeline 的装载逻辑
    pipeline.load_csv_to_ods(conn, TEST_CSV)

    ods_n = conn.execute("SELECT COUNT(*) FROM ods_user_behavior").fetchone()[0]

    conn.executescript(pipeline.DWD_SQL)
    dwd_n = conn.execute("SELECT COUNT(*) FROM dwd_user_behavior_di").fetchone()[0]

    conn.executescript(pipeline.DWS_SQL)
    dws_n = conn.execute("SELECT COUNT(*) FROM dws_user_action_1d").fetchone()[0]

    conn.executescript(pipeline.ADS_OVERVIEW)
    overview = conn.execute("""
        SELECT dt, pv, uv, buy_uv, pv_to_buy
        FROM ads_daily_overview ORDER BY dt
    """).fetchall()

    conn.executescript(pipeline.ADS_RETENTION)
    retention = conn.execute("""
        SELECT dt, active_uv, retained_uv, retention_1d
        FROM ads_user_retention ORDER BY dt
    """).fetchall()

    conn.close()

    print(f"  ODS={ods_n:,}  DWD={dwd_n:,}  DWS={dws_n:,}")
    return {
        "ods": ods_n, "dwd": dwd_n, "dws": dws_n,
        "overview": overview, "retention": retention,
    }


def run_spark(limit):
    """用 Spark 跑三层（ODS 直接用 CSV 读入）"""
    print()
    print("=" * 74)
    print("  【引擎 B】Spark 3.5.0（local 模式）")
    print("=" * 74)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("sqlite-vs-spark")
             .master("local[4]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8")
             # 关键：会话时区设成 Asia/Shanghai，
             # 这样 from_unixtime() 才会按 UTC+8 转换，与 SQLite 的 '+8 hours' 对齐
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.warehouse.dir",
                     os.path.join(WORK_DIR, "warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    print(f"  Spark 版本: {spark.version}")
    print(f"  会话时区  : {spark.conf.get('spark.sql.session.timeZone')}")

    # 读 CSV 到临时视图（ODS 层）
    df = (spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .option("mode", "PERMISSIVE")
          .csv(TEST_CSV)
          .toDF("user_id", "item_id", "category_id", "behavior", "ts"))

    # 显式转类型（等价于 SQLite 侧的 int() 转换）
    from pyspark.sql import functions as F
    df = (df
          .withColumn("user_id", F.col("user_id").cast("long"))
          .withColumn("item_id", F.col("item_id").cast("long"))
          .withColumn("category_id", F.col("category_id").cast("long"))
          .withColumn("behavior", F.trim(F.col("behavior")))
          .withColumn("ts", F.col("ts").cast("long")))
    df.createOrReplaceTempView("ods_user_behavior")

    ods_n = df.count()

    spark.sql(SPARK_DWD_SQL)
    dwd_n = spark.table("dwd_user_behavior_di").count()

    spark.sql(SPARK_DWS_SQL)
    dws_n = spark.table("dws_user_action_1d").count()

    spark.sql(SPARK_ADS_OVERVIEW_SQL)
    overview = [tuple(r) for r in
                spark.sql("SELECT dt, pv, uv, buy_uv, pv_to_buy "
                          "FROM ads_daily_overview ORDER BY dt").collect()]

    spark.sql(SPARK_ADS_RETENTION_SQL)
    retention = [tuple(r) for r in
                 spark.sql("SELECT dt, active_uv, retained_uv, retention_1d "
                           "FROM ads_user_retention ORDER BY dt").collect()]

    print(f"  ODS={ods_n:,}  DWD={dwd_n:,}  DWS={dws_n:,}")

    spark.stop()
    return {
        "ods": ods_n, "dwd": dwd_n, "dws": dws_n,
        "overview": overview, "retention": retention,
    }


def norm(v):
    """把日期/Decimal/float 统一成可比较的形式"""
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    try:
        return round(float(v), 6)
    except (TypeError, ValueError):
        return str(v)


def compare(a, b):
    print()
    print("=" * 74)
    print("  对比结果")
    print("=" * 74)
    ok = True

    print()
    print("  ── 各层行数 ──")
    for k, label in [("ods", "ODS"), ("dwd", "DWD"), ("dws", "DWS")]:
        same = a[k] == b[k]
        ok = ok and same
        print(f"    {label}: SQLite={a[k]:>10,}   Spark={b[k]:>10,}   "
              f"{'一致 OK' if same else '不一致 FAIL'}")

    for key, label, cols in [
        ("overview", "ads_daily_overview", ["dt", "pv", "uv", "buy_uv", "pv_to_buy"]),
        ("retention", "ads_user_retention", ["dt", "active_uv", "retained_uv", "retention_1d"]),
    ]:
        print()
        print(f"  ── {label} ──")
        ra, rb = a[key], b[key]
        if len(ra) != len(rb):
            print(f"    行数不一致: SQLite={len(ra)} Spark={len(rb)}  FAIL")
            ok = False
            continue
        diffs = 0
        for r1, r2 in zip(ra, rb):
            v1 = [norm(x) for x in r1]
            v2 = [norm(x) for x in r2]
            if v1 != v2:
                diffs += 1
                print(f"    [差异] SQLite={v1}")
                print(f"           Spark ={v2}")
        if diffs == 0:
            print(f"    {len(ra)} 行全部一致 OK")
            # 打印前 3 行样本
            for r in ra[:3]:
                print("      " + "  ".join(str(norm(x)) for x in r))
        else:
            print(f"    {diffs} 行不一致 FAIL")
            ok = False

    print()
    print("=" * 74)
    print(f"  总体结论: {'两个引擎结果完全一致 OK' if ok else '存在不一致，需排查 FAIL'}")
    print("=" * 74)
    return ok


class Tee:
    """同时写到控制台和文件，方便事后读取（避开 PowerShell 重定向的编码问题）"""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)

    def flush(self):
        for st in self.streams:
            st.flush()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    os.makedirs(WORK_DIR, exist_ok=True)
    log = open(RESULT_TXT, "w", encoding="utf-8")
    sys.stdout = Tee(sys.__stdout__, log)

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=1_000_000)
    args = ap.parse_args()

    if not os.path.exists(RAW_CSV):
        print(f"[错误] 找不到 {RAW_CSV}")
        return 1

    pipeline = load_pipeline_module()

    n = prepare_test_csv(args.limit)
    if n == 0:
        print("[错误] 测试文件为空")
        return 1

    a = run_sqlite(pipeline)
    b = run_spark(args.limit)

    ok = compare(a, b)
    print(f"\n完整输出已保存到: {RESULT_TXT}")
    log.close()
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
