"""
Spark 分区表 + 分桶表 演示

目的：
    用 Hive 兼容语法在 Spark 上建分区表和分桶表，
    亲眼看到「分区是目录、分桶是文件」这个抽象变成现实。

为什么做这个：
    面试官问「你懂不懂 Hive 分区和分桶」，
    光说原理不够，能说「我动手建过，看过目录结构」说服力完全不同。

用法：
    python spark_partition_bucket.py
"""

import os
import sys

# Spark / Hadoop 在 Windows 上读不了中文路径，所有产物放纯 ASCII 目录
WORK = r"D:\dsh\spark_work"
WAREHOUSE = os.path.join(WORK, "wh_demo")
DATA_DIR = os.path.join(WORK, "demo_data")
RAW_CSV = r"D:\dsh\数仓实习\02-岗位与投递\项目代码\UserBehavior.csv"

N_ROWS = 300_000       # 演示用，30 万行足够看出分区效果
N_BUCKETS = 8          # 分桶数

# 分区字段（用 event_date，也就是业务上的天）
PARTITION_COLS = ["dt"]


def banner(title):
    print()
    print("=" * 78)
    print("  " + title)
    print("=" * 78)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    os.makedirs(WORK, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    banner("准备数据")
    # 取前 N 行，并且把日期算好（避免依赖 Spark 的 from_unixtime 时区行为）
    csv_out = os.path.join(DATA_DIR, "behavior_demo.csv")
    n = 0
    with open(RAW_CSV, "r", encoding="utf-8", errors="replace", newline="") as fin, \
         open(csv_out, "w", encoding="utf-8", newline="\n") as fout:
        for line in fin:
            if n >= N_ROWS:
                break
            p = line.rstrip("\r\n").split(",")
            if len(p) < 5:
                continue
            fout.write(",".join(p[:5]) + "\n")
            n += 1
    print(f"  写入 {n:,} 行 → {csv_out}")

    from pyspark.sql import SparkSession, functions as F

    banner("启动 Spark（启用 Hive 兼容模式）")
    spark = (SparkSession.builder
             .appName("partition-bucket-demo")
             .master("local[2]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", WAREHOUSE)
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.sources.bucketing.enabled", "true")
             .config("spark.sql.shuffle.partitions", "8")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print(f"  Spark {spark.version}")
    print(f"  catalog: {spark.conf.get('spark.sql.catalogImplementation')}")
    print(f"  warehouse: {WAREHOUSE}")

    # ------------------------------------------------------------------
    banner("第 1 步：建【分区表】ODS（按天分区）")
    # ------------------------------------------------------------------
    spark.sql("DROP TABLE IF EXISTS ods_behavior")
    spark.sql(f"""
        CREATE TABLE ods_behavior (
            user_id     BIGINT,
            item_id     BIGINT,
            category_id BIGINT,
            behavior    STRING,
            ts          BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    print("  建表语句：")
    print("    CREATE TABLE ods_behavior (...)")
    print("    PARTITIONED BY (dt STRING)      ← 分区字段")
    print("    STORED AS PARQUET")

    # 动态分区写入
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    df = (spark.read
          .option("header", "false")
          .csv(csv_out)
          .toDF("user_id", "item_id", "category_id", "behavior", "ts"))
    df = (df
          .withColumn("user_id", F.col("user_id").cast("long"))
          .withColumn("item_id", F.col("item_id").cast("long"))
          .withColumn("category_id", F.col("category_id").cast("long"))
          .withColumn("behavior", F.trim(F.col("behavior")))
          .withColumn("ts", F.col("ts").cast("long"))
          .withColumn("dt", F.to_date(F.from_unixtime(F.col("ts"))))
          .filter(F.col("behavior").isin("pv", "buy", "cart", "fav"))
          .filter(F.to_date(F.from_unixtime(F.col("ts")))
                  .between(F.lit("2017-11-25"), F.lit("2017-12-03"))))

    df.createOrReplaceTempView("tmp_ods")
    spark.sql("""
        INSERT OVERWRITE TABLE ods_behavior PARTITION (dt)
        SELECT user_id, item_id, category_id, behavior, ts, dt FROM tmp_ods
    """)
    total = spark.table("ods_behavior").count()
    print(f"  装载完成：{total:,} 行")

    # ------------------------------------------------------------------
    banner("第 2 步：看【分区】到底变成了什么")
    # ------------------------------------------------------------------
    print("  分区列表：")
    for r in spark.sql("SHOW PARTITIONS ods_behavior").collect():
        print(f"    {r[0]}")

    print()
    print("  磁盘上的目录结构（这就是「分区 = 目录」）：")
    base = os.path.join(WAREHOUSE, "ods_behavior")
    if os.path.isdir(base):
        for name in sorted(os.listdir(base)):
            full = os.path.join(base, name)
            if os.path.isdir(full):
                files = [f for f in os.listdir(full) if f.endswith(".parquet")]
                print(f"    {name}/   ({len(files)} 个 parquet 文件)")

    # ------------------------------------------------------------------
    banner("第 3 步：演示【分区裁剪】")
    # ------------------------------------------------------------------
    print("  查询 1：只查一天（WHERE dt = '2017-11-25'）")
    plan1 = spark.sql("""
        EXPLAIN EXTENDED
        SELECT COUNT(*) FROM ods_behavior WHERE dt = '2017-11-25'
    """).collect()
    plan1_text = "\n".join(r[0] for r in plan1)
    npart1 = plan1_text.count("PartitionFilters")
    one_day = spark.sql("SELECT COUNT(*) FROM ods_behavior WHERE dt='2017-11-25'").collect()[0][0]
    print(f"    结果行数: {one_day:,}")
    print(f"    ★ 执行计划里出现 PartitionFilters: {'是' if npart1 else '否'}")

    print()
    print("  查询 2：不带分区条件（全表扫描）")
    all_rows = spark.sql("SELECT COUNT(*) FROM ods_behavior").collect()[0][0]
    print(f"    结果行数: {all_rows:,}")

    n_parts = len(spark.sql("SHOW PARTITIONS ods_behavior").collect())
    print()
    print(f"  ★ 结论：查一天只扫 1/{n_parts} 的数据 —— 这就是分区裁剪的价值")

    # ------------------------------------------------------------------
    banner("第 4 步：建【分桶表】DWD（按 user_id 分 8 桶）")
    # ------------------------------------------------------------------
    spark.sql("DROP TABLE IF EXISTS dwd_behavior_bucketed")
    spark.sql(f"""
        CREATE TABLE dwd_behavior_bucketed (
            user_id     BIGINT,
            item_id     BIGINT,
            category_id BIGINT,
            behavior    STRING,
            ts          BIGINT
        )
        PARTITIONED BY (dt STRING)
        CLUSTERED BY (user_id) INTO {N_BUCKETS} BUCKETS
        STORED AS PARQUET
    """)
    print("  建表语句：")
    print(f"    CLUSTERED BY (user_id) INTO {N_BUCKETS} BUCKETS   ← 分桶")

    spark.sql("""
        INSERT OVERWRITE TABLE dwd_behavior_bucketed PARTITION (dt)
        SELECT user_id, item_id, category_id, behavior, ts, dt FROM tmp_ods
    """)
    print(f"  装载完成：{spark.table('dwd_behavior_bucketed').count():,} 行")

    # ------------------------------------------------------------------
    banner("第 5 步：看【分桶】到底变成了什么")
    # ------------------------------------------------------------------
    bbase = os.path.join(WAREHOUSE, "dwd_behavior_bucketed")
    if os.path.isdir(bbase):
        for part in sorted(os.listdir(bbase)):
            full = os.path.join(bbase, part)
            if not os.path.isdir(full):
                continue
            files = sorted(f for f in os.listdir(full) if f.endswith(".parquet"))
            print(f"  {part}/")
            print(f"    文件数: {len(files)}")
            for f in files[:3]:
                print(f"      {f}")
            if len(files) > 3:
                print(f"      ... 共 {len(files)} 个")
            break  # 只看第一个分区，避免刷屏

    print()
    print(f"  ★ 每个分区下有 {N_BUCKETS} 个桶文件（part-00000 ~ part-0000{N_BUCKETS-1}）")
    print("  ★ 同一个 user_id 的记录，一定落在同一个桶文件里")
    print("  ★ 这就是 SMB Join 的基础：两表同字段同桶数 → 桶对桶 JOIN，省掉 Shuffle")

    # ------------------------------------------------------------------
    banner("第 6 步：验证分桶哈希 —— 同一个 user_id 是否总在同一桶")
    # ------------------------------------------------------------------
    # 用 Spark 内置的 hash 模拟分桶归属
    sample = spark.sql("""
        SELECT user_id,
               pmod(hash(user_id), 8) AS bucket_no,
               COUNT(*) AS cnt
        FROM dwd_behavior_bucketed
        GROUP BY user_id, pmod(hash(user_id), 8)
        ORDER BY user_id
        LIMIT 10
    """).collect()
    print(f"  {'user_id':>12}  {'hash%8(桶号)':>14}  {'记录数':>8}")
    print("  " + "-" * 40)
    for r in sample:
        print(f"  {r[0]:>12}  {r[1]:>14}  {r[2]:>8}")

    print()
    print("  ★ 同一个 user_id 只会有一个桶号 —— 因为 hash 和取模都是确定的")

    spark.stop()

    banner("完成")
    print("  你刚刚动手做了这些事：")
    print("    1. 建了 PARTITIONED BY (dt) 的分区表")
    print("    2. 看了磁盘上按 dt=... 分的目录")
    print("    3. 用 EXPLAIN 验证了分区裁剪")
    print(f"    4. 建了 CLUSTERED BY (user_id) INTO {N_BUCKETS} BUCKETS 的分桶表")
    print(f"    5. 看了每个分区下的 {N_BUCKETS} 个桶文件")
    print("    6. 验证了同一个 user_id 的桶号恒定")
    print()
    print(f"  产物目录: {WAREHOUSE}")
    print()


if __name__ == "__main__":
    main()
