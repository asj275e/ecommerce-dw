-- =============================================================================
--  DWD 明细层：清洗 + 去重
--  来源：从 dw_pipeline.py 的 DWD_SQL 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
