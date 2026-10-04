-- =============================================================================
--  ADS 应用层：连续活跃 ≥3 天
--  来源：从 dw_pipeline.py 的 ADS_CONSECUTIVE 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
