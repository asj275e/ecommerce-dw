-- =============================================================================
--  ADS 应用层：商品浏览 TopN
--  来源：从 dw_pipeline.py 的 ADS_ITEM_TOP10 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
