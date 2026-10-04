-- =============================================================================
--  ADS 应用层：次日留存率
--  来源：从 dw_pipeline.py 的 ADS_RETENTION 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
