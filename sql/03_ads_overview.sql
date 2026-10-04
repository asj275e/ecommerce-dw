-- =============================================================================
--  ADS 应用层：每日 PV / UV / 购买转化率
--  来源：从 dw_pipeline.py 的 ADS_OVERVIEW 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
