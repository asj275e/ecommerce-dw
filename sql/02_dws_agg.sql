-- =============================================================================
--  DWS 汇总层：按用户+日期沉淀原子指标
--  来源：从 dw_pipeline.py 的 DWS_SQL 常量自动抽取，与实际执行语句一致
--  数据库：SQLite 3.25+（需支持窗口函数）
-- =============================================================================

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
