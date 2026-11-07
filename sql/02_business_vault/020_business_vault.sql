USE CATALOG IDENTIFIER(:catalog);

-- Règles métier dérivées (recalculables) : séance, cluster, sentiment, catégorie, CAR
CREATE TABLE IF NOT EXISTS business_vault.bv_article_enriched (
  hk_article STRING, published_utc TIMESTAMP, updated_utc TIMESTAMP, first_seen_utc TIMESTAMP, available_utc TIMESTAMP,
  cluster_id STRING, is_first BOOLEAN, dup_rank INT, cluster_size INT,
  session_type STRING, session_date DATE, reaction_start_utc TIMESTAMP,
  positive DOUBLE, negative DOUBLE, neutral DOUBLE, sentiment_score DOUBLE, sentiment_label STRING, category STRING,
  model_version STRING, load_ts TIMESTAMP DEFAULT current_timestamp())
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS business_vault.bv_event_car (
  hk_article STRING, hk_security STRING, window_start_min INT, window_end_min INT, car DOUBLE, t_stat DOUBLE, p_value DOUBLE,
  alpha DOUBLE, beta DOUBLE, n_est INT, model_version STRING, load_ts TIMESTAMP);

CREATE TABLE IF NOT EXISTS business_vault.bv_anomaly (hk_security STRING, bar_ts TIMESTAMP, anomaly_score DOUBLE, anomaly_type STRING, model_version STRING);

-- Facteurs d'ajustement quotidiens (recalculés depuis sat_corporate_action + clôtures brutes)
CREATE OR REPLACE VIEW business_vault.v_adjustment_factor AS
WITH daily AS (SELECT hk_security, to_date(from_utc_timestamp(bar_ts,'America/New_York')) d, last(close) c FROM vault.nhsat_bar_1m GROUP BY 1,2),
ev AS (
  SELECT a.hk_security, a.ex_date d,
         CASE WHEN a.action='split' THEN 1.0/a.value
              WHEN a.action='dividend' THEN 1.0 - a.value / nullif(lag(c) OVER (PARTITION BY a.hk_security ORDER BY a.ex_date),0) END f
  FROM vault.sat_corporate_action a LEFT JOIN daily y ON y.hk_security=a.hk_security AND y.d=a.ex_date)
SELECT hk_security, d, coalesce(exp(sum(ln(f)) OVER (PARTITION BY hk_security ORDER BY d DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),1.0) adj_factor
FROM (SELECT hk_security, d, f FROM ev UNION ALL SELECT hk_security, d, 1.0 FROM daily);

-- Table PIT : état « tel que connu » à chaque instant -> aucun biais d'anticipation
CREATE OR REPLACE VIEW business_vault.pit_article AS
SELECT e.hk_article, e.available_utc, c.headline, c.url, e.sentiment_score, e.category, e.cluster_id, e.is_first
FROM business_vault.bv_article_enriched e JOIN vault.v_article_latest c USING (hk_article);
