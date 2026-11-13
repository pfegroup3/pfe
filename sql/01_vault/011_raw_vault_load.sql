USE CATALOG IDENTIFIER(:catalog);

-- ===== CHARGEMENTS (idempotents) =====
MERGE INTO vault.hub_article t USING (
  SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) hk_article,
         concat('alpaca_news|', cast(article_id AS STRING)) bk_article, min(ingested_utc) load_ts, 'alpaca_news' record_source
  FROM silver.silver_news GROUP BY article_id) s
ON t.hk_article = s.hk_article WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.sat_article_content t USING (
  SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) hk_article, current_timestamp() load_ts,
         sha2(concat_ws('|', headline, summary, url, cast(updated_utc AS STRING)), 256) hashdiff,
         headline, summary, url, source_name, published_utc, updated_utc, first_seen_utc, ingested_utc,
         record_hash bronze_record_hash, bronze_ref, 'alpaca_news' record_source
  FROM silver.silver_news) s
ON t.hk_article = s.hk_article AND t.hashdiff = s.hashdiff WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.hub_security t USING (
  SELECT DISTINCT sha2(upper(trim(sym)), 256) hk_security, upper(trim(sym)) bk_symbol, current_timestamp() load_ts, 'alpaca_news' record_source
  FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym) s
ON t.hk_security = s.hk_security WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.link_article_security t USING (
  SELECT DISTINCT sha2(concat_ws('|', ha, hs), 256) hk_link, ha hk_article, hs hk_security, current_timestamp() load_ts, 'alpaca_news' record_source
  FROM (SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) ha, sha2(upper(trim(sym)), 256) hs
        FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym)) s
ON t.hk_link = s.hk_link WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.nhsat_bar_1m t USING (
  SELECT sha2(upper(trim(symbol)), 256) hk_security, bar_ts, open, high, low, close, volume, vwap, trade_count, current_timestamp() load_ts, 'alpaca_bars' record_source
  FROM silver.silver_bars_1m) s
ON t.hk_security = s.hk_security AND t.bar_ts = s.bar_ts WHEN NOT MATCHED THEN INSERT *;

-- Vue : dernière version de chaque article (utilisée par le job d'enrichissement)
CREATE OR REPLACE VIEW vault.v_article_latest AS
SELECT * EXCEPT (rn) FROM (SELECT *, row_number() OVER (PARTITION BY hk_article ORDER BY updated_utc DESC, load_ts DESC) rn FROM vault.sat_article_content) WHERE rn = 1;