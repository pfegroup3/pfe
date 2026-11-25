USE CATALOG IDENTIFIER(:catalog);

-- 1. Tables Silver alimentées par les connecteurs batch (historique)
CREATE TABLE IF NOT EXISTS silver.silver_news_batch (
  article_id BIGINT, headline STRING, summary STRING, source_name STRING, url STRING,
  symbols ARRAY<STRING>, published_utc TIMESTAMP, updated_utc TIMESTAMP,
  first_seen_utc TIMESTAMP, ingested_utc TIMESTAMP, record_hash STRING, bronze_ref STRING);

CREATE TABLE IF NOT EXISTS silver.silver_bars_1m_batch (
  symbol STRING, bar_ts TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
  volume DOUBLE, vwap DOUBLE, trade_count DOUBLE, ingested_utc TIMESTAMP);

-- 2. Chargement depuis Bronze
MERGE INTO silver.silver_news_batch t USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT CAST(get_json_object(payload, '$.id') AS BIGINT) AS article_id,
           get_json_object(payload, '$.headline') AS headline,
           get_json_object(payload, '$.summary') AS summary,
           get_json_object(payload, '$.source') AS source_name,
           get_json_object(payload, '$.url') AS url,
           from_json(get_json_object(payload, '$.symbols'), 'array<string>') AS symbols,
           published_utc, updated_utc,
           CASE WHEN min(ingested_utc) OVER (PARTITION BY record_id) > published_utc + INTERVAL 2 HOURS
                THEN published_utc -- rattrapage historique : on ne sait pas quand l'article a été vu
                ELSE min(ingested_utc) OVER (PARTITION BY record_id) END AS first_seen_utc,
           ingested_utc, record_hash,
           concat_ws(':', source, record_id, record_hash) AS bronze_ref,
           row_number() OVER (PARTITION BY record_id, updated_utc ORDER BY ingested_utc) AS rn
    FROM bronze.raw_events WHERE source = 'alpaca_news') WHERE rn = 1) s
ON t.article_id = s.article_id AND t.updated_utc <=> s.updated_utc
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO silver.silver_bars_1m_batch t USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT get_json_object(payload, '$._symbol') AS symbol, published_utc AS bar_ts,
           CAST(get_json_object(payload, '$.o') AS DOUBLE) AS open,
           CAST(get_json_object(payload, '$.h') AS DOUBLE) AS high,
           CAST(get_json_object(payload, '$.l') AS DOUBLE) AS low,
           CAST(get_json_object(payload, '$.c') AS DOUBLE) AS close,
           CAST(get_json_object(payload, '$.v') AS DOUBLE) AS volume,
           CAST(get_json_object(payload, '$.vw') AS DOUBLE) AS vwap,
           CAST(get_json_object(payload, '$.n') AS DOUBLE) AS trade_count,
           ingested_utc,
           row_number() OVER (PARTITION BY record_id ORDER BY ingested_utc DESC) AS rn
    FROM bronze.raw_bars WHERE source = 'alpaca_bars_1m') WHERE rn = 1) s
ON t.symbol = s.symbol AND t.bar_ts = s.bar_ts
WHEN NOT MATCHED THEN INSERT *;

-- 3. Vues qui réunissent le flux en direct (DLT) et le batch
CREATE OR REPLACE VIEW silver.v_news_all AS
SELECT article_id, headline, summary, source_name, url, symbols, published_utc, updated_utc,
       first_seen_utc, ingested_utc, record_hash, bronze_ref
FROM (SELECT *, row_number() OVER (PARTITION BY article_id, updated_utc ORDER BY prio, ingested_utc) rn
      FROM (SELECT article_id, headline, summary, source_name, url, symbols, published_utc, updated_utc,
                   first_seen_utc, ingested_utc, record_hash, bronze_ref, 1 AS prio FROM silver.silver_news
            UNION ALL
            SELECT article_id, headline, summary, source_name, url, symbols, published_utc, updated_utc,
                   first_seen_utc, ingested_utc, record_hash, bronze_ref, 2 AS prio FROM silver.silver_news_batch))
WHERE rn = 1;

CREATE OR REPLACE VIEW silver.v_bars_all AS
SELECT symbol, bar_ts, open, high, low, close, volume, vwap, trade_count, ingested_utc
FROM (SELECT *, row_number() OVER (PARTITION BY symbol, bar_ts ORDER BY prio, ingested_utc DESC) rn
      FROM (SELECT symbol, bar_ts, open, high, low, close, volume, vwap, trade_count, ingested_utc, 1 AS prio FROM silver.silver_bars_1m
            UNION ALL
            SELECT symbol, bar_ts, open, high, low, close, volume, vwap, trade_count, ingested_utc, 2 AS prio FROM silver.silver_bars_1m_batch))
WHERE rn = 1;


CREATE TABLE IF NOT EXISTS silver.silver_filings_8k (
  accession STRING, cik STRING, symbol STRING, items STRING, description STRING, url STRING,
  published_utc TIMESTAMP, ingested_utc TIMESTAMP, record_hash STRING, bronze_ref STRING);

MERGE INTO silver.silver_filings_8k t USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT get_json_object(payload, '$.accessionNumber') AS accession,
           get_json_object(payload, '$._cik') AS cik,
           get_json_object(payload, '$._symbol') AS symbol,
           get_json_object(payload, '$.items') AS items,
           get_json_object(payload, '$.primaryDocDescription') AS description,
           concat('https://www.sec.gov/Archives/edgar/data/',
                  CAST(CAST(get_json_object(payload, '$._cik') AS BIGINT) AS STRING), '/',
                  replace(get_json_object(payload, '$.accessionNumber'), '-', ''), '/',
                  get_json_object(payload, '$.primaryDocument')) AS url,
           published_utc, ingested_utc, record_hash,
           concat_ws(':', source, record_id, record_hash) AS bronze_ref,
           row_number() OVER (PARTITION BY record_id ORDER BY ingested_utc) AS rn
    FROM bronze.raw_events WHERE source = 'sec_8k') WHERE rn = 1 AND accession IS NOT NULL) s
ON t.accession = s.accession AND t.symbol = s.symbol
WHEN NOT MATCHED THEN INSERT *;

CREATE TABLE IF NOT EXISTS silver.silver_news_finnhub (
  article_id BIGINT, symbol STRING, headline STRING, summary STRING, source_name STRING, url STRING,
  published_utc TIMESTAMP, first_seen_utc TIMESTAMP, ingested_utc TIMESTAMP,
  record_hash STRING, bronze_ref STRING);

MERGE INTO silver.silver_news_finnhub t USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT try_cast(get_json_object(payload, '$.id') AS BIGINT) AS article_id,
           upper(trim(get_json_object(payload, '$._symbol'))) AS symbol,
           get_json_object(payload, '$.headline') AS headline,
           get_json_object(payload, '$.summary') AS summary,
           get_json_object(payload, '$.source') AS source_name,
           get_json_object(payload, '$.url') AS url,
           published_utc,
           published_utc AS first_seen_utc,   -- Finnhub ne donne pas l'heure de première vue : publication = disponibilité
           ingested_utc, record_hash,
           concat_ws(':', source, record_id, record_hash) AS bronze_ref,
           row_number() OVER (PARTITION BY record_id ORDER BY ingested_utc) AS rn
    FROM bronze.raw_events WHERE source = 'finnhub_news')
  WHERE rn = 1 AND article_id IS NOT NULL AND symbol IS NOT NULL
    AND headline IS NOT NULL AND length(trim(headline)) > 0
    AND published_utc <= current_timestamp() + INTERVAL 5 MINUTES) s
ON t.article_id = s.article_id AND t.symbol = s.symbol
WHEN NOT MATCHED THEN INSERT *;
