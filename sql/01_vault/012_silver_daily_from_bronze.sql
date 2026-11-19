USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS silver.silver_prices_daily (
  symbol STRING, session_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
  adj_close DOUBLE, volume DOUBLE, ingested_utc TIMESTAMP);
CREATE TABLE IF NOT EXISTS silver.silver_corporate_actions (
  symbol STRING, ex_date DATE, action STRING, value DOUBLE, ingested_utc TIMESTAMP);
CREATE TABLE IF NOT EXISTS silver.silver_macro_news (
  url STRING, title STRING, domain STRING, language STRING, published_utc TIMESTAMP, ingested_utc TIMESTAMP);

-- Lecture typée des lignes Yahoo (dernière version de chaque jour)
CREATE OR REPLACE VIEW silver.v_yahoo_parsed AS
SELECT symbol, session_date, open, high, low, close, adj_close, volume, dividends, splits, ingested_utc
FROM (
  SELECT get_json_object(payload, '$._symbol') AS symbol,
         to_date(from_utc_timestamp(published_utc, 'America/New_York')) AS session_date,
         CAST(get_json_object(payload, '$.Open') AS DOUBLE) AS open,
         CAST(get_json_object(payload, '$.High') AS DOUBLE) AS high,
         CAST(get_json_object(payload, '$.Low') AS DOUBLE) AS low,
         CAST(get_json_object(payload, '$.Close') AS DOUBLE) AS close,
         CAST(get_json_object(payload, '$[\'Adj Close\']') AS DOUBLE) AS adj_close,
         CAST(get_json_object(payload, '$.Volume') AS DOUBLE) AS volume,
         coalesce(CAST(get_json_object(payload, '$.Dividends') AS DOUBLE), 0) AS dividends,
         coalesce(CAST(get_json_object(payload, '$[\'Stock Splits\']') AS DOUBLE), 0) AS splits,
         ingested_utc,
         row_number() OVER (PARTITION BY record_id ORDER BY ingested_utc DESC) AS rn
  FROM bronze.raw_bars WHERE source = 'yahoo_daily')
WHERE rn = 1;

-- Prix quotidiens : seulement les lignes valides
MERGE INTO silver.silver_prices_daily t USING (
  SELECT symbol, session_date, open, high, low, close, adj_close, volume, ingested_utc
  FROM silver.v_yahoo_parsed
  WHERE symbol IS NOT NULL AND close > 0 AND open > 0 AND high >= low AND volume >= 0) s
ON t.symbol = s.symbol AND t.session_date = s.session_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Fractionnements et dividendes
MERGE INTO silver.silver_corporate_actions t USING (
  SELECT symbol, session_date AS ex_date, 'split' AS action, splits AS value, ingested_utc
  FROM silver.v_yahoo_parsed WHERE splits > 0
  UNION ALL
  SELECT symbol, session_date, 'dividend', dividends, ingested_utc
  FROM silver.v_yahoo_parsed WHERE dividends > 0) s
ON t.symbol = s.symbol AND t.ex_date = s.ex_date AND t.action = s.action
WHEN NOT MATCHED THEN INSERT *;

-- GDELT : actualité macro, anglaise uniquement, sans doublon d'URL
MERGE INTO silver.silver_macro_news t USING (
  SELECT url, title, domain, language, published_utc, ingested_utc FROM (
    SELECT get_json_object(payload, '$.url') AS url,
           get_json_object(payload, '$.title') AS title,
           get_json_object(payload, '$.domain') AS domain,
           get_json_object(payload, '$.language') AS language,
           published_utc, ingested_utc,
           row_number() OVER (PARTITION BY record_id ORDER BY ingested_utc DESC) AS rn
    FROM bronze.raw_events WHERE source = 'gdelt')
  WHERE rn = 1 AND url IS NOT NULL AND title IS NOT NULL AND length(trim(title)) > 0) s
ON t.url = s.url
WHEN NOT MATCHED THEN INSERT *;

-- Vault : fractionnements et dividendes (alimente l'ajustement des prix)
MERGE INTO vault.sat_corporate_action t USING (
  SELECT sha2(upper(trim(symbol)), 256) AS hk_security, ex_date, action, value,
         current_timestamp() AS load_ts, 'yahoo_daily' AS record_source
  FROM silver.silver_corporate_actions) s
ON t.hk_security = s.hk_security AND t.ex_date = s.ex_date AND t.action = s.action
WHEN NOT MATCHED THEN INSERT *;