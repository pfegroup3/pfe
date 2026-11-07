# PFE — News-Market Platform
Corrélation entre actualités financières et dynamiques boursières — Azure Databricks · Médaillon · Data Vault 2.0.

## 1. Architecture

```
Sources batch  (Finnhub, Alpaca REST, EDGAR 8-K, yfinance, GDELT, Reddit)  ──► jobs Python (idempotents, watermark, retry)  ─┐
Sources flux   (Alpaca News WS, Alpaca Bars 1m WS) ─► streamer (Docker/ACA) ─► Event Hubs (Kafka) ─► DLT streaming ─────────┤
                                                                                                                           ▼
 BRONZE (brut, append-only, hash)  ─►  SILVER (typé, UTC, contrôles)  ─►  RAW VAULT (hubs/links/sats)  ─►  BUSINESS VAULT  ─►  GOLD  ─►  modèle sémantique ─► Power BI
 bronze.raw_events/raw_bars/rejects     silver_news / silver_bars_1m       hub_article, hub_security,      bv_article_enriched   fact_news_impact     (DirectQuery
                                                                           link_article_security,          bv_event_car          fact_bar_1m          SQL Warehouse)
                                                                           sat_article_content (hashdiff)  bv_anomaly            dim_*, fact_*
                                                                           nhsat_bar_1m, sat_corporate_action  pit_article
 OPS : run_log · watermarks · freshness · api_quota · incidents          ML : MLflow (champion/challenger) · drift (PSI)
```

**Traçabilité** : `gold.fact_news_impact.hk_article → vault.sat_article_content.bronze_ref/bronze_record_hash → bronze.raw_events.payload` (JSON d'origine) + `url` de l'article.

## 2. Plan de réalisation (14 semaines)

| Phase | Sem. | Livrables | Critères d'acceptation |
|---|---|---|---|
| 0. Cadrage & infra | 1 | Dépôt, Terraform/Portail Azure (Databricks, ADLS, Key Vault, Event Hubs), Unity Catalog (`pfe_dev/stg/prod`), service principal, secrets | `databricks bundle validate` OK ; secrets via scope `pfe` |
| 1. Ingestion batch | 2-3 | `ingestion/sources.py`, Bronze, `ops.*`, retry/quota/quarantaine | Rejeu d'un même lot = 0 doublon ; quota simulé → statut `QUOTA`, reprise au watermark |
| 2. Streaming | 3-4 | `streamer.py`, DLT `dlt_stream.py`, Silver | Latence news→Silver < 2 min ; coupure réseau → reprise sans perte |
| 3. Data Vault + temps | 5-6 | `sql/01_vault`, `timeutil.py`, ajustement prix, PIT | Tests DST/férié/week-end ; split 2:1 sans saut ; aucune jointure avec `ingested_utc > T` |
| 4. Valeur métier | 6-7 | dédup MinHash, association ticker, sentiment, catégorie | ≥ 95 % des reprises isolées sur échantillon annoté |
| 5. ML | 8-11 | Modèles 1, 2, 3 + baselines + MLflow | Validation chronologique ; FinBERT vs Finnhub sur ≥ 500 titres annotés ; CAR avec p-values |
| 6. Gold + Power BI | 11-13 | Vues Gold, modèle sémantique, 4 rapports (`docs/powerbi.md`) | Dictionnaire de données ; rafraîchissement auto 30 s sur le fil |
| 7. Production | 13-14 | CI/CD, alertes, runbook, tests de charge | CI verte ; alertes fraîcheur/échec/dérive actives ; déploiement prod approuvé |

**Décisions clés (anti-biais)** : 1) `available_utc = max(published, first_seen)` : jamais la mise à jour ; 2) estimation du modèle de marché **avant** la fenêtre d'événement ; 3) terciles de la cible calculés sur le **train** ; 4) `TimeSeriesSplit(gap)` + hold-out chronologique ; 5) prix **bruts** stockés, ajustés dans Gold ; 6) seule la **1re publication** (`is_first`) alimente le fait d'impact.

## 3. Démarrage

```bash
cp .env.example .env            # renseigner les clés
make install && make test
docker compose up -d --build    # Redpanda + MLflow + flux Alpaca (news, barres)
# Databricks
databricks bundle deploy -t dev --var="warehouse_id=<id>"
databricks bundle run init_schema -t dev --var="warehouse_id=<id>"
databricks bundle run batch_ingest -t dev --var="warehouse_id=<id>"
```

## 4. Points à valider avant production
- `business_vault.v_adjustment_factor` (SQL) doit être comparé au calcul de référence `transform/adjust.py` sur quelques titres (AAPL 2020, NVDA 2024).
- Le job `enrich` charge la fenêtre de 72 h en pandas : au-delà de quelques centaines de milliers d'articles, passer à `groupBy(date).applyInPandas` et GPU pour FinBERT.
- Charges SQL Vault : ajouter le chargement Finnhub/EDGAR/GDELT/Reddit (même patron que `alpaca_news`, `record_source` distinct).
- Les quotas gratuits (Finnhub 60 req/min, Alpaca IEX, GDELT) limitent l'univers de tickers : commencer par 10-50 titres.
- Respecter les conditions d'utilisation de chaque API (redistribution, Reddit, SEC User-Agent).

## 5. Orchestration Airflow
`airflow/dags/pfe_dags.py` pilote les jobs Databricks (`DatabricksRunNowOperator`, résolution par nom) :
`pfe_batch_5min`, `pfe_daily` (22h30 ET, lun-ven), `pfe_train_weekly`, `pfe_stream_watchdog` (alerte si le pipeline DLT s'arrête).
Les schedules Databricks sont en `PAUSED` pour éviter tout double déclenchement.
```bash
export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://:@adb-XXXX.azuredatabricks.net?token=<PAT>'
docker compose -f docker-compose.airflow.yml up -d     # UI : http://localhost:8081
```
En production : Azure Container Apps / AKS (ou Astronomer / MWAA), secrets via Key Vault, authentification service principal plutôt que PAT.
