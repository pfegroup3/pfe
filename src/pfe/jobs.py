"""
    Tâches Databricks (python_wheel_task) : ingestion batch, enrichissement, ML, santé.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from pfe.common.config import get_settings

log = logging.getLogger(__name__)


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def ingest(source: str, lookback_days: int = 3) -> None:
    from pfe.ingestion.base import run_source
    from pfe.ingestion.sources import REGISTRY

    s = get_settings()
    start = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    run_source(_spark(), REGISTRY[source](s), default_start=start)


def enrich(lookback_hours: int = 72) -> None:
    """
        Silver -> Business Vault : dédoublonnage, séance, sentiment FinBERT, catégorie.
    """
    import pandas as pd

    from pfe.common.runlog import run_log
    from pfe.common.timeutil import attach_session, build_sessions
    from pfe.ml.sentiment import ITEM_8K, FinBertScorer
    from pfe.transform.dedup import cluster_articles

    s, spark = get_settings(), _spark()
    cat = s.catalog
    with run_log(spark, cat, "enrich") as st:
        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        pdf = spark.sql(f"""
            SELECT hk_article, headline, summary, published_utc, updated_utc, first_seen_utc
            FROM {cat}.vault.v_article_latest WHERE published_utc >= TIMESTAMP'{since.isoformat()}'""").toPandas()
        st.received = len(pdf)
        if pdf.empty:
            return
        pdf["text"] = pdf["headline"].fillna("") + ". " + pdf["summary"].fillna("")
        d = cluster_articles(pdf)
        sess = build_sessions((since - timedelta(days=10)).date().isoformat(), (datetime.now(timezone.utc) + timedelta(days=14)).date().isoformat())
        d = attach_session(d, sess)
        # disponibilité : jamais avant la 1re vue réelle (évite le biais d'anticipation en backfill/rejeu)
        d["available_utc"] = d[["published_utc", "first_seen_utc"]].max(axis=1)
        sc = FinBertScorer().score(d["text"].tolist())
        d = pd.concat([d.reset_index(drop=True), sc], axis=1)
        d["model_version"] = "finbert-prosus-v1"
        st.kept = len(d)
        spark.createDataFrame(d.drop(columns=["text"])).createOrReplaceTempView("_enriched")
        spark.sql(f"""MERGE INTO {cat}.business_vault.bv_article_enriched t USING _enriched s
                      ON t.hk_article = s.hk_article AND t.model_version = s.model_version
                      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def train(experiment: str = "/Shared/pfe/event_impact") -> None:
    from pfe.ml.event_classifier import train_and_log

    spark = _spark()
    df = spark.table(f"{get_settings().catalog}.gold.v_training_set").toPandas()
    res = train_and_log(df, experiment, f"{get_settings().catalog}.gold.event_magnitude")
    log.info(res)


def health() -> None:
    """Contrôles de fraîcheur -> table ops.freshness (alimente le rapport « État de la plateforme »)."""
    cat = get_settings().catalog
    _spark().sql(f"""
        INSERT INTO {cat}.ops.freshness
        SELECT current_timestamp(), source, max(published_utc), max(ingested_utc),
               (unix_timestamp(current_timestamp()) - unix_timestamp(max(ingested_utc)))/60.0
        FROM {cat}.bronze.raw_events GROUP BY source""")
