"""
    Tâches Databricks (python_wheel_task) : ingestion batch, enrichissement, ML, santé.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from pfe.common.config import get_settings

from pfe.ml.sentiment import ITEM_8K, FinBertScorer, categorize_8k, categorize_rules

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
            SELECT hk_article, headline, summary, published_utc, updated_utc, first_seen_utc, record_source
            FROM {cat}.vault.v_article_latest 
            WHERE published_utc >= TIMESTAMP'{since.isoformat()}'""").toPandas()
        st.received = len(pdf)
        if pdf.empty:
            return
        pdf["text"] = pdf["headline"].fillna("") + ". " + pdf["summary"].fillna("")
        d = cluster_articles(pdf)
        sess = build_sessions((since - timedelta(days=10)).date().isoformat(), (datetime.now(timezone.utc) + timedelta(days=14)).date().isoformat())
        d = attach_session(d, sess)
        # disponibilité : jamais avant la 1re vue réelle (évite le biais d'anticipation en backfill/rejeu)
        d["available_utc"] = d[["published_utc", "first_seen_utc"]].max(axis=1)

        score_cols = ["positive", "negative", "neutral", "sentiment_score", "sentiment_label"]
        # FinBERT : 1re publication de chaque groupe seulement, et pas les 8-K (leur titre n'a pas de contenu à noter)
        scorable = d["is_first"] & (d["record_source"] != "sec_8k")
        if scorable.any():
            sc = FinBertScorer().score(d.loc[scorable, "text"].tolist())
            sc.index = d.index[scorable]
            d = d.join(sc[score_cols])
        else:
            d = d.assign(**{c: float("nan") for c in score_cols[:4]}, sentiment_label=None)
        # les reprises héritent du score de la 1re publication de leur groupe
        d[score_cols] = d.groupby("cluster_id")[score_cols].transform("first")
        d["sentiment_label"] = d["sentiment_label"].fillna("non_evalue")
        d[score_cols[:4]] = d[score_cols[:4]].fillna(0.0)
        d["model_version"] = "finbert-prosus-v1"
        d["category"] = [
            (categorize_8k(h) if src == "sec_8k" else None) or categorize_rules(t)
            for h, t, src in zip(d["headline"], d["text"], d["record_source"])
        ]
        d["dup_rank"] = d["dup_rank"].astype("int32")
        d["cluster_size"] = d["cluster_size"].astype("int32")
        st.kept = len(d)

        cols = ["hk_article", "published_utc", "updated_utc", "first_seen_utc", "available_utc",
                    "cluster_id", "is_first", "dup_rank", "cluster_size", "session_type", "session_date",
                    "reaction_start_utc", "positive", "negative", "neutral", "sentiment_score",
                    "sentiment_label", "category", "model_version"]
        spark.createDataFrame(d[cols]).createOrReplaceTempView("_enriched")

        upd = ", ".join(f"t.{c} = s.{c}" for c in cols)
        ins_cols = ", ".join(cols + ["load_ts"])
        ins_vals = ", ".join([f"s.{c}" for c in cols] + ["current_timestamp()"])
        spark.sql(f"""MERGE INTO {cat}.business_vault.bv_article_enriched t USING _enriched s
                        ON t.hk_article = s.hk_article AND t.model_version = s.model_version
                        WHEN MATCHED THEN UPDATE SET {upd}, t.load_ts = current_timestamp()
                        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})""")


def train(experiment: str = "/Shared/pfe/event_impact") -> None:
    from pfe.ml.event_classifier import train_and_log

    spark = _spark()
    df = spark.table(f"{get_settings().catalog}.gold.v_training_set").toPandas()
    if len(df) < 300:
        raise RuntimeError(
            f"Jeu d'entraînement insuffisant : {len(df)} lignes (minimum technique 300, "
            "utile à partir de 1000). Vérifiez bv_event_car et le volume d'articles."
        )
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

def compute_car(lookback_days: int = 60, market: str = "SPY") -> None:
    """CAR [-15, +60] barres par (article signal, ticker), marché = SPY. Écrit business_vault.bv_event_car."""
    import numpy as np
    import pandas as pd

    from pfe.common.runlog import run_log
    from pfe.transform.abnormal import event_car

    cat, spark = get_settings().catalog, _spark()
    with run_log(spark, cat, "compute_car") as st:
        ev = spark.sql(f"""
            SELECT e.hk_article, l.hk_security, h.bk_symbol AS symbol, e.reaction_start_utc
            FROM {cat}.business_vault.bv_article_enriched e
            JOIN {cat}.vault.link_article_security l ON l.hk_article = e.hk_article
            JOIN {cat}.vault.hub_security h ON h.hk_security = l.hk_security
            LEFT ANTI JOIN {cat}.business_vault.bv_event_car c
              ON c.hk_article = e.hk_article AND c.hk_security = l.hk_security
            WHERE e.is_first AND e.reaction_start_utc IS NOT NULL""").toPandas()
        bars = spark.sql(f"""
            SELECT symbol, bar_ts, close FROM {cat}.silver.v_bars_all
            WHERE bar_ts >= current_timestamp() - INTERVAL {int(lookback_days)} DAYS""").toPandas()
        st.received = len(ev)
        bars["bar_ts"] = pd.to_datetime(bars["bar_ts"], utc=True)

        def ret(sym: str):
            b = bars[bars.symbol == sym].drop_duplicates("bar_ts").set_index("bar_ts")["close"].sort_index()
            return np.log(b).diff().dropna() if len(b) > 1 else None

        mkt = ret(market)
        cache, rows = {}, []
        if mkt is not None:
            for r in ev.itertuples():
                if r.symbol == market:
                    continue
                if r.symbol not in cache:
                    cache[r.symbol] = ret(r.symbol)
                if cache[r.symbol] is None:
                    continue
                t0 = pd.Timestamp(r.reaction_start_utc)
                t0 = t0.tz_localize("UTC") if t0.tzinfo is None else t0.tz_convert("UTC")
                res = event_car(cache[r.symbol], mkt, t0)
                if res:
                    rows.append({"hk_article": r.hk_article, "hk_security": r.hk_security,
                                 "window_start_min": -15, "window_end_min": 60,
                                 **{k: res[k] for k in ("car", "t_stat", "p_value", "alpha", "beta", "n_est")},
                                 "model_version": "market-model-ols-v1"})
        st.kept, st.rejected = len(rows), len(ev) - len(rows)
        if not rows:
            return
        out = pd.DataFrame(rows)
        for c in ("window_start_min", "window_end_min", "n_est"):
            out[c] = out[c].astype("int32")
        out["load_ts"] = pd.Timestamp.now(tz="UTC")
        spark.createDataFrame(out).write.mode("append").saveAsTable(f"{cat}.business_vault.bv_event_car")
