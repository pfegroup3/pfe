from __future__ import annotations

import os
from dataclasses import dataclass, field


def _secret(name: str, default: str = "") -> str:
    """
        Env d'abord ; sinon secret scope Databricks 'pfe' si disponible.
    """
    from dotenv import load_dotenv
    load_dotenv()
    val = os.getenv(name)
    if val:
        return val
    try:  # pragma: no cover - uniquement sur Databricks
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate()).secrets.get("pfe", name)
    except Exception:
        return default

@dataclass(frozen=True)
class Settings:
    catalog: str = field(default_factory=lambda: os.getenv("PFE_CATALOG", "pfe_dev"))
    tickers: tuple[str, ...] = field(
        default_factory=lambda: tuple(t.strip() for t in os.getenv("TICKERS", "AAPL,MSFT").split(","))
    )
    finnhub_key: str = field(default_factory=lambda: _secret("FINNHUB_API_KEY"))
    alpaca_key: str = field(default_factory=lambda: _secret("ALPACA_KEY_ID"))
    alpaca_secret: str = field(default_factory=lambda: _secret("ALPACA_SECRET"))
    alpaca_feed: str = field(default_factory=lambda: os.getenv("ALPACA_FEED", "iex"))
    newsapi_key: str = field(default_factory=lambda: _secret("NEWSAPI_KEY"))
    sec_user_agent: str = field(default_factory=lambda: os.getenv("SEC_USER_AGENT", "pfe contact@example.com"))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_password: str = field(default_factory=lambda: _secret("KAFKA_SASL_PASSWORD"))
    overlap_minutes: int = 60  # chevauchement des fenêtres : sûr car l'ingestion est idempotente


def get_settings() -> Settings:
    return Settings()
