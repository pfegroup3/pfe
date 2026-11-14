"""Socle d'ingestion : Bronze append-only, versionné par hash, idempotent, avec quarantaine."""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pfe.common.timeutil import to_utc
from typing import Any

import requests

from pfe.common.config import Settings
from pfe.common.lakehouse import get_watermark, merge_insert_only, set_watermark
from pfe.common.retry import SchemaDrift
from pfe.common.runlog import run_log
from pfe.common.config import get_symbols

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BRONZE_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("record_id", StringType(), False),
    StructField("symbol", StringType(), True),
    StructField("published_utc", TimestampType(), False),
    StructField("updated_utc", TimestampType(), True),   
    StructField("ingested_utc", TimestampType(), False),
    StructField("payload", StringType(), False),
    StructField("record_hash", StringType(), False),
    StructField("schema_fp", StringType(), False),
])

REJECTS_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("raw", StringType(), True),
    StructField("error", StringType(), True),
])


@dataclass
class Record:
    source: str
    record_id: str
    symbol: str | None
    published_utc: datetime | None
    updated_utc: datetime | None
    payload: dict[str, Any]


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def schema_fingerprint(payload: dict) -> str:
    return hashlib.md5(",".join(sorted(payload)).encode()).hexdigest()  # noqa: S324 - non cryptographique


class BaseSource(ABC):
    name: str
    bronze_table: str = "bronze.raw_events"

    expected_keys: frozenset[str] = frozenset()  # clés minimales attendues -> détecte un changement de structure

    def __init__(self, settings: Settings):
        self.s       = settings
        self.http    = requests.Session()

    @abstractmethod
    def fetch(self, since: datetime, until: datetime) -> Iterator[dict]: ...

    @abstractmethod
    def to_record(self, raw: dict) -> Record | None: ...

    def check_schema(self, raw: dict) -> None:
        missing = self.expected_keys - raw.keys()
        if missing:
            raise SchemaDrift(f"{self.name}: champs manquants {sorted(missing)}")


def run_source(spark, src: BaseSource, *, default_start: datetime, until: datetime | None = None) -> None:
    """
        Ingestion fenêtrée : watermark - chevauchement -> fetch -> validation -> MERGE -> watermark.
    """
    pfe_catalog = src.s.catalog

    since = to_utc(get_watermark(spark, pfe_catalog, src.name, default_start))- timedelta(minutes=src.s.overlap_minutes)
    until = to_utc(until) or datetime.now(timezone.utc)
    
    with run_log(spark, pfe_catalog, "ingest", src.name) as run_stats:
        good, bad, fingerprints = [], [], set()

        for raw in src.fetch(since, until):
            run_stats.received += 1
            try:
                src.check_schema(raw)
                rec = src.to_record(raw)
            except SchemaDrift:
                raise
            except Exception as exc:  # enregistrement illisible entrainant la mise en quarantaine, on évite l'arrêt du lot
                bad.append({"source": src.name, "raw": json.dumps(raw, default=str)[:20000], "error": str(exc)[:500]})
                continue
            if rec is None or rec.record_id is None or rec.published_utc is None:
                bad.append({"source": src.name, "raw": json.dumps(raw, default = str)[:20000], "error": "id/horodatage manquant"})
                continue
            fingerprints.add(schema_fingerprint(raw))
            good.append(
                {
                    "source": rec.source,
                    "record_id": str(rec.record_id),
                    "symbol": rec.symbol,
                    "published_utc": rec.published_utc,
                    "updated_utc": rec.updated_utc,
                    "ingested_utc": datetime.now(timezone.utc),
                    "payload": json.dumps(rec.payload, default = str),
                    "record_hash": payload_hash(rec.payload),
                    "schema_fp": schema_fingerprint(rec.payload),
                }
            )
        run_stats.rejected, run_stats.kept = len(bad), len(good)
        if good:
            names = BRONZE_SCHEMA.fieldNames()
            rows  = [tuple(g[n] for n in names) for g in good]   # ordre garanti = ordre du schéma
            df    = spark.createDataFrame(rows, schema = BRONZE_SCHEMA)
            merge_insert_only(
                    spark, 
                    df, 
                    f"{pfe_catalog}.{src.bronze_table}", 
                    ["source", "record_id", "record_hash"], 
                    src.name
                )
            set_watermark(spark, pfe_catalog, src.name, max(g["published_utc"] for g in good))
        if bad:
            spark.createDataFrame(
                [(b["source"], b["raw"], b["error"]) for b in bad], schema = REJECTS_SCHEMA
            ).write.mode("append").saveAsTable(f"{pfe_catalog}.bronze.rejects")
        if len(fingerprints) > 1:
            run_stats.message = f"Hétérogénéité de structure détectée ({len(fingerprints)} empreintes)"
