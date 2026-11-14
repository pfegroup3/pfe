"""
    Connecteurs batch. Chaque source stocke le JSON brut ; le parsing vit en Silver.
"""
from __future__ import annotations
 
import logging
import random
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pfe.common.timeutil import _dt, _rfc3339
 
import pandas as pd
 
from pfe.common.retry import get_json
from pfe.ingestion.base import BaseSource, Record
 
log = logging.getLogger(__name__)
 
class FinnhubNews(BaseSource):
    """
        Extraction de Finnhub News.
    """
    name          = "finnhub_news"
    expected_keys = frozenset({"id", "headline", "datetime"})
    URL           = "https://finnhub.io/api/v1/company-news"
 
    def fetch(self, since, until) -> Iterator[dict]:
        for symbol in self.s.tickers:
            rows = get_json(
                self.http, self.URL,
                params={"symbol": symbol, "from": since.date().isoformat(), "to": until.date().isoformat()},
                headers={"X-Finnhub-Token": self.s.finnhub_key},  # token hors URL -> pas de fuite dans les logs
            )
            if not isinstance(rows, list):   # ex. {"error": "..."}
                log.warning("finnhub news %s: réponse inattendue", symbol)
                continue
            for row in rows:
                row["_symbol"] = symbol
                yield row
 
    def to_record(self, raw):
        ts = pd.to_datetime(raw["datetime"], unit="s", utc=True).to_pydatetime()
        return Record(self.name, f"{raw['id']}:{raw['_symbol']}", raw["_symbol"], ts, None, raw)
 
 
class FinnhubSentiment(BaseSource):
    """
        Sentiment pré-calculé Finnhub = référence de comparaison pour FinBERT (instantané quotidien).
    """
    name          = "finnhub_sentiment"
    expected_keys = frozenset({"_symbol"})
    URL           = "https://finnhub.io/api/v1/news-sentiment"
    MIN_GAP       = 1.1   # ~55 appels/min, sous la limite de 60
 
    def fetch(self, since, until):
        asof = datetime.now(timezone.utc).isoformat()
        for symbol in self.s.tickers:
            try:
                row = get_json(self.http, self.URL, params={"symbol": symbol},
                               headers={"X-Finnhub-Token": self.s.finnhub_key})
            except Exception as e:
                # ne pas logger l'URL complète ; un ticker en échec ne doit pas tuer le run
                log.warning("finnhub sentiment %s failed: %s", symbol, type(e).__name__)
                continue
            finally:
                time.sleep(self.MIN_GAP)
            if not row:   # {} = pas de données pour ce symbole
                continue
            row["_symbol"] = symbol
            row["_asof"] = asof
            yield row
 
    def to_record(self, raw):
        ts     = _dt(raw["_asof"])
        symbol = raw["_symbol"]
        return Record(self.name, f"{symbol}:{ts:%Y%m%d}", symbol, ts, None, raw)
 
 
class AlpacaNews(BaseSource):
    """
        Historique/rattrapage REST ; le temps réel passe par streaming/producer.py.
    """
    name          = "alpaca_news"
    expected_keys = frozenset({"id", "headline", "created_at", "symbols"})
    URL           = "https://data.alpaca.markets/v1beta1/news"
 
    def fetch(self, since, until):
        token = None
        while True:
            params = {
                "start": _rfc3339(since),
                "end": _rfc3339(until or datetime.now(timezone.utc)),
                "limit": 50, 
                "sort": "asc",
                "symbols": ",".join(self.s.tickers), 
                "include_content": "false"
            }
            if token:
                params["page_token"] = token
            res = get_json(self.http, self.URL, params=params,
                           headers={"APCA-API-KEY-ID": self.s.alpaca_key, "APCA-API-SECRET-KEY": self.s.alpaca_secret})
            yield from res.get("news", [])
            token = res.get("next_page_token")
            if not token:
                return
 
    def to_record(self, raw):
        return Record(self.name, str(raw["id"]), None, _dt(raw["created_at"]), _dt(raw.get("updated_at")), raw)
 
 
class EdgarForm8K(BaseSource):
    """
        8-K via l'API submissions (User-Agent déclaratif obligatoire ; <= 10 req/s).
    """
    name          = "sec_8k"
    expected_keys = frozenset({"accessionNumber", "form", "acceptanceDateTime"})
    URL           = "https://www.sec.gov/files/company_tickers.json"
    SUBMISSIONS   = "https://data.sec.gov/submissions/CIK{cik}.json"
 
    def _cik_map(self) -> dict[str, str]:
        d = get_json(self.http, self.URL, headers={"User-Agent": self.s.sec_user_agent})
        return {v["ticker"]: str(v["cik_str"]).zfill(10) for v in d.values()}
 
    def fetch(self, since, until):
        ciks = self._cik_map()
        for symbol in self.s.tickers:
            cik = ciks.get(symbol)
            if not cik:
                log.warning("sec_8k: pas de CIK pour %s", symbol)
                continue
            sub = get_json(self.http, self.SUBMISSIONS.format(cik=cik),
                           headers={"User-Agent": self.s.sec_user_agent})
            recent = sub["filings"]["recent"]
            for i, form in enumerate(recent["form"]):
                if form != "8-K":
                    continue
                row = {k: recent[k][i] for k in recent if len(recent[k]) > i}
                ts = _dt(row["acceptanceDateTime"])  # horodatage d'acceptation EDGAR (UTC)
                if ts and since <= ts <= until:
                    row.update(_symbol=symbol, _cik=cik)
                    yield row
            time.sleep(0.15)  # ~6 req/s, sous la limite SEC
 
    def to_record(self, raw):
        return Record(self.name, f"{raw['_cik']}:{raw['accessionNumber']}", raw["_symbol"],
                      _dt(raw["acceptanceDateTime"]), None, raw)
 
 
class GdeltDoc(BaseSource):
    """
        Articles macro GDELT (artlist). Limité à 250 articles par fenêtre : découper la fenêtre en amont.
    """
    name          = "gdelt"
    expected_keys = frozenset({"url", "title", "seendate"})
    QUERY         = '(inflation OR "interest rates" OR Fed OR tariffs OR sanctions OR OPEC) sourcelang:eng'
    URL           = "https://api.gdeltproject.org/api/v2/doc/doc"
    MIN_GAP       = 6.0      # GDELT tolère ~1 requête / 5 s
    MAX_TRIES     = 6
 
    def _get(self, params):
        for i in range(self.MAX_TRIES):
            r = self.http.get(self.URL, params=params, timeout=30)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", self.MIN_GAP * 2**i)) + random.random()
                time.sleep(min(wait, 120))
                continue
            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                # GDELT renvoie parfois du texte/HTML (erreur de requête, résultat vide)
                return {}
        raise RuntimeError("GDELT: 429 persistant après retries")
 
    def fetch(self, since, until):
        until = min(until, datetime.now(timezone.utc))   # pas de fenêtre dans le futur
        since = since.astimezone(timezone.utc)
        until = until.astimezone(timezone.utc)
        params = {
            "query": self.QUERY, "mode": "artlist", "format": "json",
            "maxrecords": 250, "sort": "datedesc",
            "startdatetime": since.strftime("%Y%m%d%H%M%S"),
            "enddatetime": until.strftime("%Y%m%d%H%M%S"),
        }
        res = self._get(params)
        time.sleep(self.MIN_GAP)   # espace les appels successifs
        yield from res.get("articles", [])
 
    def to_record(self, raw):
        return Record(self.name, raw["url"], None,
                      _dt(pd.to_datetime(raw["seendate"], format="%Y%m%dT%H%M%SZ", utc=True)), None, raw)
 
 
class AlpacaBars1m(BaseSource):
    """
        Barres 1 min brutes (adjustment=raw) : on ajuste nous-mêmes -> recalculable et traçable.
    """
    name          = "alpaca_bars_1m"
    bronze_table  = "bronze.raw_bars"
    expected_keys = frozenset({"t", "o", "h", "l", "c", "v"})
    URL           = "https://data.alpaca.markets/v2/stocks/bars"
 
    def fetch(self, since, until):
        for symbol in self.s.tickers:
            token = None
            while True:
                params = {
                    "symbols": symbol, 
                    "timeframe": "1Min", 
                    "start": _rfc3339(since),
                    "end": _rfc3339(until or datetime.now(timezone.utc)),
                    "limit": 10000, 
                    "adjustment": "raw", 
                    "feed": self.s.alpaca_feed
                }
                if token:
                    params["page_token"] = token
                res = get_json(self.http, self.URL, params = params,
                               headers={"APCA-API-KEY-ID": self.s.alpaca_key, "APCA-API-SECRET-KEY": self.s.alpaca_secret})
                for b in (res.get("bars") or {}).get(symbol, []):
                    b["_symbol"] = symbol
                    yield b
                token = res.get("next_page_token")
                if not token:
                    break
 
    def to_record(self, raw):
        return Record(self.name, f"{raw['_symbol']}:{raw['t']}", raw["_symbol"], _dt(raw["t"]), None, raw)
 
 
class YahooDaily(BaseSource):
    """
        Quotidien brut + actions corporatives (fractionnements, dividendes).
    """
    name          = "yahoo_daily"
    bronze_table  = "bronze.raw_bars"
    expected_keys = frozenset({"Close"})
 
    def fetch(self, since, until):
        import yfinance as yf
 
        for symbol in self.s.tickers:
            h = yf.Ticker(symbol).history(start=since.date(), end=until.date() + timedelta(days=1),
                                          auto_adjust=False, actions=True)
            for ts, row in h.iterrows():
                d = {k: (None if pd.isna(v) else float(v)) for k, v in row.items()}
                t = pd.Timestamp(ts)
                t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
                d.update(_symbol=symbol, _ts=t.isoformat())
                yield d
 
    def to_record(self, raw):
        return Record(self.name, f"{raw['_symbol']}:{raw['_ts']}", raw["_symbol"], _dt(raw["_ts"]), None, raw)
 
 
REGISTRY = {c.name: c for c in (FinnhubNews, FinnhubSentiment, AlpacaNews, EdgarForm8K, GdeltDoc, AlpacaBars1m, YahooDaily)}

