import argparse
import logging
import os

from pfe import jobs


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser("pfe")
    p.add_argument("--catalog")
    p.add_argument("--tickers")
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("ingest")
    i.add_argument("--source", required=True)
    i.add_argument("--lookback-days", type=int, default=3)
    e = sub.add_parser("enrich")
    e.add_argument("--lookback-hours", type=int, default=72)
    sub.add_parser("train")
    sub.add_parser("health")
    sub.add_parser("compute_car")
    a = p.parse_args()
    if a.catalog:
        os.environ["PFE_CATALOG"] = a.catalog
    if a.tickers:
        os.environ["TICKERS"] = a.tickers
    if a.cmd == "ingest":
        jobs.ingest(a.source, a.lookback_days)
    elif a.cmd == "enrich":
        jobs.enrich(a.lookback_hours)
    else:
        getattr(jobs, a.cmd)()

if __name__ == "__main__":
    main()
