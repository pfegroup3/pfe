"""
    Orchestration Airflow des jobs Databricks de PFE SDD (le calcul reste dans Databricks).
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

ENV = os.getenv("PFE_ENV", "dev")
CONN = "databricks_default"
NY = pendulum.timezone("America/Montreal")

DEFAULT_ARGS = {
    "owner": "pfe-group3",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email": [os.getenv("ALERT_EMAIL", "data-team@exemple.com")],
    "email_on_failure": True,
}


def run_job(task_id: str, job_name: str, **kw) -> DatabricksRunNowOperator:
    # job_name : résolution par nom -> pas d'ID codé en dur entre environnements
    return DatabricksRunNowOperator(task_id=task_id, databricks_conn_id=CONN, job_name=f"{job_name}-{ENV}",
                                    wait_for_termination=True, **kw)


@dag(dag_id="pfe_batch_5min", schedule="*/5 * * * *", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
     catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS, tags=["pfe", "batch"],
     dagrun_timeout=timedelta(minutes=20))
def batch_5min():
    run_job("ingest_enrich_health", "pfe-batch-ingest", execution_timeout=timedelta(minutes=15),
            sla=timedelta(minutes=10))


@dag(dag_id="pfe_daily", schedule="30 22 * * 1-5", start_date=pendulum.datetime(2025, 1, 1, tz=NY),
     catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS, tags=["pfe", "daily"])
def daily():
    run_job("daily_batch", "pfe-daily")


@dag(dag_id="pfe_train_weekly", schedule="0 3 * * 0", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
     catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS, tags=["pfe", "ml"])
def train_weekly():
    run_job("train", "pfe-train", execution_timeout=timedelta(hours=3))


@dag(dag_id="pfe_stream_watchdog", schedule="*/10 * * * *", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
     catchup=False, max_active_runs=1, default_args={**DEFAULT_ARGS, "retries": 0}, tags=["pfe", "monitoring"])
def stream_watchdog():
    """Alerte si le pipeline DLT en continu n'est plus en cours d'exécution."""

    @task
    def check_pipeline():
        import requests

        hook = DatabricksHook(CONN)
        base = f"https://{hook.host}/api/2.0/pipelines"
        hdr = {"Authorization": f"Bearer {hook._get_token()}"}
        rows = requests.get(base, headers=hdr, params={"filter": f"name LIKE 'pfe-stream-{ENV}'"}, timeout=30).json()
        for p in rows.get("statuses", []):
            if p.get("state") not in ("RUNNING", "STARTING"):
                raise RuntimeError(f"Pipeline {p['name']} en état {p.get('state')}")

    check_pipeline()


batch_5min(); daily(); train_weekly(); stream_watchdog()
