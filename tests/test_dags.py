import os
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

import pytest

pytest.importorskip("airflow")


def test_dags_importables():
    from airflow.dag_processing.dagbag import DagBag
    #from airflow.sdk import dag, task


    bag = DagBag(dag_folder="airflow/dags")
    assert not bag.import_errors
    assert {"pfe_batch_5min", "pfe_daily", "pfe_train_weekly", "pfe_stream_watchdog"} <= set(bag.dags)
