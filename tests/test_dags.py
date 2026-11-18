"""DAG wiring: both DAGs parse, and the asset chain ingest -> silver -> (gold) is intact.

Needs Airflow, which lives in its own environment (.venv-airflow, see requirements-airflow.txt)
because its constraints pin library versions; skipped when Airflow is not installed.
"""

import os
import sys
import tempfile
from pathlib import Path

DAGS = Path(__file__).resolve().parent.parent / "dags"
# Before Airflow is imported: keep its home out of ~/airflow. dags/ goes on sys.path the same
# way the containers do it (PYTHONPATH in docker-compose.yml), so the shared assets resolve.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
sys.path.insert(0, str(DAGS))

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS))


def uris(assets):
    return {a.uri for a in assets}


def test_dags_import_cleanly(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"ingest_openalex", "bronze_to_silver"}


def test_ingest_runs_daily_after_budget_reset_and_emits_bronze(dagbag):
    dag = dagbag.dags["ingest_openalex"]
    assert dag.timetable.expression == "15 0 * * *"
    assert dag.max_active_runs == 1
    assert uris(dag.get_task("ingest").outlets) == {"s3://bronze/works"}


def test_silver_is_triggered_by_bronze(dagbag):
    dag = dagbag.dags["bronze_to_silver"]
    assert {a.uri for a in dag.timetable.asset_condition.objects} == {"s3://bronze/works"}
    assert dag.max_active_runs == 1


def test_silver_asset_is_emitted_only_after_the_quality_gate(dagbag):
    dag = dagbag.dags["bronze_to_silver"]
    transform, gate = dag.get_task("transform"), dag.get_task("quality_gate")

    assert gate.upstream_task_ids == {"transform"}
    assert transform.outlets == []  # silver that has not been checked must not trigger gold
    assert uris(gate.outlets) == {"s3://silver/works"}
