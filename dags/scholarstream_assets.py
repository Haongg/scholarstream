"""Assets that chain the medallion DAGs: each layer's DAG is scheduled on the layer before it.

An asset event only means "the producing task succeeded"; Airflow never looks inside the
bucket. The guarantees behind each event live in the code: bronze days carry a _SUCCESS
marker once reconciled, and the silver event is emitted only after the quality gate passes.
"""

from airflow.sdk import Asset

BRONZE_WORKS = Asset(name="bronze_works", uri="s3://bronze/works")
SILVER_WORKS = Asset(name="silver_works", uri="s3://silver/works")  # quality-checked silver
