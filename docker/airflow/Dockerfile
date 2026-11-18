# Airflow plus the libraries the pipeline code needs. The project code itself is mounted at
# runtime (see docker-compose.yml), so editing a DAG or module needs no rebuild.
FROM apache/airflow:3.3.2-python3.14

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.14
COPY requirements.txt /tmp/requirements.txt
# Pin apache-airflow and use its constraints so the extra libraries cannot upgrade Airflow's own.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
