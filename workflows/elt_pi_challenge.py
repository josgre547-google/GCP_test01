from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = "project-1d5fe13d-8034-4422-8f5"
REGION = "us-east1"
CLUSTER_NAME = "spark-dev-single"

BUCKET = "pi-data-challenge-2026"

default_args = {
    "owner": "jose",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BRONZE_TO_SILVER_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/jobs/bronze_to_silver.py",
    },
}

SILVER_TO_GOLD_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/jobs/silver_to_gold.py",
    },
}

EXECUTION_LOGS_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/jobs/execution_logs.py",
    },
}

with DAG(
    dag_id="elt_pi_challenge_spark",
    default_args=default_args,
    description="ELT pipeline with Cloud Run ingestion and Dataproc Spark transformations",
    start_date=datetime(2026, 4, 23),
    schedule=None,
    catchup=False,
    tags=["gcp", "spark", "dataproc", "elt"],
) as dag:

    copy_data_to_bronze = HttpOperator(
        task_id="copy_data_to_bronze",
        http_conn_id="copy_data_to_bronze_http",
        endpoint="/",
        method="GET",
        log_response=True,
    )

    csv_to_parquet = HttpOperator(
        task_id="csv_to_parquet",
        http_conn_id="csv_to_parquet_http",
        endpoint="/",
        method="GET",
        log_response=True,
    )

    sqlserver_to_parquet = HttpOperator(
        task_id="sqlserver_to_parquet",
        http_conn_id="sqlserver_to_parquet_http",
        endpoint="/",
        method="GET",
        log_response=True,
    )

    bronze_to_silver = DataprocSubmitJobOperator(
        task_id="bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        job=BRONZE_TO_SILVER_JOB,
    )

    silver_to_gold = DataprocSubmitJobOperator(
        task_id="silver_to_gold",
        project_id=PROJECT_ID,
        region=REGION,
        job=SILVER_TO_GOLD_JOB,
    )

    execution_logs = DataprocSubmitJobOperator(
        task_id="execution_logs",
        project_id=PROJECT_ID,
        region=REGION,
        job=EXECUTION_LOGS_JOB,
    )

    [copy_data_to_bronze, sqlserver_to_parquet] >> csv_to_parquet
    csv_to_parquet >> bronze_to_silver >> silver_to_gold >> execution_logs