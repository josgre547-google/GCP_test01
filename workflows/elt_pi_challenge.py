from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project-1d5fe13d-8034-4422-8f5"
LOCATION = "US"

BRONZE_DATASET = "bronze_dataset"
SILVER_DATASET = "silver_dataset"
GOLD_DATASET = "gold_dataset"

default_args = {
    "owner": "jose",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_pi_challenge",
    default_args=default_args,
    description="Orquestacion ELT: Cloud Run + BigQuery",
    start_date=datetime(2026, 4, 16),
    schedule=None,
    catchup=False,
    tags=["gcp", "elt", "challenge"],
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

    build_silver = BigQueryInsertJobOperator(
        task_id="build_silver",
        configuration={
            "query": {
                "query": f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.unificado_clean` AS

WITH combined AS (
  SELECT *, 1 AS priority
  FROM `{PROJECT_ID}.{BRONZE_DATASET}.unificado`

  UNION ALL

  SELECT
    CHROM,
    cast(POS as string),
    ID,
    REF,
    ALT,
    cast(QUAL as string),
    `FILTER`,
    INFO,
    FORMAT,
    cast(MUESTRA as string),
    VALOR,
    ORIGEN,
    timestamp_seconds(cast(FECHA_COPIA as int64)),
    RESULTADO,
    2 AS priority
  FROM `{PROJECT_ID}.{BRONZE_DATASET}.nuevas_filas`
),

dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY priority DESC
    ) AS rn
  FROM combined
)

SELECT *
FROM dedup
WHERE rn = 1
                """,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    build_gold = BigQueryInsertJobOperator(
        task_id="build_gold",
        configuration={
            "query": {
                "query": f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.unificado_final` AS
SELECT *
FROM `{PROJECT_ID}.{SILVER_DATASET}.unificado_clean`
                """,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    copy_data_to_bronze >> csv_to_parquet >> sqlserver_to_parquet >> build_silver >> build_gold