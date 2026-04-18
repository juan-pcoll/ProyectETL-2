from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import pandas as pd
import os

sys.path.append("/opt/airflow/src")

from extract import extract_data
from extract_api import extract_api
from clean import run_cleaning
from transform import transform_data
from validate import validate_data
from load import load_data


DATA_DIR = "/opt/airflow/data"
TABLES_DIR = "/opt/airflow/data/tables"


# ===============================
# EXTRACT CSV
# ===============================

def extract_csv_task(**context):

    df = extract_data()

    path = f"{DATA_DIR}/tmp_csv.parquet"

    df.to_parquet(path)

    context["ti"].xcom_push(
        key="df_csv_path",
        value=path
    )


# ===============================
# EXTRACT API
# ===============================

def extract_api_task(**context):

    df_api = extract_api()

    path = f"{DATA_DIR}/tmp_api.parquet"

    df_api.to_parquet(path)

    context["ti"].xcom_push(
        key="df_api_path",
        value=path
    )


# ===============================
# CLEAN CSV
# ===============================

def clean_csv_task(**context):

    csv_path = context["ti"].xcom_pull(
        key="df_csv_path",
        task_ids="extract_csv"
    )

    df = pd.read_parquet(csv_path)

    df_clean = run_cleaning(df)

    clean_path = f"{DATA_DIR}/tmp_clean.parquet"

    df_clean.to_parquet(clean_path)

    context["ti"].xcom_push(
        key="df_clean_path",
        value=clean_path
    )


# ===============================
# TRANSFORM
# ===============================

def transform_task(**context):

    os.makedirs(TABLES_DIR, exist_ok=True)

    df_path = context["ti"].xcom_pull(
        key="df_clean_path",
        task_ids="clean_csv"
    )

    api_path = context["ti"].xcom_pull(
        key="df_api_path",
        task_ids="extract_api"
    )

    df = pd.read_parquet(df_path)
    df_api = pd.read_parquet(api_path)

    tables = transform_data(df, df_api)

    for name, table in tables.items():

    # convertir columnas object a string para evitar errores pyarrow
        for col in table.select_dtypes(include="object").columns:
            table[col] = table[col].astype("string")

        table.to_parquet(
            f"{TABLES_DIR}/{name}.parquet"
        )

    context["ti"].xcom_push(
        key="tables_path",
        value=TABLES_DIR
    )


# ===============================
# VALIDATE
# ===============================

def validate_task(**context):

    tables_path = context["ti"].xcom_pull(
        key="tables_path",
        task_ids="transform"
    )

    tables = {}

    for file in os.listdir(tables_path):

        name = file.replace(".parquet", "")

        tables[name] = pd.read_parquet(
            f"{tables_path}/{file}"
        )

    validate_data(tables)


# ===============================
# LOAD DATA WAREHOUSE
# ===============================

def load_task(**context):

    tables_path = context["ti"].xcom_pull(
        key="tables_path",
        task_ids="transform"
    )

    tables = {}

    for file in os.listdir(tables_path):

        name = file.replace(".parquet", "")

        tables[name] = pd.read_parquet(
            f"{tables_path}/{file}"
        )

    load_data(tables)


# ===============================
# DAG STRUCTURE
# ===============================

with DAG(
    dag_id="ods_pobreza_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract_csv_op = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv_task
    )

    extract_api_op = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api_task
    )

    clean_csv_op = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv_task
    )

    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    validate_op = PythonOperator(
        task_id="quality_check",
        python_callable=validate_task
    )

    load_op = PythonOperator(
        task_id="load_dw",
        python_callable=load_task
    )


    # PIPELINE FLOW

    extract_csv_op >> clean_csv_op >> transform_op
    extract_api_op >> transform_op
    transform_op >> validate_op >> load_op