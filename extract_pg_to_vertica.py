from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def extract_table_to_vertica(table_name, vertica_table, ds, date_column):
    # PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgre_db1')

    # SQL query – extract by date
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE DATE({date_column}) = DATE('{ds}');
    """
    df = pg_hook.get_pandas_df(query)

    # Save to CSV
    csv_path = f"/tmp/{table_name}_{ds}.csv"
    df.to_csv(csv_path, index=False, header=True)

    # Vertica connection
    vertica_hook = VerticaHook(vertica_conn_id='vertica_dwh')

    # COPY command
    copy_query = f"""
        COPY STV202506141__STAGING.{vertica_table}
        FROM LOCAL '{csv_path}'
        DELIMITER ',' ENCLOSED BY '"'
        SKIP 1;
    """
    vertica_hook.run(copy_query)

    # Clean up
    os.remove(csv_path)

def load_transactions(ds, **kwargs):
    extract_table_to_vertica(
        table_name='transactions',
        vertica_table='transactions',
        ds=ds,
        date_column='transaction_dt'
    )

def load_currencies(ds, **kwargs):
    extract_table_to_vertica(
        table_name='currencies',
        vertica_table='currencies',
        ds=ds,
        date_column='date_update'
    )

default_args = {
    'start_date': datetime(2022, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_staging_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
)

load_transactions_task = PythonOperator(
    task_id='load_transactions',
    python_callable=load_transactions,
    provide_context=True,
    dag=dag,
)

load_currencies_task = PythonOperator(
    task_id='load_currencies',
    python_callable=load_currencies,
    provide_context=True,
    dag=dag,
)

load_transactions_task >> load_currencies_task
