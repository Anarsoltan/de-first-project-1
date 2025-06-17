from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_global_metrics',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
)

update_metrics = VerticaOperator(
    task_id='update_metrics_task',
    sql="""
        INSERT INTO STV202506141__DWH.global_metrics
       SELECT
            t.transaction_dt::DATE AS date_update,
            t.currency_code AS currency_from,
            SUM(t.amount * c.currency_code_div) AS amount_total,
            COUNT(*) AS cnt_transactions,
            AVG(sub.cnt) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM STV202506141__STAGING.transactions t
        JOIN STV202506141__STAGING.currencies c
          ON t.currency_code = c.currency_code 
        --   GROUP BY 1, 2;
          AND c.currency_code_with = 430  -- USD
        JOIN (
            SELECT account_number_from, COUNT(*) AS cnt
            FROM STV202506141__STAGING.transactions
            WHERE account_number_from > 0
            GROUP BY account_number_from
        ) sub ON sub.account_number_from = t.account_number_from
        WHERE t.account_number_from > 0
        --  AND t.transaction_dt::DATE = CURRENT_DATE - 1
          AND t.status = 'done'
        GROUP BY 1, 2;
    """,
    vertica_conn_id='vertica_dwh',
    dag=dag,
)
