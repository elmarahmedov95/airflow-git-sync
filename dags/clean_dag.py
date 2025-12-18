from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# ✅ Default args
default_args = {
    'owner': 'airflow',
    'retries': 2,                # Task uğursuz olarsa retry sayı
    'retry_delay': timedelta(seconds=5),  # Retry interval
}

with DAG(
    dag_id="clean_store_transactions",
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False
) as dag:

    run_clean_job = SSHOperator(
        task_id="run_clean_store_transactions",
        ssh_conn_id="spark_ssh_conn",
        command="""
        source /opt/airflowenv/bin/activate &&
        python /opt/jobs/clean_store_transactions.py
        """,
    )
