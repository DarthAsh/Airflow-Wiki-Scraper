from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="mysql_write_example",
    start_date=datetime(2023, 1, 1),
    schedule=None,      # instead of schedule_interval=None
    catchup=False,
) as dag:
    run_sql = SQLExecuteQueryOperator(
        task_id="run_sql",
        conn_id="mysql_default",  # your MySQL connection id
        sql="SELECT 1;",
        # hook_params={"schema": "my_db"},  # optional: target DB/schema
    )
