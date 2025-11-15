import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LIMA = pendulum.timezone("America/Lima")
with DAG(
    dag_id='dag_3',
    schedule=None, catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  
  task_1 = EmptyOperator(task_id="task_1")

  task_1
