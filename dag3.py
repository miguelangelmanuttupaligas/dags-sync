import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LIMA = pendulum.timezone("America/Lima")
with DAG(
    dag_id='dag_3',
    schedule=None, catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  
  task_1 = EmptyOperator(task_id="task_1")
  task_2 = BashOperator(task_id="task_2",bash_commad='echo "Me encuentro en DAG 3"')

  task_1
