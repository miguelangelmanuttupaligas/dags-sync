import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LIMA = pendulum.timezone("America/Lima")
with DAG(
  dag_id='dag_2',
  schedule=None, catchup=False,
  start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  
  start_task = EmptyOperator(task_id="start_task")

  with TaskGroup("task_group_2") as tg2:
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = BashOperator(task_id="task_2",bash_commad='echo "Me encuentro en DAG 2"')

    task_1 >> task_2

  trigger_dag3 = TriggerDagRunOperator(
    task_id="trigger_dag3",
    trigger_dag_id="dag_3"
  )

  start_task >> tg2 >> trigger_dag3
