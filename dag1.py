import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LIMA = pendulum.timezone("America/Lima")
with DAG(
  dag_id='dag_1',
  schedule=None, catchup=False,
  start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  start_task = EmptyOperator(task_id="start_task")

  with TaskGroup("task_group_1") as tg1:
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")

    task_1 >> task_2 >> task_3

  trigger_dag2 = TriggerDagRunOperator(
    task_id="trigger_dag2",
    trigger_dag_id="dag_2"
  )
  
  start_task >> tg1 >> trigger_dag2
