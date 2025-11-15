import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

LIMA = pendulum.timezone("America/Lima")
with DAG(
    dag_id='dag_2',
    schedule=None, catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  start_task = EmptyOperator(task_id="start_task")
  with TaskGroup("task_group_2", dag=dag2) as tg2:
      task_1 = EmptyOperator(task_id="task_1")
      task_2 = EmptyOperator(task_id="task_2")
  trigger_dag3 = TriggerDagRunOperator(
      task_id="trigger_dag3",
      trigger_dag_id="dag_3"
  )

# Dependencias
start_task >> tg2 >> trigger_dag3
