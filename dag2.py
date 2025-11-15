from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Definimos el DAG 2
with DAG(dag_id="dag_1",
         schedule_interval=None,
         start_date=days_ago(1),
         catchup=False,
) as dag:
  # Tareas del DAG 2
  start_task = EmptyOperator(task_id="start_task")
  with TaskGroup("task_group_2", dag=dag2) as tg2:
      task_1 = EmptyOperator(task_id="task_1")
      task_2 = EmptyOperator(task_id="task_2")
  # Invocar el DAG 3
  trigger_dag3 = TriggerDagRunOperator(
      task_id="trigger_dag3",
      trigger_dag_id="dag_3"
  )

# Dependencias
start_task >> tg2 >> trigger_dag3
