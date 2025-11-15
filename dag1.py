from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Definimos el DAG 1
with DAG(dag_id="dag_1",
         schedule_interval=None,
         start_date=days_ago(1),
         catchup=False,
) as dag:
  # Tareas del DAG 
  start_task = EmptyOperator(task_id="start_task")
  # Usamos un TaskGroup para agrupar tareas
  with TaskGroup("task_group_1", dag=dag1) as tg1:
      task_1 = EmptyOperator(task_id="task_1")
      task_2 = EmptyOperator(task_id="task_2")
      task_3 = EmptyOperator(task_id="task_3")
  # Invocar el DAG 2
  trigger_dag2 = TriggerDagRunOperator(
      task_id="trigger_dag2",
      trigger_dag_id="dag_2",  # El DAG que se invoca
  )
# Dependencias
start_task >> tg1 >> trigger_dag2
