from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Definimos el DAG 3
with DAG(dag_id="dag_3",
         schedule_interval=None,
         start_date=days_ago(1),
         catchup=False,
) as dag:
  # Tareas del DAG 3
  task_1 = EmptyOperator(task_id="task_1")

task_1
