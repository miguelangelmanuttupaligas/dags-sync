import os
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Definimos el DAG
with DAG(
    dag_id='mi_dag_ejemplo',
    schedule="0 6 * * *", catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA),
) as dag:
  # Tareas representadas como DummyOperator por ser solo ejemplos
  dzonaterritorio = DummyOperator(task_id='DZONATERRITORIO', dag=dag)
  dzona = DummyOperator(task_id='DZONA', dag=dag)
  drecursos = DummyOperator(task_id='DRECURSO', dag=dag)
  dsubrecurso = DummyOperator(task_id='DSUBRECURSO', dag=dag)

  dclasepersona = DummyOperator(task_id='DCLASEPERSONA', dag=dag)
  dubigeo = DummyOperator(task_id='DUBIGEO', dag=dag)
  dpais = DummyOperator(task_id='DPAIS', dag=dag)
  dtipovia = DummyOperator(task_id='DTIPOVIA', dag=dag)
  dcliente = DummyOperator(task_id='DCLIENTE', dag=dag)

  dsector_economico = DummyOperator(task_id='DSECTORECONOMICO', dag=dag)
  dactividadeconomica = DummyOperator(task_id='DACTIVIDADECONOMICA', dag=dag)
  djeferegional = DummyOperator(task_id='djeferegional', dag=dag)
  dcomite = DummyOperator(task_id='dcomite', dag=dag)
  dsolicitudestado = DummyOperator(task_id='dsolicitudestado', dag=dag)
  dadeudado = DummyOperator(task_id='dadeudado', dag=dag)
  dadministrador = DummyOperator(task_id='dadministrador', dag=dag)

  dsolicitud = DummyOperator(task_id='DSOLICITUD', dag=dag)
  ddestinopivot = DummyOperator(task_id='DDESTINOPIVOT', dag=dag)
  # Definimos las dependencias basadas en el gráfico
  
dzonaterritorio>> dzona
drecursos >> dsubrecurso
[dclasepersona,dubigeo,dpais,dtipovia] >> dcliente
dsector_economico >> [dactividadeconomica,dsolicitudestado]
dactividadeconomica >> djeferegional
dsolicitudestado >> dadeudado
djeferegional >> dcomite
dadeudado >> dadministrador
[dsubrecurso,dcliente,dcomite,dadministrador] >> dsolicitud
dsolicitud >> ddestinopivot
