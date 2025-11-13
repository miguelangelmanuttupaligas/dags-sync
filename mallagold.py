import os
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
#Test
# Definimos el DAG
LIMA = pendulum.timezone("America/Lima")
with DAG(
    dag_id='mi_dag_ejemplo',
    schedule="0 6 * * *", catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA)
) as dag:
  # Tareas representadas como EmptyOperator por ser solo ejemplos
  dzonaterritorio = EmptyOperator(task_id='DZONATERRITORIO')
  dzona = EmptyOperator(task_id='DZONA')
  drecursos = EmptyOperator(task_id='DRECURSO')
  dsubrecurso = EmptyOperator(task_id='DSUBRECURSO')

  dclasepersona = EmptyOperator(task_id='DCLASEPERSONA')
  dubigeo = EmptyOperator(task_id='DUBIGEO')
  dpais = EmptyOperator(task_id='DPAIS')
  dtipovia = EmptyOperator(task_id='DTIPOVIA')
  dcliente = EmptyOperator(task_id='DCLIENTE')

  dsector_economico = EmptyOperator(task_id='DSECTORECONOMICO')
  dactividadeconomica = EmptyOperator(task_id='DACTIVIDADECONOMICA')
  djeferegional = EmptyOperator(task_id='djeferegional')
  dcomite = EmptyOperator(task_id='dcomite')
  dsolicitudestado = EmptyOperator(task_id='dsolicitudestado')
  dadeudado = EmptyOperator(task_id='dadeudado')
  dadministrador = EmptyOperator(task_id='dadministrador')

  dsolicitud = EmptyOperator(task_id='DSOLICITUD')
  ddestinopivot = EmptyOperator(task_id='DDESTINOPIVOT')
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
