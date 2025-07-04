from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="submit_jar_to_k8s",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["spark", "k8s"],
) as dag:

    submit_spark = SparkSubmitOperator(
        task_id="run_pi_spark_k8s",
        conn_id="spark_conn",
        application="local:////opt/spark/examples/src/main/python/pi.py",
        application_args=["2"],
        java_class="org.apache.spark.examples.SparkPi",
        verbose=True,
        conf={
            "spark.app.name": "pi-airflow-k8s",
            "spark.kubernetes.container.image": "miguelmanuttupa/pyspark-k8s:3.5.0",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-sa",
            "spark.kubernetes.driver.pod.name": "pi-airflow-k8s-driver",
            "spark.kubernetes.namespace": "airflow",
            "spark.driver.memory": "1g",
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "2g",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }
    )

submit_spark
