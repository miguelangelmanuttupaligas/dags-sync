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
        verbose=True,
        conf={
            "spark.kubernetes.container.image": "miguelmanuttupa/spark-k8s:3.5.0",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-sa",
            "spark.kubernetes.namespace": "airflow",
            "spark.driver.memory": "1g",
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "2g",
            "spark.app.name": "pi-airflow-k8s",
            "spark.executorEnv.SPARK_SUBMIT_DEPLOY_MODE": "client",
            "spark.executorEnv.SPARK_LIVY_DEPLOY_MODE": "client",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }
    )

submit_spark