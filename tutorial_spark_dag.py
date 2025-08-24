import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

print(os.getenv("SERVICE_ACCOUNT_NAME"))
print(os.getenv("POD_NAMESPACE"))
print(os.getenv("S3_ENDPOINT_URL"))
print(os.getenv("ACCESS_KEY"))
print(os.getenv("SECRET_KEY"))
print(os.getenv("WAREHOUSE_DIR"))
print(os.getenv("METASTORE_URI"))
print(os.getenv("NB_USER"))

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
            "spark.kubernetes.container.image": "miguelmanuttupa/pyspark-k8s-python3.11:3.5.0",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME"),
            "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE"),
            "spark.driver.memory": "1g",
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "2g",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY"),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR"),
            "hive.metastore.uris": os.getenv("METASTORE_URI"),
            "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER"),
        }
    )

submit_spark
