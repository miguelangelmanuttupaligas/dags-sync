import os
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# -------- Configuración base común --------
BASE_SPARK_CONF = {
    "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
    "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME"),
    "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE"),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR"),
    "hive.metastore.uris": os.getenv("METASTORE_URI"),
    "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY"),
}

LIMA = pendulum.timezone("America/Lima")
ID_SPARK_CONF="spark_conn"
IMAGEN_ORIGENDEMO = "miguelmanuttupa/lchcimage-brz-origendemo:latest"

with DAG(
    dag_id="origendemo_malla", tags=["spark", "k8s"],
    schedule="0 6 * * *", catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA),
) as dag:
    
    brz_origendemo_users_ddl = SparkSubmitOperator(
        task_id="brz_origendemo_users_ddl", conn_id=ID_SPARK_CONF, verbose=True,
        application="local:////opt/spark/app/users/ddl/brz_origendemo_users_ddl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "2g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "2g",
            **BASE_SPARK_CONF,
        },
        env_vars={
            "BUCKET": "lhchprd",
        }
    )

    brz_origendemo_users_etl = SparkSubmitOperator(
        task_id="brz_origendemo_users_etl", conn_id=ID_SPARK_CONF, verbose=True,
        application="local:////opt/spark/app/users/etl/brz_origendemo_users_etl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "2g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "2g",
            **BASE_SPARK_CONF,
        },
        env_vars={
            "BUCKET": "lhchprd",
            "SERVER_DB": "mssql-service.data-services.svc.cluster.local:1433",
            "FUENTE": "origendemo",
            "USER": "SA",
            "PASSWORD": "StrongPassword!23",
        }
    )

    brz_origendemo_products_ddl = SparkSubmitOperator(
        task_id="brz_origendemo_products_ddl", conn_id=ID_SPARK_CONF, verbose=True,
        application="local:////opt/spark/app/products/ddl/brz_origendemo_products_ddl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "2g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "2g",
            **BASE_SPARK_CONF,
        },
        env_vars={
            "BUCKET": "lhchprd",
        }
    )

    brz_origendemo_products_etl = SparkSubmitOperator(
        task_id="brz_origendemo_products_etl", conn_id=ID_SPARK_CONF, verbose=True,
        application="local:////opt/spark/app/products/etl/brz_origendemo_products_etl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "2g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "2g",
            **BASE_SPARK_CONF,
        },
        env_vars={
            "BUCKET": "lhchprd",
            "SERVER_DB": "mssql-service.data-services.svc.cluster.local:1433",
            "FUENTE": "origendemo",
            "USER": "SA",
            "PASSWORD": "StrongPassword!23",
        }
    )

brz_origendemo_users_ddl >> brz_origendemo_users_etl
brz_origendemo_products_ddl >> brz_origendemo_products_etl
