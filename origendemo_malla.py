import os
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# -------- Configuración base común --------
BASE_SPARK_CONF = {
    "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
    "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME",default="spark-sa-airflow"),
    "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE",default="airflow"),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR",default="s3a://warehouse-prd/"),
    "hive.metastore.uris": os.getenv("METASTORE_URI",default="thrift://hive-metastore-prd.metastore.svc.cluster.local:9083"),
    "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER",default="user_ch_prod"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL",default="http://minio.data-services.svc.cluster.local:9000"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY",default="hive-key-prd-132fsad"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY",default="hive-secret-prd-13rfsdfsadf"),
}

LIMA = pendulum.timezone("America/Lima")
SPARK_CONN="spark_conn"
IMAGEN_ORIGENDEMO = "miguelmanuttupa/lchcimage-brz-origendemo:latest"

with DAG(
    dag_id="origendemo_malla", tags=["spark", "k8s"],
    schedule="0 6 * * *", catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz=LIMA),
) as dag:
    
    brz_origendemo_users_ddl = SparkSubmitOperator(
        task_id="brz_origendemo_users_ddl", conn_id=SPARK_CONN, verbose=True, java_class="org.apache.spark.examples.SparkPi",
        application="local:////opt/spark/app/users/ddl/brz_origendemo_users_ddl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "3g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "3g",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME",default="spark-sa-airflow"),
            "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE",default="airflow"),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR",default="s3a://warehouse-prd/"),
            "hive.metastore.uris": os.getenv("METASTORE_URI",default="thrift://hive-metastore-prd.metastore.svc.cluster.local:9083"),
            "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER",default="user_ch_prod"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL",default="http://minio.data-services.svc.cluster.local:9000"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY",default="hive-key-prd-132fsad"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY",default="hive-secret-prd-13rfsdfsadf"),
        },
        env_vars={
            "POD_NAMESPACE": "airflow", "BUCKET": "lhchprd",
        }
    )

    brz_origendemo_users_etl = SparkSubmitOperator(
        task_id="brz_origendemo_users_etl", conn_id=SPARK_CONN, verbose=True, java_class="org.apache.spark.examples.SparkPi",
        application="local:////opt/spark/app/users/etl/brz_origendemo_users_etl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "3g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "3g",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME",default="spark-sa-airflow"),
            "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE",default="airflow"),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR",default="s3a://warehouse-prd/"),
            "hive.metastore.uris": os.getenv("METASTORE_URI",default="thrift://hive-metastore-prd.metastore.svc.cluster.local:9083"),
            "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER",default="user_ch_prod"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL",default="http://minio.data-services.svc.cluster.local:9000"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY",default="hive-key-prd-132fsad"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY",default="hive-secret-prd-13rfsdfsadf"),
        },
        env_vars={
            "POD_NAMESPACE": "airflow", "BUCKET": "lhchprd",
            "SERVER_DB": "mssql-service.data-services.svc.cluster.local:1433",
            "FUENTE": "origendemo",
            "USER": "SA",
            "PASSWORD": "StrongPassword!23",
        }
    )

    brz_origendemo_products_ddl = SparkSubmitOperator(
        task_id="brz_origendemo_products_ddl", conn_id=SPARK_CONN, verbose=True, java_class="org.apache.spark.examples.SparkPi",
        application="local:////opt/spark/app/products/ddl/brz_origendemo_products_ddl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "3g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "3g",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME",default="spark-sa-airflow"),
            "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE",default="airflow"),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR",default="s3a://warehouse-prd/"),
            "hive.metastore.uris": os.getenv("METASTORE_URI",default="thrift://hive-metastore-prd.metastore.svc.cluster.local:9083"),
            "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER",default="user_ch_prod"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL",default="http://minio.data-services.svc.cluster.local:9000"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY",default="hive-key-prd-132fsad"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY",default="hive-secret-prd-13rfsdfsadf"),
        },
        env_vars={
            "POD_NAMESPACE": "airflow", "BUCKET": "lhchprd",
        }
    )

    brz_origendemo_products_etl = SparkSubmitOperator(
        task_id="brz_origendemo_products_etl", conn_id=SPARK_CONN, verbose=True, java_class="org.apache.spark.examples.SparkPi",
        application="local:////opt/spark/app/products/etl/brz_origendemo_products_etl.py",
        conf={
            "spark.kubernetes.container.image": IMAGEN_ORIGENDEMO,
            "spark.driver.cores": "1", "spark.driver.memory": "3g",
            "spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "3g",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": os.getenv("SERVICE_ACCOUNT_NAME",default="spark-sa-airflow"),
            "spark.kubernetes.namespace": os.getenv("POD_NAMESPACE",default="airflow"),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": os.getenv("WAREHOUSE_DIR",default="s3a://warehouse-prd/"),
            "hive.metastore.uris": os.getenv("METASTORE_URI",default="thrift://hive-metastore-prd.metastore.svc.cluster.local:9083"),
            "spark.databricks.delta.commitInfo.userMetadata": os.getenv("NB_USER",default="user_ch_prod"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT_URL",default="http://minio.data-services.svc.cluster.local:9000"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.getenv("ACCESS_KEY",default="hive-key-prd-132fsad"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("SECRET_KEY",default="hive-secret-prd-13rfsdfsadf"),
        },
        env_vars={
            "POD_NAMESPACE": "airflow", "BUCKET": "lhchprd",
            "SERVER_DB": "mssql-service.data-services.svc.cluster.local:1433",
            "FUENTE": "origendemo",
            "USER": "SA",
            "PASSWORD": "StrongPassword!23",
        }
    )

brz_origendemo_users_ddl >> brz_origendemo_users_etl
brz_origendemo_products_ddl >> brz_origendemo_products_etl
