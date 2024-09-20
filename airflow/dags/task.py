import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from hdfs import InsecureClient

with DAG(
        'etl_task',
        description='DAG that executes the ETL pipeline',
        schedule_interval=None,
        start_date=datetime(2023, 10, 12),
        catchup=False,
) as dag:
    metadata_filepath = "/opt/airflow/data/input/test/metadata.json"
    with open(metadata_filepath, "r") as json_file:
        metadata = json.load(json_file)


    def hdfs_upload():
        client = InsecureClient('http://namenode:9870')
        client.upload('/data/input/test', '/opt/airflow/data/input/test/input-data.json',
                      overwrite=True)


    create_spark_cluster = EmptyOperator(
        task_id='create_spark_cluster',
    )

    create_hdfs_cluster = EmptyOperator(
        task_id='create_hdfs_cluster',
    )

    create_kafka_cluster = EmptyOperator(
        task_id='create_kafka_cluster',
    )

    upload_to_hdfs = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=hdfs_upload
    )

    spark_job = SparkSubmitOperator(
        task_id='spark_job',
        conn_id='spark_docker',
        application="/opt/airflow/dags/pyspark_apps/etl.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3',
        executor_memory="512M",
        application_args=["--metadata", json.dumps(metadata),
                          '--kafka_broker', 'kafka:9092',
                          '--hdfs_host', 'namenode',
                          '--hdfs_port', '9000'],
    )

    stop_spark_cluster = EmptyOperator(
        task_id='stop_spark_cluster',
    )

    stop_hdfs_cluster = EmptyOperator(
        task_id='stop_hdfs_cluster',
    )

    stop_kafka_cluster = EmptyOperator(
        task_id='stop_kafka_cluster',
    )

    # Set the task dependencies
    [create_hdfs_cluster, create_spark_cluster, create_kafka_cluster] >> upload_to_hdfs >> spark_job
    spark_job >> [stop_spark_cluster, stop_hdfs_cluster, stop_kafka_cluster]