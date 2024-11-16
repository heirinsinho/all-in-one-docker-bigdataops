import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG('SparkBusTrips',
         start_date=datetime.datetime(2024, 10, 1),
         schedule_interval=None,
         description='DAG that executes the ETL of spark for the BusTrips data',
         catchup=False) as dag:

    intermediate = SparkSubmitOperator(
        task_id='intermediate',
        conn_id='spark_docker',
        application="/opt/airflow/dags/pyspark_apps/bus_trips_etl_intermediate.py",
        conf={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "hdfs://namenode:9000/shared/spark-logs",
        },
        deploy_mode="client"
    )

    lines = SparkSubmitOperator(
        task_id='lines',
        conn_id='spark_docker',
        application="/opt/airflow/dags/pyspark_apps/bus_trips_etl_lines.py",
        conf={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "hdfs://namenode:9000/shared/spark-logs",
        },
        deploy_mode="client"
    )

    stats = SparkSubmitOperator(
        task_id='stats',
        conn_id='spark_docker',
        application="/opt/airflow/dags/pyspark_apps/bus_trips_etl_stats.py",
        conf={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "hdfs://namenode:9000/shared/spark-logs",
        },
        deploy_mode="client"
    )

    # Set the task dependencies
    intermediate >> [lines, stats]
