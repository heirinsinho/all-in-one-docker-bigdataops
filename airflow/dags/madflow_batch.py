import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

with (DAG('MadFlowBatch',
          start_date=datetime.datetime(2024, 10, 1),
          schedule_interval=None,
          description='DAG that executes the DAG for MadFlow streaming app',
          catchup=False,
          max_active_tasks=5,
          max_active_runs=5) as dag):
    spark_batch_traffic_locations = SparkSubmitOperator(
        task_id='spark_batch_traffic_locations',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch/traffic_locations.py",
        executor_memory="512M",
    )

    spark_batch_parkings = SparkSubmitOperator(
        task_id='spark_batch_parkings',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch/parkings.py",
        executor_memory="512M",
    )

    spark_batch_points_of_interest = SparkSubmitOperator(
        task_id='spark_batch_points_of_interest',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch/points_of_interest.py",
        executor_memory="512M",
    )

    # You need to run traffic_history job isolately so it is not affected by airflow overhead
    # Traffic history is mandatory for the whole application to run
    [spark_batch_traffic_locations, spark_batch_parkings, spark_batch_points_of_interest]
