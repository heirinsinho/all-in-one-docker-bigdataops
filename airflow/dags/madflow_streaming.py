import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from utils.utils import create_kafka_topics

with (DAG('MadFlowStreaming',
          start_date=datetime.datetime(2024, 10, 1),
          schedule_interval=None,
          description='DAG that executes the DAG for MadFlow streaming app',
          catchup=False,
          max_active_tasks=5,
          max_active_runs=5) as dag):

    create_kafka_topics = PythonOperator(
        task_id='create_kafka_topics',
        python_callable=create_kafka_topics,
        op_kwargs=dict(
            topic_names=["bicimad", "parkings", "traffic", "bicimad-output-stream",
                         "parkings-output-stream", "traffic-output-stream", "madflow-output-stream"])
    )

    kafka_producers = BashOperator(
        task_id='madflow_kafka_producers',
        bash_command="python /opt/airflow/dags/madflow/kafka/producer.py"
    )

    spark_streaming_bicimad = SparkSubmitOperator(
        task_id='spark_streaming_bicimad',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming/bicimad.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
    )

    spark_streaming_traffic = SparkSubmitOperator(
        task_id='spark_streaming_traffic',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming/traffic.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
    )

    spark_streaming_parkings = SparkSubmitOperator(
        task_id='spark_streaming_parkings',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming/parkings.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
    )

    spark_streaming_main_job = SparkSubmitOperator(
        task_id='spark_streaming_main_job',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming/madflow_main_job.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
    )

    # Set the task dependencies
    create_kafka_topics >> kafka_producers
    [spark_streaming_bicimad, spark_streaming_traffic, spark_streaming_parkings, spark_streaming_main_job]
