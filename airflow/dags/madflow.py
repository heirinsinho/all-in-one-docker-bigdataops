import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator



with DAG('MadFlow',
         start_date=datetime.datetime(2024, 10, 1),
         schedule_interval=None,
         description='DAG that executes the DAG for MadFlow streaming app',
         catchup=False,
         max_active_tasks=6,
         max_active_runs=6) as dag:

    bicimad_producer = BashOperator(
        task_id='madflow_kafka_bicimad_producer',
        bash_command="python /opt/airflow/dags/madflow/kafka_bicimad_producer.py"
    )

    traffic_producer = BashOperator(
        task_id='madflow_kafka_traffic_producer',
        bash_command="python /opt/airflow/dags/madflow/kafka_traffic_producer.py"
    )

    parkings_producer = BashOperator(
        task_id='madflow_kafka_parkings_producer',
        bash_command="python /opt/airflow/dags/madflow/kafka_parkings_producer.py"
    )

    spark_bicimad = SparkSubmitOperator(
        task_id='spark_streaming_bicimad',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming_bicimad.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
        executor_memory="512M",
    )

    spark_traffic = SparkSubmitOperator(
        task_id='spark_streaming_traffic',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming_traffic.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
        executor_memory="512M",
    )

    spark_parkings = SparkSubmitOperator(
        task_id='spark_streaming_parkings',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_streaming_parkings.py",
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4',
        executor_memory="512M",
    )

    # Set the task dependencies
    bicimad_producer
    traffic_producer
    parkings_producer
    spark_bicimad
    spark_traffic
    spark_parkings
