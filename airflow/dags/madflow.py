import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream

from utils.utils import hdfs_upload, create_kafka_topics


with (DAG('MadFlow',
         start_date=datetime.datetime(2024, 10, 1),
         schedule_interval=None,
         description='DAG that executes the DAG for MadFlow streaming app',
         catchup=False,
         max_active_tasks=6,
         max_active_runs=6) as dag):

    submit_file = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=hdfs_upload,
        op_kwargs=dict(
            input_path='/opt/airflow/data/input/madflow',
            output_path='/data/input/madflow')
    )

    create_kafka_topics = PythonOperator(
        task_id='create_kafka_topics',
        python_callable=create_kafka_topics,
        op_kwargs=dict(
            topic_names=["bicimad", "parkings", "traffic", "bicimad-output-stream",
                         "parkings-output-stream", "traffic-output-stream"])
    )

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

    spark_batch_traffic = SparkSubmitOperator(
        task_id='spark_batch_historic_traffic',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch_historic_traffic.py",
        executor_memory="512M",
    )

    spark_batch_parkings_statistics = SparkSubmitOperator(
        task_id='spark_batch_parkings_statistics',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch_parkings_statistics.py",
        executor_memory="512M",
    )

    spark_batch_locations = SparkSubmitOperator(
        task_id='spark_batch_locations',
        conn_id='spark_docker',
        application="/opt/airflow/dags/madflow/spark_batch_locations.py",
        executor_memory="512M",
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
    submit_file >> [spark_batch_traffic, spark_batch_locations, spark_batch_parkings_statistics]
    create_kafka_topics >>  [bicimad_producer, traffic_producer, parkings_producer]
    cross_downstream([submit_file, create_kafka_topics],
                     [spark_bicimad, spark_traffic, spark_parkings])

