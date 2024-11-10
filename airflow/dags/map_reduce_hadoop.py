import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.contrib.operators.ssh_operator import SSHOperator

from utils.utils import hdfs_upload

with DAG('MapReduceHadoop',
         description='DAG that executes compiles a java class holding a map reduce job and the job itself',
         start_date=datetime.datetime(2024, 10, 1),
         schedule_interval=None,
         params={
             "java_class": "WordCount"
         }) as dag:
    base_path = "/opt/hadoop-3.3.6/share/hadoop"
    hadoop_core_packages = ("common/hadoop-common",
                            "mapreduce/hadoop-mapreduce-client-core",
                            "mapreduce/hadoop-mapreduce-client-common",
                            "hdfs/hadoop-hdfs",
                            "hdfs/hadoop-hdfs-client")
    hadoop_packages_cmd = ":".join(f"{base_path}/{pkg}-3.3.6.jar" for pkg in hadoop_core_packages)

    bash_command = (
        f'javac -Xlint:deprecation -cp {hadoop_packages_cmd}:. /hadoop/applications/{dag.params["java_class"]}.java && '
        f'jar cf /hadoop/applications/{dag.params["java_class"]}.jar /hadoop/applications/*.class')

    create_jar = SSHOperator(
        task_id='create_jar_mapreduce',
        command=bash_command,
        ssh_conn_id='ssh_hadoop',
        dag=dag,
    )

    submit_file = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=hdfs_upload,
        op_kwargs=dict(
            input_path='/opt/airflow/data/input/word_count/word_count.txt',
            output_path='/data/input/word_count')
    )

    ssh_mapreduce = SSHOperator(
        task_id='execute_mapreduce_job',
        command='source /etc/profile.d/env_vars.sh && hadoop jar /hadoop/applications/WordCount.jar hadoop.applications.WordCount /data/input/word_count /data/output/word_count',
        ssh_conn_id='ssh_hadoop',
        dag=dag,
        conn_timeout=360,
        cmd_timeout=360,
        banner_timeout=360
    )

    create_jar >> submit_file >> ssh_mapreduce
