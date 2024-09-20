from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from datetime import datetime
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient
from airflow.contrib.operators.ssh_operator import SSHOperator

args = {
    'owner': 'airflow'
    , 'start_date': datetime(2017, 1, 27)
    , 'provide_context': True
}
d = datetime(2017, 1, 17, 3, 15, 00)


def hdfs_upload():
    client = InsecureClient('http://namenode:9870')
    client.upload('/data/input/word_count/',
                  '/opt/airflow/data/input/word_count/word_count.txt',
                  overwrite=True)


with DAG('usgs',
         start_date=d,
         schedule_interval=None,
         params={
             "java_class": "WordCount"
         },
         default_args=args) as dag:

    base_path = "/opt/hadoop-3.3.6/share/hadoop"
    hadoop_core_packages = ("common/hadoop-common",
                            "mapreduce/hadoop-mapreduce-client-core",
                            "mapreduce/hadoop-mapreduce-client-common",
                            "hdfs/hadoop-hdfs",
                            "hdfs/hadoop-hdfs-client")
    hadoop_packages_cmd = ":".join(f"{base_path}/{pkg}-3.3.6.jar" for pkg in hadoop_core_packages)

    bash_command = (f'javac -Xlint:deprecation -cp {hadoop_packages_cmd}:. /hadoop/applications/{dag.params["java_class"]}.java && '
                    f'jar cf /hadoop/applications/{dag.params["java_class"]}.jar /hadoop/applications/*.class')

    create_jar = SSHOperator(
        task_id='create_jar_mapreduce',
        command=bash_command,
        ssh_conn_id='ssh_hadoop',
        dag=dag,
    )

    submit_file = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=hdfs_upload
    )

    ssh_mapreduce = SSHOperator(
        task_id='execute_mapreduce_job',
        command='source /etc/profile.d/env_vars.sh && hadoop jar /hadoop/applications/WordCount.jar hadoop.applications.WordCount /data/input/word_count /data/output/word_count',
        ssh_conn_id='ssh_hadoop',
        dag=dag,
    )

    create_jar >> submit_file >> ssh_mapreduce
