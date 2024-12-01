from hdfs import InsecureClient
from kafka.admin import KafkaAdminClient, NewTopic


def hdfs_upload(input_path, output_path):
    client = InsecureClient('http://namenode:9870')
    client.upload(output_path, input_path, overwrite=True)


def create_kafka_topics(topic_names):
    client = KafkaAdminClient(bootstrap_servers="kafka:9092")
    new_topics = []

    for topic in topic_names:
        if topic not in client.list_topics():
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

    client.create_topics(new_topics=new_topics, validate_only=False)
