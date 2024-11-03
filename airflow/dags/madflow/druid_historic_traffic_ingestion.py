
# Uso:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 druid_historic_traffic_ingestion.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from kafka import KafkaProducer


# Establecer la configuración para la conexión con Kafka
kafka_broker_hostname = 'localhost'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_traffic_druid_topic = 'traffic-druid-history-topic'

producer = KafkaProducer(bootstrap_servers=kafka_broker)


# Creación de la SparkSession
sparkSession = SparkSession\
        .builder\
        .appName("druid_historic_traffic_ingestion_application")\
        .getOrCreate()
         #.master("local[*]") \
         #.config('job.local.dir', 'file:/Users/alberto/TFM/MadFlow') \

sparkSession.sparkContext.setLogLevel('WARN')
#sparkSession.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")

traffic_historic = sparkSession.read.parquet("/user/alberto/madflow/traffic/history")
_traffic_year = str(traffic_historic.agg(sf.max("year")).collect()[0][0])
_traffic_month = str(traffic_historic.agg(sf.max("month")).collect()[0][0])


#HACEMOS EL ENVÍO DE ALGUNOS DÍAS PORQUE NO PUEDE CON EL MES COMPLETO DE UNA VEZ
#Para ingestar el mes entero borrar del path: +_traffic_month+"/day=31"
last_traffic_historic = sparkSession.read.parquet(
    "/user/alberto/madflow/traffic/history/year="+_traffic_year+"/month="+_traffic_month+"/day=24")

last_traffic_historic = last_traffic_historic.select(last_traffic_historic["id"].cast('string').alias("key"),
                             to_json(struct("*")).alias("value"))\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


last_traffic_historic.write\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", kafka_traffic_druid_topic)\
    .save()


