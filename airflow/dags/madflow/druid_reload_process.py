
# Uso:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 druid_reload_process.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from kafka import KafkaProducer


# Establecer la configuración para la conexión con Kafka
kafka_broker_hostname = 'localhost'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port


producer = KafkaProducer(bootstrap_servers=kafka_broker)


# Creación de la SparkSession
sparkSession = SparkSession\
        .builder\
        .appName("druid_reload_application")\
        .getOrCreate()
         #.master("local[*]") \
         #.config('job.local.dir', 'file:/Users/alberto/TFM/MadFlow') \

sparkSession.sparkContext.setLogLevel('WARN')
sparkSession.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")

# Configurar el topic de kafka que se corresponde con el tipo de datos que queremos recargar en druid desde hdfs
# traffic-druid-stream
# bicimad-druid-stream
# parkings-druid-stream
# traffic-druid-history-topic
kafka_reload_topic = 'traffic-druid-history-topic'

# Elegimos la ruta desde la que queremos recargar datos en druid
# /user/alberto/madflow/traffic/stream_data
# /user/alberto/madflow/bicimad_data
# /user/alberto/madflow/parkings/stream_data
# /user/alberto/madflow/traffic/history
base_path = "/user/alberto/madflow/traffic/history"

# Configuramos la partición que queremos cargar
year = "2020"
month = "11"
day = "26"

# Si no especificamos un día se cargará la partición completa del mes
if day != "":
    path = (base_path+"/year="+year+"/month="+month+"/day="+day)
else:
    path = (base_path+"/year="+year+"/month="+month)

data_reload = sparkSession.read.parquet(path)

data_reload = data_reload.select(data_reload["id"].cast('string').alias("key"),
                                 to_json(struct("*")).alias("value"))\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

"""
data_reload.show(5, False)
data_reload.printSchema()
"""

data_reload.write\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", kafka_reload_topic)\
    .save()

