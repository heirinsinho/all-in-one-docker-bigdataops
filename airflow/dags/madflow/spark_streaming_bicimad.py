
# Uso:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark_streaming_bicimad.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import year, month, dayofmonth, hour, minute


# Establecer la configuración para la conexión con Kafka
kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_bicimad_topic_input = 'bicimad-topic'

# Creación de la SparkSession
sparkSession = SparkSession\
        .builder\
        .appName("spark_streaming_bicimad_application")\
        .getOrCreate()
         #.master("local[*]") \
         #.config('job.local.dir', 'file:/Users/alberto/TFM/MadFlow') \

sparkSession.sparkContext.setLogLevel('WARN')
sparkSession.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")


bicimadSchema = StructType()\
    .add("id", StringType())\
    .add("name", StringType())\
    .add("light", StringType())\
    .add("number", StringType())\
    .add("address", StringType())\
    .add("activate", StringType())\
    .add("no_available", StringType())\
    .add("total_bases", StringType())\
    .add("dock_bikes", StringType())\
    .add("free_bases", StringType()) \
    .add("reservations_count", StringType())\
    .add("geometry", StringType()) \
    .add("datetime", TimestampType())

# Creción del dataStream de bicimad
kafka_bicimad_stream = sparkSession\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_broker)\
    .option("subscribe", kafka_bicimad_topic_input)\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
bicimad_stream_parsed = kafka_bicimad_stream.select(from_json(kafka_bicimad_stream.value,
                                                              bicimadSchema).alias("bicimad_parsed"))
# Obtención de los campos incluidos en el mensaje
bicimadc_ds = bicimad_stream_parsed.select("bicimad_parsed.*")\

bicimadc_ds = bicimadc_ds.withColumnRenamed("light", "occupation")

bicimad_filtered = bicimadc_ds.filter(sf.col("activate") == "1")

bicimad_coordinates = bicimad_filtered.withColumn("geometry", sf.substring("geometry", 35, 40))\
    .withColumn("longitud", sf.split("geometry", ",")[0]) \
    .withColumn("latitud", sf.split("geometry", ",")[1]) \
    .withColumn("latitud", sf.expr("substring(latitud, 2, length(latitud)-3)")) \
    .drop("geometry")

bicimad_partition = bicimad_coordinates\
    .withColumn("year", year("datetime")) \
    .withColumn("month", month("datetime")) \
    .withColumn("day", dayofmonth("datetime")) \
    .withColumn("hour", hour("datetime")) \
    .withColumn("minute", minute("datetime"))


# Envío del resultado a kafka en micro-batches
queryToKafka = bicimad_partition\
    .select(bicimad_partition["id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='3 minutes') \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "bicimad-druid-stream") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/stream/bicimad/") \
    .outputMode("Append") \
    .start()
#old checkpoint path: /tmp/checkpoint/kafka/bicimad/


queryToHDFS = bicimad_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='3 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "/tmp/checkpoint/hdfs/stream/bicimad") \
    .option("path", "hdfs://namenode:9000/output/madflow/bicimad_data") \
    .outputMode("append") \
    .start()
# old checkpoint path: .option("checkpointLocation", "/tmp/checkpoint/hdfs/bicimad-stream/")
# old hdfs path: .option("path", "/user/alberto/madflow/bicimad")
"""
query = bicimad_partition.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()
"""

sparkSession.streams.awaitAnyTermination()


