
#  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 spark_streaming_parkings.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth, hour, minute


# Establecer la configuración para la conexión con Kafka
kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_parking_topic_input = 'parkings-topic'


# Creación de la SparkSession
sparkSession = SparkSession\
        .builder\
        .appName("parkings-test-streaming")\
        .getOrCreate()
         #.master("local[*]") \
         #.config('job.local.dir', 'file:/Users/alberto/TFM/MadFlow') \

sparkSession.sparkContext.setLogLevel('WARN')
#sparkSession.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")


parkingSchema = StructType()\
    .add("id", StringType())\
    .add("name", StringType())\
    .add("address", StringType())\
    .add("postalCode", StringType())\
    .add("isEmtPark", StringType())\
    .add("lastUpd", TimestampType())\
    .add("freeParking", StringType())\
    .add("geometry", StringType())\
    .add("datetime", TimestampType())


# Creación del dataStream de parkings
kafka_parkings_stream = sparkSession\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_broker)\
    .option("subscribe", kafka_parking_topic_input)\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
parkings_stream_parsed = kafka_parkings_stream.select(from_json(kafka_parkings_stream.value,
                                                                parkingSchema).alias("parkings_parsed"))
# Obtención de los campos incluidos en el mensaje
parkings_ds = parkings_stream_parsed.select("parkings_parsed.*")\

#Extracción y limpieza de los datos de latitud y longitud
parkings_coordinates = parkings_ds.withColumn("geometry", sf.substring("geometry", 36, 40))\
    .withColumn("longitud", sf.split("geometry", ",")[0]) \
    .withColumn("latitud", sf.split("geometry", ",")[1]) \
    .withColumn("longitud", sf.expr("substring(longitud, 1, length(longitud)-1)")) \
    .withColumn("latitud", sf.expr("substring(latitud, 3, length(latitud)-5)")) \
    .drop("geometry")\
    .drop("datetime") #TODO ver si se mantiene este campo o solo nos interesa la fecha de actualizacion


#cuando un parking no se encuentra actualizado en tiempo real lleva asociado el siguiente timestamp
s = "01/01/1900T00:00:00"
unupdated = datetime.strptime(s, "%d/%m/%YT%H:%M:%S")

#Filtramos aquellos parkings que no estén actualizados o informando correctamente
parkings_filtered = parkings_coordinates.filter(sf.col("freeParking").isNotNull()) \
    .filter(sf.col("freeParking") != "") \
    .filter(sf.col("freeParking") != " ") \
    .filter(sf.col("lastUpd") != unupdated)


#get last year and month partitions to join with updated locations data
parking_statistics = sparkSession.read.parquet("hdfs://namenode:9000/output/madflow/parkings/occupation_statistics")
_park_year = str(parking_statistics.agg(sf.max("year")).collect()[0][0])
_park_month = str(parking_statistics.agg(sf.max("month")).collect()[0][0])
_park_day = str(parking_statistics.agg(sf.max("day")).collect()[0][0])

last_parking_statistics = sparkSession.read.parquet(
    "hdfs://namenode:9000/output/madflow/parkings/occupation_statistics/year="+_park_year+"/month="+_park_month+"/day="+_park_day)


# Join with occupation statistics to get the full information
parkings_filtered_statistics = parkings_filtered.join(last_parking_statistics,  on=["id"], how="inner")\
    .withColumn("freeParking", sf.col("freeParking").cast("float"))


# Create the metric of occupation level comparing the  real time freeParking with his historic max, min and avg values
parkings_occupation_metrics = parkings_filtered_statistics.withColumn("occupation_level", when(
    (sf.col("freeParking") <= sf.col("min_free_parking")), 4)
    .when(
    (sf.col("freeParking") > sf.col("min_free_parking")) & (sf.col("freeParking") < sf.col("avg_free_parking")), 3)
    .when(
    (sf.col("freeParking") >= sf.col("avg_free_parking")) & (sf.col("freeParking") < sf.col("max_free_parking")), 2)
    .when(
    (sf.col("freeParking") >= sf.col("max_free_parking")), 1).otherwise(0))

#occupation level 1 => VERY LOW
#occupation level 2 => LOW
#occupation level 3 => HIGH
#occupation level 4 => VERY HIGH
#occupation level 0 => NULL DATA


# Create partition fields
parkings_partition = parkings_occupation_metrics \
    .withColumn("year", year("lastUpd")) \
    .withColumn("month", month("lastUpd")) \
    .withColumn("day", dayofmonth("lastUpd")) \
    .withColumn("hour", hour("lastUpd")) \
    .withColumn("minute", minute("lastUpd"))



# Envío del resultado a kafka en micro-batches
queryToKafka = parkings_partition\
    .select(parkings_partition["id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='5 minutes') \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "parkings-druid-stream") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/stream/parkings/") \
    .outputMode("Append") \
    .start()
#old checkpoint path: /tmp/checkpoint/kafka/parkings/

queryToHDFS = parkings_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='5 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "/tmp/checkpoint/hdfs/stream/parkings") \
    .option("path", "hdfs://namenode:9000/output/madflow/parkings/stream_data") \
    .outputMode("append") \
    .start()

# old checkpoint path: .option("/tmp/checkpoint/hdfs/parkings-stream/")
# old hdfs path: .option("/user/alberto/madflow/parkings/stream")

"""
query = parkings_partition.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()
"""

sparkSession.streams.awaitAnyTermination()


