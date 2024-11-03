
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark_streaming_traffic.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.storagelevel import StorageLevel
from datetime import datetime

#KAFKA
kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_traffic_topic_input = 'traffic-topic'


#SPARK
sparkSession = SparkSession\
        .builder\
        .appName("traffic-spark-structured-streaming")\
        .getOrCreate()
         #.master("local[*]") \
         #.config('job.local.dir', 'file:/Users/alberto/TFM/MadFlow') \

sparkSession.sparkContext.setLogLevel('WARN')
#sparkSession.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")


# Definición del esquema de tráfico
trafficSchema = StructType()\
    .add("fecha_hora", StringType())\
    .add("idelem", StringType())\
    .add("intensidad", StringType())\
    .add("ocupacion", StringType())\
    .add("carga", StringType())\
    .add("nivelServicio", StringType())\
    .add("error", StringType())\
    .add("st_x", StringType())\
    .add("st_y", StringType())
#nivelServicio no está en el histórico pero en el histórico tenemos la velocidad media en los últimos 15 minutos


# Creción del dataStream de tráfico
kafka_traffic_stream = sparkSession\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_broker)\
    .option("subscribe", kafka_traffic_topic_input)\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
parsed = kafka_traffic_stream.select(from_json(kafka_traffic_stream.value, trafficSchema).alias("parsed_value"))
# Obtención de los campos incluidos en el mensaje
traffic_ds = parsed.select("parsed_value.*")\

# Limpieza y filtrado de los datos
traffic_cleaned = traffic_ds.filter(sf.col("error") == "N")\
    .withColumnRenamed("idelem", "id")\
    .withColumnRenamed("fecha_hora", "fecha")\
    .withColumnRenamed("nivelServicio", "fluidez")\
    .select("id", "fecha", "intensidad", "ocupacion", "carga", "fluidez")


# read dataframe of locations of points of measure
locations = sparkSession.read.parquet("hdfs://namenode:9000/output/madflow/traffic/locations")

#get last partitions to join with updated locations data
_year = str(locations.agg(sf.max("year")).collect()[0][0])
_month = str(locations.agg(sf.max("month")).collect()[0][0])
last_locations = sparkSession.read.parquet("hdfs://namenode:9000/output/madflow/traffic/locations/year="+_year+"/month="+_month) \
    .select("id", "longitud", "latitud", "distrito", "cod_cent", "nombre", "tipo_elem")

# traffic data enriched with location of point
traffic_locations = traffic_cleaned.join(last_locations,  on=["id"], how="inner")

# create partitition fields
traffic_locations_partitions = traffic_locations \
    .withColumn("year", sf.substring('fecha', 7, 4).cast("int")) \
    .withColumn("month", sf.substring('fecha', 4, 2).cast("int")) \
    .withColumn("day", sf.substring('fecha', 0, 2).cast("int")) \
    .withColumn("hour", sf.substring('fecha', 12, 2).cast("int")) \
    .withColumn("minute", sf.substring('fecha', 15, 2).cast("int"))

# Envío del resultado a kafka en micro-batches
queryToKafka = traffic_locations_partitions\
    .select(traffic_cleaned["id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='2 minutes') \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("topic", "traffic-druid-stream") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/stream/traffic") \
    .outputMode("Append") \
    .start()
#old checkpoint path: /tmp/checkpoint/kafka/traffic/stream/

# Escritura del resultado en HDFS
queryToHDFS = traffic_locations_partitions.writeStream \
    .format("parquet") \
    .trigger(processingTime='2 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "/tmp/checkpoint/hdfs/stream/traffic") \
    .option("path", "hdfs://namenode:9000/output/madflow/traffic/stream_data") \
    .outputMode("append") \
    .start()

# old checkpoint path: .option("/tmp/checkpoint/hdfs/traffic-stream/")
# old hdfs path: .option("/user/alberto/madflow/traffic/stream")

"""
# Imprimir por consola el resultado de cada micro-batch
query = traffic_locations_partitions.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()
"""

sparkSession.streams.awaitAnyTermination()


###################################################################################################################
#DESCOMENTAR ESTE BLOQUE PARA EJECUTAR LOS 3 PROCESOS STREAMING EN UNO SOLO
"""
kafka_parking_topic_input = 'parkings-topic'
kafka_bicimad_topic_input = 'bicimad-topic'

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
parking_statistics = sparkSession.read.parquet("/user/alberto/madflow/parkings/occupation_statistics")
_park_year = str(parking_statistics.agg(sf.max("year")).collect()[0][0])
_park_month = str(parking_statistics.agg(sf.max("month")).collect()[0][0])
_park_day = str(parking_statistics.agg(sf.max("day")).collect()[0][0])

last_parking_statistics = sparkSession.read.parquet(
    "/user/alberto/madflow/parkings/occupation_statistics/year="+_park_year+"/month="+_park_month+"/day="+_park_day)


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
queryParkingToKafka = parkings_partition\
    .select(parkings_partition["id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='2 minutes') \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("topic", "parkings-druid-stream") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/stream/parkings/") \
    .outputMode("Append") \
    .start()


queryParkingToHDFS = parkings_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='2 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "/tmp/checkpoint/hdfs/stream/parkings") \
    .option("path", "/user/alberto/madflow/parkings/stream_data") \
    .outputMode("append") \
    .start()


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
queryBicimadToKafka = bicimad_partition\
    .select(bicimad_partition["id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='1 minutes') \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("topic", "bicimad-druid-stream") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/stream/bicimad/") \
    .outputMode("Append") \
    .start()

queryBicimadToHDFS = bicimad_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='1 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "/tmp/checkpoint/hdfs/stream/bicimad") \
    .option("path", "/user/alberto/madflow/bicimad_data") \
    .outputMode("append") \
    .start()
"""
###################################################################################################################
