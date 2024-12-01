import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from datetime import datetime

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077") \
        .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}') \
        .appName("traffic-spark-structured-streaming")\
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

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

kafka_traffic_stream = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "traffic")\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
parsed = kafka_traffic_stream.select(F.from_json(kafka_traffic_stream.value, trafficSchema).alias("parsed_value"))

traffic_ds = parsed.select("parsed_value.*")

traffic_cleaned = traffic_ds.filter(F.col("error") == "N")\
    .withColumnRenamed("idelem", "id")\
    .withColumnRenamed("fecha_hora", "fecha")\
    .withColumnRenamed("nivelServicio", "fluidez")\
    .select("id", "fecha", "intensidad", "ocupacion", "carga", "fluidez")

locations = spark.read.parquet("hdfs://namenode:9000/output/madflow/traffic/locations")

_year = str(locations.agg(F.max("year")).collect()[0][0])
_month = str(locations.agg(F.max("month")).collect()[0][0])
last_locations = spark.read.parquet("hdfs://namenode:9000/output/madflow/traffic/locations/year="+_year+"/month="+_month) \
    .select("id", "longitud", "latitud", "distrito", "cod_cent", "nombre", "tipo_elem")

# traffic data enriched with location of point
traffic_locations = traffic_cleaned.join(last_locations,  on=["id"], how="inner")

# create partitition fields
traffic_locations_partitions = traffic_locations \
    .withColumn("year", F.substring('fecha', 7, 4).cast("int")) \
    .withColumn("month", F.substring('fecha', 4, 2).cast("int")) \
    .withColumn("day", F.substring('fecha', 0, 2).cast("int")) \
    .withColumn("hour", F.substring('fecha', 12, 2).cast("int")) \
    .withColumn("minute", F.substring('fecha', 15, 2).cast("int"))

# Envío del resultado a kafka en micro-batches
queryToKafka = traffic_locations_partitions\
    .select(traffic_cleaned["id"].cast('string').alias("key"),
            F.to_json(F.struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='2 minutes') \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("topic", "traffic-output-stream") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/stream/traffic") \
    .outputMode("Append") \
    .start()

queryToHDFS = traffic_locations_partitions.writeStream \
    .format("parquet") \
    .trigger(processingTime='2 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/stream/traffic") \
    .option("path", "hdfs://namenode:9000/output/madflow/traffic/stream_data") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
