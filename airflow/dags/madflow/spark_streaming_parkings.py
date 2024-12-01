import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime

kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_parking_topic_input = 'parkings'

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077") \
        .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}') \
        .appName("parkings-test-streaming")\
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

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

kafka_parkings_stream = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "parkings")\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

parkings_stream_parsed = kafka_parkings_stream.select(F.from_json(kafka_parkings_stream.value,
                                                                parkingSchema).alias("parkings_parsed"))

parkings_ds = parkings_stream_parsed.select("parkings_parsed.*")

parkings_coordinates = parkings_ds.withColumn("geometry", F.substring("geometry", 36, 40))\
    .withColumn("longitud", F.split("geometry", ",")[0]) \
    .withColumn("latitud", F.split("geometry", ",")[1]) \
    .withColumn("longitud", F.expr("substring(longitud, 1, length(longitud)-1)")) \
    .withColumn("latitud", F.expr("substring(latitud, 3, length(latitud)-5)")) \
    .drop("geometry")\
    .drop("datetime")

s = "01/01/1900T00:00:00"
unupdated = datetime.strptime(s, "%d/%m/%YT%H:%M:%S")

parkings_filtered = parkings_coordinates.filter(F.col("freeParking").isNotNull()) \
    .filter(F.col("freeParking") != "") \
    .filter(F.col("freeParking") != " ") \
    .filter(F.col("lastUpd") != unupdated)

#get last year and month partitions to join with updated locations data
parking_statistics = spark.read.parquet("hdfs://namenode:9000/output/madflow/parkings/occupation_statistics")
_park_year = str(parking_statistics.agg(F.max("year")).collect()[0][0])
_park_month = str(parking_statistics.agg(F.max("month")).collect()[0][0])
_park_day = str(parking_statistics.agg(F.max("day")).collect()[0][0])

last_parking_statistics = spark.read.parquet(
    "hdfs://namenode:9000/output/madflow/parkings/occupation_statistics/year="+_park_year+"/month="+_park_month+"/day="+_park_day)

# Join with occupation statistics to get the full information
parkings_filtered_statistics = parkings_filtered.join(last_parking_statistics,  on=["id"], how="inner")\
    .withColumn("freeParking", F.col("freeParking").cast("float"))

# Create the metric of occupation level comparing the  real time freeParking with his historic max, min and avg values
parkings_occupation_metrics = parkings_filtered_statistics.withColumn("occupation_level", F.when(
    (F.col("freeParking") <= F.col("min_free_parking")), 4)
    .when(
    (F.col("freeParking") > F.col("min_free_parking")) & (F.col("freeParking") < F.col("avg_free_parking")), 3)
    .when(
    (F.col("freeParking") >= F.col("avg_free_parking")) & (F.col("freeParking") < F.col("max_free_parking")), 2)
    .when(
    (F.col("freeParking") >= F.col("max_free_parking")), 1).otherwise(0))

#occupation level 1 => VERY LOW
#occupation level 2 => LOW
#occupation level 3 => HIGH
#occupation level 4 => VERY HIGH
#occupation level 0 => NULL DATA

parkings_partition = parkings_occupation_metrics \
    .withColumn("year", F.year("lastUpd")) \
    .withColumn("month", F.month("lastUpd")) \
    .withColumn("day", F.dayofmonth("lastUpd")) \
    .withColumn("hour", F.hour("lastUpd")) \
    .withColumn("minute", F.minute("lastUpd"))

queryToKafka = parkings_partition\
    .select(parkings_partition["id"].cast('string').alias("key"),
            F.to_json(F.struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='5 minutes') \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "parkings-output-stream") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/stream/parkings/") \
    .outputMode("Append") \
    .start()

queryToHDFS = parkings_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='5 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/parkings/stream_data") \
    .option("path", "hdfs://namenode:9000/output/madflow/parkings/stream_data") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()


