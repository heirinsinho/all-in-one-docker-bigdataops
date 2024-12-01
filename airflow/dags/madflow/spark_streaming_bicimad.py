import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import functions as F

# Creación de la SparkSession
spark = SparkSession\
        .builder\
        .appName("spark_streaming_bicimad_application") \
        .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}') \
        .master("spark://spark-master:7077")\
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")

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

kafka_bicimad_stream = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "bicimad")\
    .option("failOnDataLoss", False)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

bicimad_stream_parsed = kafka_bicimad_stream.select(F.from_json(kafka_bicimad_stream.value,
                                                              bicimadSchema).alias("bicimad_parsed"))
bicimadc_ds = bicimad_stream_parsed.select("bicimad_parsed.*")\

bicimadc_ds = bicimadc_ds.withColumnRenamed("light", "occupation")

bicimad_filtered = bicimadc_ds.filter(F.col("activate") == "1")

bicimad_coordinates = bicimad_filtered.withColumn("geometry", F.substring("geometry", 35, 40))\
    .withColumn("longitud", F.split("geometry", ",")[0]) \
    .withColumn("latitud", F.split("geometry", ",")[1]) \
    .withColumn("latitud", F.expr("substring(latitud, 2, length(latitud)-3)")) \
    .drop("geometry")

bicimad_partition = bicimad_coordinates\
    .withColumn("year", F.year("datetime")) \
    .withColumn("month", F.month("datetime")) \
    .withColumn("day", F.dayofmonth("datetime")) \
    .withColumn("hour", F.hour("datetime")) \
    .withColumn("minute", F.minute("datetime"))

queryToKafka = bicimad_partition\
    .select(bicimad_partition["id"].cast('string').alias("key"),
            F.to_json(F.struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='3 minutes') \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "bicimad-output-stream") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/stream/bicimad") \
    .outputMode("Append") \
    .start()


queryToHDFS = bicimad_partition.writeStream \
    .format("parquet") \
    .trigger(processingTime='3 minutes') \
    .partitionBy("year", "month", "day") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/stream/bicimad") \
    .option("path", "hdfs://namenode:9000/output/madflow/bicimad_data") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()


