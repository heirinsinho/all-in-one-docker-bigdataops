from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType

spark = SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "512m") \
    .appName("parkings-spark-structured-streaming") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/parkings")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")
spark.conf.set("spark.sql.shuffle.partitions", 2)
spark.sparkContext.setLogLevel('WARN')

parkingsSchema = StructType() \
    .add("datetime", TimestampType()) \
    .add("id", StringType()) \
    .add("free_slots", IntegerType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "parkings") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
df = df.select(F.from_json(df.value, parkingsSchema).alias("parsed_value"))

df = df.select("parsed_value.*")

static_df = spark.read.parquet("hdfs://namenode:9000/madflow/parkings/details")

df = df.filter(F.col("free_slots").isNotNull())
df = df.join(static_df, on="id")
df = df.withColumn("occupancy", 1-(F.col("free_slots")/F.col("total_slots")))

queryToKafka = df \
    .select(df["id"].cast('string').alias("key"),
            F.to_json(F.struct("id", "occupancy", "longitude", "latitude")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "parkings-output-stream") \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .start()

queryToKafka.awaitTermination()