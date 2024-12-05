from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions as F

# Creación de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark_streaming_bicimad_application") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "512m") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/bicimad")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")
spark.sparkContext.setLogLevel('WARN')

bicimadSchema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("active", IntegerType()) \
    .add("datetime", TimestampType()) \
    .add("total_bases", IntegerType()) \
    .add("free_bases", IntegerType()) \
    .add("reservations", IntegerType()) \
    .add("longitude", FloatType()) \
    .add("latitude", FloatType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bicimad") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")
df = df.select(F.from_json(df.value, bicimadSchema).alias("bicimad_parsed"))
df = df.select("bicimad_parsed.*")
df = df.filter(F.col("active") == 1)
df = df.withColumn("occupancy", (F.col("free_bases") + F.col("reservations")) / F.col("total_bases"))

queryToKafka = df \
    .select(df["id"].cast('string').alias("key"),
            F.to_json(F.struct("id", "occupancy", "longitude", "latitude")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "bicimad-output-stream") \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .start()

queryToKafka.awaitTermination()
