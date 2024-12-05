from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "1") \
    .appName("traffic-spark-structured-streaming") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/traffic")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.sparkContext.setLogLevel('WARN')

trafficSchema = StructType() \
    .add("id", StringType()) \
    .add("intensity", StringType()) \
    .add("occupancy", StringType()) \
    .add("load", StringType())

kafka_traffic = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
kafka_traffic = kafka_traffic.select(F.from_json(kafka_traffic.value, trafficSchema).alias("parsed_value"))
kafka_traffic = kafka_traffic.select("parsed_value.*")
kafka_traffic = kafka_traffic.withColumn("intensity", F.col("intensity").cast("float"))

locations = spark.read.parquet("hdfs://namenode:9000/madflow/traffic/locations")
traffic_history = spark.read.parquet("hdfs://namenode:9000/madflow/traffic/history")
traffic_history = (traffic_history
                   .filter(F.col("month") == F.month(F.current_timestamp())))
traffic_history = traffic_history.withColumn("diff_hour", F.abs(F.hour(F.col("datetime")) - F.hour(F.current_timestamp())))
traffic_history = traffic_history.filter(F.abs(F.col("diff_hour")) <= 1)

grouped_traffic_history = traffic_history.groupBy("id").agg(F.max("intensity").alias("intensity_max"))

# traffic data enriched with location of point
df = kafka_traffic.join(locations, on="id", how="inner")
df = df.join(grouped_traffic_history, on="id", how="inner")

df = df.withColumn("occupancy", F.col("intensity")/F.col("intensity_max"))
df = df.withColumn("occupancy", F.when(F.col("occupancy") > 1, 1).otherwise(F.col("occupancy")))


queryToKafka = df \
    .select(df["id"].cast('string').alias("key"),
            F.to_json(F.struct("id", "occupancy", "longitude", "latitude")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "traffic-output-stream") \
    .trigger(processingTime='20 seconds') \
    .outputMode("append") \
    .start()

queryToKafka.awaitTermination()