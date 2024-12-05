from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .appName("madflow-spark-structured-streaming") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/main")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "True")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.sparkContext.setLogLevel('WARN')

kafkaSchema = StructType() \
    .add("id", StringType()) \
    .add("occupancy", FloatType()) \
    .add("longitude", FloatType()) \
    .add("latitude", FloatType()) \

static_poi = spark.read.parquet("hdfs://namenode:9000/madflow/poi")

kafka_traffic = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic-output-stream") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

kafka_parkings = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "parkings-output-stream") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

kafka_bicimad = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bicimad-output-stream") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

kafka_bicimad = kafka_bicimad.select(F.from_json(kafka_bicimad.value, kafkaSchema).alias("parsed_value"))
kafka_bicimad = kafka_bicimad.select("parsed_value.*")

kafka_parkings = kafka_parkings.select(F.from_json(kafka_parkings.value, kafkaSchema).alias("parsed_value"))
kafka_parkings = kafka_parkings.select("parsed_value.*")

kafka_traffic = kafka_traffic.select(F.from_json(kafka_traffic.value, kafkaSchema).alias("parsed_value"))
kafka_traffic = kafka_traffic.select("parsed_value.*")

cond1 = F.sqrt(F.pow(static_poi.longitude-kafka_traffic.longitude, 2) + F.pow(static_poi.latitude-kafka_traffic.latitude, 2)) < 0.1
df1 = static_poi.join(kafka_traffic, on=cond1).drop(kafka_traffic.longitude, kafka_traffic.latitude, kafka_traffic.id)

cond2 = F.sqrt(F.pow(static_poi.longitude-kafka_parkings.longitude, 2) + F.pow(static_poi.latitude-kafka_parkings.latitude, 2)) < 0.1
df2 = static_poi.join(kafka_parkings, on=cond2).drop(kafka_parkings.longitude, kafka_parkings.latitude, kafka_parkings.id)

cond3 = F.sqrt(F.pow(static_poi.longitude-kafka_bicimad.longitude, 2) + F.pow(static_poi.latitude-kafka_bicimad.latitude, 2)) < 0.1
df3 = static_poi.join(kafka_bicimad, on=cond3).drop(kafka_bicimad.longitude, kafka_bicimad.latitude, kafka_bicimad.id)

df = df1.unionByName(df2).unionByName(df3)

df = df.filter(df.occupancy.isNotNull()).groupBy("id", "longitude", "latitude").agg(F.avg("occupancy").alias("occupancy"))

queryToKafka = df \
    .select(F.to_json(F.struct("id", "longitude", "latitude", "occupancy")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("topic", "madflow-output-stream") \
    .trigger(processingTime='10 seconds') \
    .outputMode("update") \
    .start()

queryToKafka.awaitTermination()
