from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077")\
        .appName("hdfs_historic_traffic_ingestion_application")\
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

execution_date = datetime.now()
execution_year = execution_date.year
execution_month = execution_date.month
execution_day = execution_date.day

parkings = spark.read.parquet("hdfs://namenode:9000/output/madflow/parkings/stream_data")

parkings_statistics = parkings.withColumn("freeParking", F.col("freeParking").cast("float"))\
    .groupBy("id")\
    .agg(
    F.mean('freeParking').alias("avg_free_parking"),
    F.max('freeParking').alias("max_free_parking"),
    F.min('freeParking').alias("min_free_parking"))

parkings_statistics_partition = parkings_statistics.withColumn("year", F.lit(execution_year))\
    .withColumn("month", F.lit(execution_month))\
    .withColumn("day", F.lit(execution_day))

parkings_statistics_partition.coalesce(1)\
    .write\
    .partitionBy("year", "month", "day")\
    .mode("append")\
    .parquet("hdfs://namenode:9000/output/madflow/parkings/occupation_statistics")

