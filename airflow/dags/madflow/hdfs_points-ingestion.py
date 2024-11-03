from pyspark.sql.types import StructType, StringType, DateType, FloatType
from pyspark.sql.functions import lit, col, udf, year, month
from datetime import datetime
import os
from pyspark.sql import SparkSession

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 hdfs_points-ingestion.py
spark = SparkSession\
        .builder\
        .appName("hdfs_points-ingestion-application")\
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

points_csv_path = os.path.join('Users/alberto/TFM/Madflow/data', 'points_traffic_2020-03-31.csv')
date = "2020-03-31"

func = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())


schema = StructType() \
      .add("tipo_elem", StringType(), True) \
      .add("distrito", StringType(), True) \
      .add("id", StringType(), True) \
      .add("cod_cent", StringType(), True) \
      .add("nombre", StringType(), True) \
      .add("utm_x", StringType(), True) \
      .add("utm_y", StringType(), True) \
      .add("longitud", FloatType(), True) \
      .add("latitud", FloatType(), True)

points = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("delimiter", ";") \
    .schema(schema) \
    .load("file:///" + points_csv_path)


points_date = points.withColumn("date", lit(date)).withColumn('date', func(col('date')))
points_partition = points_date.withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date")))


points_partition.write.partitionBy("year","month").mode("append").parquet("/user/alberto/madflow/traffic/locations")
