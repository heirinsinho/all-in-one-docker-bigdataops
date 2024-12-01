from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DateType, FloatType
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077")\
        .appName("hdfs_points-ingestion-application")\
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

date = "2020-03-31"

func = F.udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())


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
    .load("hdfs://namenode:9000/data/input/madflow/points_traffic_2020-10-31.csv")


points_date = points.withColumn("date", F.lit(date)).withColumn('date', func(F.col('date')))
points_partition = points_date.withColumn("year", F.year(F.col("date"))) \
    .withColumn("month", F.month(F.col("date")))


points_partition.write.partitionBy("year","month").mode("append").parquet("hdfs://namenode:9000/output/madflow/traffic/locations")
