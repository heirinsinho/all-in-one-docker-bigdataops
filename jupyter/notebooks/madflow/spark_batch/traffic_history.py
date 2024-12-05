from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077") \
        .appName("hdfs_historic_traffic_ingestion_application") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.files.openCostInBytes", "4MB")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

schema = StructType() \
      .add("id", StringType(), True) \
      .add("fecha", StringType(), True) \
      .add("tipo_elem", StringType(), True) \
      .add("intensidad", StringType(), True) \
      .add("ocupacion", StringType(), True) \
      .add("carga", StringType(), True) \
      .add("vmed", StringType(), True) \
      .add("error", StringType(), True) \
      .add("periodo_integracion", StringType(), True)

traffic = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("delimiter", ";") \
    .schema(schema) \
    .load("hdfs://namenode:9000/input/madflow/traffic/history") \
    .filter(F.col("error") == "N") \
    .withColumn("datetime", F.to_timestamp(F.col("fecha"))) \
    .withColumn("year", F.year(F.col("fecha"))) \
    .withColumn("month", F.month(F.col("fecha"))) \
    .withColumn("day", F.dayofmonth(F.col("fecha"))) \
    .selectExpr("id", "datetime", "year", "month", "day", "intensidad as intensity", "ocupacion as occupancy", "carga as load")

traffic.write.partitionBy("year", "month").mode("append").parquet("hdfs://namenode:9000/madflow/traffic/history")

spark.stop()