from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F

spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077")\
        .appName("hdfs_historic_traffic_ingestion_application")\
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

_year = "2020"
_month = "3"

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
    .load("hdfs://namenode:9000/data/input/madflow/points_traffic_2020-10-31.csv") \
    .filter(F.col("error") == "N") \
    .select("fecha", "id", "intensidad", "ocupacion", "carga", "tipo_elem", "vmed") \
    .withColumnRenamed("vmed", "velocidad_media")

locations = spark.read.parquet("hdfs://namenode:9000/output/madflow/traffic/locations/year="+_year+"/month="+_month) \
    .select("id", "longitud", "latitud", "distrito", "cod_cent", "nombre")

traffic_locations = traffic.join(locations,  on=["id"], how="inner")

traffic_locations_partitions = traffic_locations\
    .withColumn("year", F.substring('fecha', 0, 4)) \
    .withColumn("month", F.substring('fecha', 6, 2)) \
    .withColumn("day", F.substring('fecha', 9, 2)) \
    .withColumn("hour", F.substring('fecha', 12, 2)) \
    .withColumn("minute", F.substring('fecha', 15, 2))

traffic_locations_partitions.write.partitionBy("year", "month", "day").mode("append").parquet("hdfs://namenode:9000/output/madflow/traffic/history")
