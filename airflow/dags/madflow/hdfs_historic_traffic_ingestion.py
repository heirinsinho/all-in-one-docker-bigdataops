import os
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 hdfs_historic_traffic_ingestion.py


spark = SparkSession\
        .builder\
        .appName("hdfs_historic_traffic_ingestion_application")\
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#Escoger ruta del mes a cargar
traffic_historic_csv_path = os.path.join('Users/alberto/TFM/Madflow/data/historic', '11-2019.csv')
_year = traffic_historic_csv_path[-8:-4]
_month = traffic_historic_csv_path[-11:-9]


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
    .load("file:///" + traffic_historic_csv_path) \
    .filter(sf.col("error") == "N") \
    .select("fecha", "id", "intensidad", "ocupacion", "carga", "tipo_elem", "vmed") \
    .withColumnRenamed("vmed", "velocidad_media")

"""
print(traffic.count())
#11401995
"""

locations = spark.read.parquet("/user/alberto/madflow/traffic/locations/year="+_year+"/month="+_month) \
    .select("id", "longitud", "latitud", "distrito", "cod_cent", "nombre")

"""
print(locations.count())
#4353
"""

traffic_locations = traffic.join(locations,  on=["id"], how="inner")

traffic_locations_partitions = traffic_locations\
    .withColumn("year", sf.substring('fecha', 0, 4)) \
    .withColumn("month", sf.substring('fecha', 6, 2)) \
    .withColumn("day", sf.substring('fecha', 9, 2)) \
    .withColumn("hour", sf.substring('fecha', 12, 2)) \
    .withColumn("minute", sf.substring('fecha', 15, 2))

print("#################")
print(traffic_locations_partitions.count())
#11401984



traffic_locations_partitions.write.partitionBy("year", "month", "day").mode("append").parquet("/user/alberto/madflow/traffic/history")
