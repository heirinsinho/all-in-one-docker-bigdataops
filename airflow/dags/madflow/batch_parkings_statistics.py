from pyspark.sql import functions as sf
from datetime import datetime
from pyspark.sql import SparkSession

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 batch_parkings_statistics.py
spark = SparkSession\
        .builder\
        .appName("batch-parkings-statistics-application")\
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

execution_date = datetime.now()
execution_year = execution_date.year
execution_month = execution_date.month
execution_day = execution_date.day

#En la primera ejecución usar la siguiente ruta que contiene una muestra de valores para generar la tabla inicial
#hdfs_parkings_streaming_path = ("/user/alberto/test/parking/year=2020")
hdfs_parkings_streaming_path = ("/user/alberto/madflow/parkings/stream_data")


#TODO Pensar si leo el histórico completo o solo las particiones más recientes cada vez que actualizo la tabla
parkings = spark.read.parquet(hdfs_parkings_streaming_path)

"""
Este proceso genera una tabla con el máximo, mínimo y media de plazas libres leyendo del histórico guardado en hdfs.
De esta forma, aunque no tenemos la cifra de plazas totales de las que dispone cada parking, podemos generar unos valores
relativos sobre los que crear una métrica
"""
parkings_statistics = parkings.withColumn("freeParking", sf.col("freeParking").cast("float"))\
    .groupBy("id")\
    .agg(
    sf.mean('freeParking').alias("avg_free_parking"),
    sf.max('freeParking').alias("max_free_parking"),
    sf.min('freeParking').alias("min_free_parking"))

parkings_statistics_partition = parkings_statistics.withColumn("year", sf.lit(execution_year))\
    .withColumn("month", sf.lit(execution_month))\
    .withColumn("day", sf.lit(execution_day))


#Forzamos para que escriba el resultado en una sola partición ya que es una tabla muy pequeña
parkings_statistics_partition.repartition(1)\
    .write\
    .partitionBy("year", "month", "day")\
    .mode("append")\
    .parquet("/user/alberto/madflow/parkings/occupation_statistics")

