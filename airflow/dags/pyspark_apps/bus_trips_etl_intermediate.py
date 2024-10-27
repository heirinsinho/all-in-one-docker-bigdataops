import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.window import Window


def main(df):

    # Filter for only regular services
    df = df.filter(F.col("trip_type") == "regular")

    w = Window.partitionBy("line_number")

    # Convert necessary columns to numeric types
    df = df.withColumn("trip_duration_hours",
                       F.coalesce(F.col("trip_duration_hours"), F.avg("trip_duration_hours").over(w)).cast("float")) \
        .withColumn("delay_start_minutes", F.coalesce(F.col("delay_start_minutes"), F.lit(0)).cast("float")) \
        .withColumn("delay_end_minutes", F.coalesce(F.col("delay_end_minutes"), F.lit(0)).cast("float")) \
        .withColumn("travelled_distance_km",
                    F.coalesce(F.col("travelled_distance_km"), F.avg("trip_duration_hours").over(w)).cast("float"))

    # Calculate total delay, delay percentage, and average speed
    df = df.withColumn("total_delay_minutes", F.col("delay_start_minutes") + F.col("delay_end_minutes")) \
        .withColumn("delay_percentage", (F.col("total_delay_minutes") / (F.col("trip_duration_hours") * 60)) * 100) \
        .withColumn("speed_kmh", F.round(F.col("travelled_distance_km") / F.col("trip_duration_hours"), 2))

    # Fake the dates. If year 2020 we use month = 8 If year 2019, month = 7 and drop year
    df = df.withColumn("month", F.when(F.col("year") == 2019, 7).otherwise(8))
    df = df.withColumn("year", F.lit(2024))
    df = df.withColumn("date", F.make_date(F.col("year"), F.col("month"), F.col("day")))
    df = df.drop("trip_type", "year", "day")

    # Save the intermediate table with partitioning by `day`
    df.coalesce(1).write.mode("overwrite").partitionBy("date").bucketBy(4, "line_number").format("parquet").option(
        "path", "hdfs://namenode:9000/output/bus_trips/intermediate").saveAsTable("intermediate")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SparkPipeline").getOrCreate()
    df = spark.read \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/input/data/bus_trips.csv")

    main(df)

    spark.stop()