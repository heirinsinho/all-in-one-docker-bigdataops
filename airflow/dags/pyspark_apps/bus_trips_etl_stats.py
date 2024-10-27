import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.window import Window


def main(df):
    window_spec = Window.partitionBy("line_number").rowsBetween(Window.unboundedPreceding, 0)

    df = df.groupby("date", "line_number").agg(
        F.avg("delay_percentage").alias("delay_percentage"),
        F.avg("trip_duration_hours").alias("trip_duration_hours"),
    )
    # Join overall statistics with trip summary and calculate ratios
    trip_summary_ratios_df = df.withColumn("avg_delay_ratio",
                                           F.col("delay_percentage") / F.avg("delay_percentage").over(window_spec)) \
        .withColumn("max_delay_ratio", F.col("delay_percentage") / F.max("delay_percentage").over(window_spec)) \
        .withColumn("avg_trip_duration_hours",
                    F.col("trip_duration_hours") / F.avg("trip_duration_hours").over(window_spec)) \
        .withColumn("max_trip_duration_hours",
                    F.col("trip_duration_hours") / F.max("trip_duration_hours").over(window_spec))

    trip_summary_ratios_df = trip_summary_ratios_df.drop("delay_percentage", "trip_duration_hours")

    # Save results as the final output
    trip_summary_ratios_df.write.partitionBy("date").mode("overwrite").parquet(
        "hdfs://namenode:9000/output/bus_trips/stats")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SparkPipeline").getOrCreate()
    df = spark.read.parquet("hdfs://namenode:9000/output/bus_trips/intermediate")

    main(df)

    spark.stop()