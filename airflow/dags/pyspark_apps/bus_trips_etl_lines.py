import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(df):

    df_lines = df.withColumn("line_key", F.when(F.col("origin") < F.col("destination"),
                                                F.concat_ws("-", F.col("origin"), F.col("destination")))
                             .otherwise(F.concat_ws("-", F.col("destination"), F.col("origin"))))

    # Aggregate data to keep only unique two-way records per line
    lines_df = df_lines.select("company", "line_number", "line_key").distinct()

    # Save this as a second intermediate table
    lines_df.write.mode("overwrite").parquet("hdfs://namenode:9000/output/bus_trips/lines")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SparkPipeline").getOrCreate()
    df = spark.read \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/input/data/bus_trips.csv")

    main(df)

    spark.stop()