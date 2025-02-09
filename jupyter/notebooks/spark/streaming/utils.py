from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType


def flatten_df(df):
    json_schema = StructType(
        [
            StructField("customerId", StringType(), True),
            StructField(
                "data",
                StructType(
                    [
                        StructField(
                            "devices",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("deviceId", StringType(), True),
                                        StructField("measure", StringType(), True),
                                        StructField("status", StringType(), True),
                                        StructField("temperature", LongType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
            StructField("eventId", StringType(), True),
            StructField("eventOffset", LongType(), True),
            StructField("eventPublisher", StringType(), True),
            StructField("eventTime", StringType(), True),
        ]
    )

    df = df.withColumn("value", F.expr("cast(value as string)"))
    df = df.withColumn(
        "values_json", F.from_json(F.col("value"), json_schema)
    ).selectExpr("values_json.*")
    exploded_df = df.withColumn("data_devices", F.explode("data.devices"))
    flattened_df = (
        exploded_df.drop("data")
        .withColumn("deviceId", F.col("data_devices.deviceId"))
        .withColumn("measure", F.col("data_devices.measure"))
        .withColumn("status", F.col("data_devices.status"))
        .withColumn("temperature", F.col("data_devices.temperature"))
        .drop("data_devices")
    )

    return flattened_df
