import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, concat_ws, lit, to_json, struct
import json


class ETL:
    def __init__(self, spark, metadata, kafka_broker, hdfs_host, hdfs_port):
        self.spark = spark
        self.metadata = metadata
        self.kafka_broker = kafka_broker
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

    def get_hdfs_path(self, path):
        return f"hdfs://{self.hdfs_host}:{self.hdfs_port}{path}"

    def read_hdfs(self, path, format):
        if format == 'JSON':
            return self.spark.read.json(path)

    def validate_fields(self, df, validations):
        # By default all rows are valid
        df = df.withColumn("failed_validations", lit(""))

        for validation in validations:
            field = validation["field"]
            field_validations = validation["validations"]
            if "notEmpty" in field_validations:
                df = df.withColumn(
                    "failed_validations",
                    when((col(field) != "") & col(field).isNotNull(), col("failed_validations")).otherwise(
                        concat_ws(",", col("failed_validations"), lit(f"notEmpty validation failed for {field}"))
                    )
                )

            if "notNull" in field_validations:
                df = df.withColumn(
                    "failed_validations",
                    when(col(field).isNotNull(), col("failed_validations")).otherwise(
                        concat_ws(",", col("failed_validations"), lit(f"notNull validation failed for {field}"))
                    )
                )

        df.cache()
        valid_df = df.filter(col("failed_validations") == "").drop("failed_validations")
        invalid_df = df.filter(col("failed_validations") != "")
        return {'ok': valid_df, 'ko': invalid_df}

    def add_fields(self, df, fields):
        for field in fields:
            field_name = field["name"]
            field_function = field["function"]

            if field_function == "current_timestamp":
                df = df.withColumn(field_name, current_timestamp())

        return df

    def to_kafka(self, df, topic):
        df \
            .select(to_json(struct("*")).alias("value")) \
            .selectExpr("CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("topic", topic) \
            .save()

    def to_file(self, df, path, format, save_mode):
        df \
            .write \
            .format(format) \
            .mode(save_mode) \
            .save(path)

    def run(self):
        # Iterate over dataflows
        for dataflow in self.metadata["dataflows"]:

            # Read sources
            sources = dataflow["sources"]
            inputs = dict()
            for source in sources:
                source_name = source["name"]
                path = source["path"].replace("*", "")
                format_type = source["format"]
                hdfs_path = self.get_hdfs_path(path)
                df = self.read_hdfs(hdfs_path, format_type)
                inputs[source_name] = df

            # Apply transformations
            transformations = dataflow["transformations"]
            for transformation in transformations:
                transformation_type = transformation["type"]
                transformation_name = transformation["name"]
                params = transformation["params"]
                input = params["input"]

                if transformation_type == "validate_fields":
                    validations = params["validations"]
                    validation_result_dfs = self.validate_fields(inputs[input], validations)
                    inputs[f"{transformation_name}_ok"] = validation_result_dfs["ok"]
                    inputs[f"{transformation_name}_ko"] = validation_result_dfs["ko"]

                elif transformation_type == "add_fields":
                    fields = params["addFields"]
                    df = self.add_fields(inputs[input], fields)
                    inputs[transformation_name] = df

            # Write data to sinks
            sinks = dataflow["sinks"]
            for sink in sinks:
                input = sink["input"]
                format_type = sink["format"]
                df = inputs[input]

                if format_type == "KAFKA":
                    topics = sink["topics"]
                    for topic in topics:
                        self.to_kafka(df, topic)

                elif format_type == "JSON":
                    save_mode = sink["saveMode"]
                    paths = sink["paths"]
                    for path in paths:
                        hdfs_path = self.get_hdfs_path(path)
                        self.to_file(df, hdfs_path, "json", save_mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=str, help="ETL metadata")
    parser.add_argument("--kafka_broker", type=str, help="Kafka broker")
    parser.add_argument("--hdfs_host", type=str, help="HDFS host")
    parser.add_argument("--hdfs_port", type=str, help="HDFS port")

    args = parser.parse_args()

    spark = SparkSession.builder.appName("SparkPipeline").getOrCreate()
    ETL(
        spark=spark,
        metadata=json.loads(args.metadata),
        kafka_broker=args.kafka_broker,
        hdfs_host=args.hdfs_host,
        hdfs_port=args.hdfs_port
    ).run()
    spark.stop()