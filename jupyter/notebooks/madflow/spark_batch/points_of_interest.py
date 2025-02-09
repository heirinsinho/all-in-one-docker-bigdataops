import requests
import os

from pyspark.sql import SparkSession

from dotenv import load_dotenv

load_dotenv()


def get_access_token():
    url_login = "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/"

    headers = {
        'X-ClientId': os.environ["X_CLIENT_ID"],
        'passKey': os.environ["PASS_KEY"]
    }

    res = requests.get(url_login, headers=headers)
    return res.json()["data"][0]["accessToken"]


def fetch_data():
    payload = {"coordinates": {"longitude": -3.7004000, "latitude": 40.4146500, "radius": 10000},
               "family": [{"familyCode": "101"}]}

    headers = {
        'Content-Type': "application/json",
        'accessToken': get_access_token()
    }
    try:

        res = requests.post("https://openapi.emtmadrid.es/v1/citymad/places/arroundxy/ES/", json=payload,
                            headers=headers)

        raw_poi = next(
            (source for source in res.json()["data"] if
             source["sourceName"] == 'Point of Interest from MADRID-DESTINO'),
            {}).get("data", [])

        output = [[record["category"], float(record["longitude"]), float(record["latitude"]), int(record["idPoi"]),
                   record["name"]] for record
                  in raw_poi]

    except Exception:
        output = []

    return output


def append_new_poi(df):
    schema = "category string, longitude float, latitude float, id integer, name string"
    data = fetch_data()
    new_df = spark.createDataFrame(data, schema=schema)

    new_locations_df = new_df.join(df, on="id", how="left_anti")

    # Step 4: Append new locations
    if new_locations_df.count() > 0:
        new_locations_df.coalesce(1).write.mode("append").parquet("hdfs://namenode:9000/madflow/poi")


if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .master("spark://spark-master:7077")
             .config("spark.cores.max", "2") \
             .config("spark.executor.cores", "1") \
             .config("spark.executor.memory", "512m") \
             .appName("hdfs_points-ingestion-application")
             .getOrCreate())

    spark.sparkContext.setLogLevel('WARN')

    try:
        df = spark.read.parquet("hdfs://namenode:9000/madflow/poi")

    except Exception:

        schema = "category string, longitude float, latitude float, id integer, name string"
        df = spark.createDataFrame([], schema=schema)

    finally:
        append_new_poi(df)
        spark.stop()
