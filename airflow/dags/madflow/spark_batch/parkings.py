import requests

from pyspark.sql import SparkSession


def get_access_token():
    url_login = "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/"

    headers = {
        'X-ClientId': "4614aca0-0a67-4bb9-b912-87b9bd62003c",
        'passKey': "6D42B608EFDB34D1EE2623846AF4CFB3B88161A28C6CF6973D9569FA746757BF907C058CA211FD8F5787A918E928F30D82F0EFFE8F219F4A54BF53BA10549F06"
    }

    res = requests.get(url_login, headers=headers)
    return res.json()["data"][0]["accessToken"]


def fetch_data():
    url_parkings = "https://openapi.emtmadrid.es/v3/citymad/places/parkings/availability"

    headers = {'accessToken': get_access_token()}

    res = requests.get(url_parkings, headers=headers)
    data = res.json()["data"]
    parkings = []
    for record in data:

        try:
            current_url = "https://openapi.emtmadrid.es/v1/citymad/places/parking/{parking_id}/ES".format(
                parking_id=record["id"])
            res = requests.get(current_url, headers=headers)

            record2 = res.json()["data"]

        except Exception:
            record2 = None

        if not record2:
            continue

        features = record2[0].get("features", []) or []
        total = next((int(occ.get("content")) for occ in features if occ.get("name") == "Total"), None)

        # occupation = record2[0].get("occupation")
        # free_slots2 = occupation[0].get("free") if occupation else None
        if total:
            x = {"id": record["id"],
                 "latitude": record["geometry"]["coordinates"][1],
                 "longitude": record["geometry"]["coordinates"][0],
                 "name": record["name"],
                 "total_slots": total}

            parkings.append(x)

    return parkings


def append_new_parkings_details(df):
    schema = ["id", "latitude", "longitude", "name", "total_slots"]
    data = fetch_data()
    new_df = spark.createDataFrame(data, schema=schema)

    new_details_df = new_df.join(df, on="id", how="left_anti")

    # Step 4: Append new locations
    if new_details_df.count() > 0:
        new_details_df.coalesce(1).write.mode("append").parquet("hdfs://namenode:9000/madflow/parkings/details")


if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .master("spark://spark-master:7077")
             .config("spark.cores.max", "2") \
             .config("spark.executor.cores", "1") \
             .config("spark.executor.memory", "512m") \
             .appName("parkings_batch")
             .getOrCreate())

    spark.sparkContext.setLogLevel('WARN')

    try:
        df = spark.read.parquet("hdfs://namenode:9000/madflow/parkings/details")

    except Exception:

        schema = "id integer, latitude float, longitude float, name string, total_slots integer"
        df = spark.createDataFrame([], schema=schema)

    finally:
        append_new_parkings_details(df)
        spark.stop()
