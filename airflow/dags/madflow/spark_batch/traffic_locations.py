from functools import lru_cache
from html.parser import HTMLParser
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (SparkSession
         .builder
         .master("spark://spark-master:7077")
         .config("spark.cores.max", "2") \
         .config("spark.executor.cores", "1") \
         .config("spark.executor.memory", "512m") \
         .appName("hdfs_traffic_locations")
         .getOrCreate())

spark.sparkContext.setLogLevel('WARN')


class LinkExtractor(HTMLParser):
    url = "https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=ee941ce6ba6d3410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD"

    def __init__(self):
        super().__init__()
        self.links = []
        with urlopen(self.url) as response:
            html_content = response.read().decode('utf-8')
            self.feed(html_content)

    def handle_starttag(self, tag, attrs):
        # Check for 'a' tags and attributes
        if tag == 'a':
            attr_dict = dict(attrs)
            if "class" in attr_dict and "asociada-link ico-csv" in attr_dict["class"]:
                href = attr_dict.get("href")
                if href:
                    self.links.append("https://datos.madrid.es" + href)


@lru_cache
def get_links():
    parser = LinkExtractor()
    return parser.links


def process_csv(url):
    i = int(url.split("/")[-1].split("-")[1])

    temp_df = None
    try:
        with urlopen(url) as response:
            csv_content = response.read().decode('utf-8')

        # Convert text to a Spark DataFrame
        temp_df = spark.read.csv(spark.sparkContext.parallelize(csv_content.split("\r\n")), sep=";", header=True,
                                 inferSchema=True, encoding="latin1")

        # Select the desired columns and add the csv_index column
        temp_df = temp_df.selectExpr("id", "nombre as name", "longitud as longitude", "latitud as latitude").withColumn(
            "csv_index", F.lit(i))
        temp_df = temp_df.withColumn("longitude", F.regexp_replace("longitude", "\.", "")).withColumn("longitude",
           F.expr("concat(substr(longitude, 1, 2), '.', substr(longitude, 3))"))
        temp_df = temp_df.withColumn("latitude", F.regexp_replace("latitude", "\.", "")).withColumn("latitude",
            F.expr("concat(substr(latitude, 1, 2), '.', substr(latitude, 3))"))
        temp_df = temp_df.withColumn("longitude", F.col("longitude").cast("double"))
        temp_df = temp_df.withColumn("latitude", F.col("latitude").cast("double"))

    except Exception:
        pass

    return temp_df


def get_first_bacth_of_locations():
    # Initialize an empty DataFrame
    schema = "id integer, name string, longitude float, latitude float, csv_index integer"
    df = spark.createDataFrame([], schema=schema)

    # Loop through the URLs and fetch data
    urls = get_links()
    for url in urls:

        temp_df = process_csv(url)
        if temp_df:
            # Append the fetched data to the main DataFrame
            df = df.unionByName(temp_df)

    # Data cleaning
    df = (df
          .withColumn("id", F.col("id").cast("int"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("csv_index", F.col("csv_index").cast("int"))
          .dropna(how="any")
          .select("id", "name", "longitude", "latitude", "csv_index"))

    # Define a window to partition by id and order by csv_index in descending order
    window = Window.partitionBy("id").orderBy(F.col("csv_index").desc())

    # Add a row number column and filter to keep only the first row per id
    df = df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    # Write the resulting DataFrame to a Parquet file
    df.coalesce(1).write.mode("overwrite").parquet("hdfs://namenode:9000/madflow/traffic/locations")


def get_new_locations(df):
    last_link = get_links()[-1]
    new_df = process_csv(last_link)

    if df.agg(F.max("csv_index")).take(1) == new_df.select("csv_index").take(1):
        return

    new_locations_df = new_df.join(df, on="id", how="left_anti")

    # Step 4: Append new locations
    if new_locations_df.count() > 0:
        new_locations_df.coalesce(1).write.mode("append").parquet("hdfs://namenode:9000/madflow/traffic/locations")


try:
    df = spark.read.parquet("hdfs://namenode:9000/madflow/traffic/locations")
    get_new_locations(df)

except Exception:
    get_first_bacth_of_locations()

finally:
    spark.stop()