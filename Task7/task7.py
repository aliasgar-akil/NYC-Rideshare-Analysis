import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, concat, lit
from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Loading datasets
    rideshare_data = spark.read.option("header",True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("header",True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    # Joining datasets
    df = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    df = df.join(taxi_zone_lookup_df, df.dropoff_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")

    # Adding a new column Route by concatenating Pickup_Zone and Dropoff_Zone and grouping the dataset by Route to get aggregated count
    total_count_df = df.withColumn("Route", concat(col("Pickup_Zone"), lit("to"), col("Dropoff_Zone"))).groupby("Route").count().withColumnRenamed("count", "total_count")
    # Using only Uber records to get aggregated count of each Route
    uber_df = df.where(col("business") == "Uber").withColumn("Route", concat(col("Pickup_Zone"), lit("to"),col("Dropoff_Zone")))\
    .groupby("Route").count().withColumnRenamed("count", "uber_count")

    # Subtracting uber_count from total_count to get lyft_count column and sorting in descending order of total_count
    merged_df = total_count_df.join(uber_df, on='Route').withColumn("lyft_count", col("total_count") - col("uber_count")).sort(col("total_count").desc())\
    .select(col("Route"), col("uber_count"), col("lyft_count"), col("total_count"))

    merged_df.show(10, truncate=False)
    
    spark.stop()