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
from pyspark.sql.functions import count, col
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

    # Loading rideshare_data.csv and taxi_zone_lookup.csv files
    rideshare_data = spark.read.option("header",True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("header",True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    # Performing join operation on dataframe and renaming columns and storing in a temp object
    temp = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")

    # Performing join operation on temp dataframe and renaming columns to give final dataframe with six additional columns
    final_df = temp.join(taxi_zone_lookup_df, temp.dropoff_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")

    # Converting UNIX Timestamp to "yyyy-MM-dd" format in date column
    final_df = final_df.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM-dd"))
    # Printing first 10 rows of merged datframe
    final_df.show(10)
    # Printing number of rows
    print("Number of Rows:", final_df.count())
    # Printing Schema
    print("Schema:")
    final_df.printSchema()    
    
    spark.stop()