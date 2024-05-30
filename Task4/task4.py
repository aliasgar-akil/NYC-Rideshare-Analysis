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
from pyspark.sql.functions import to_date, count, col, avg
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

    # Loading dataset
    rideshare_data = spark.read.option("header",True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")

    # 1. Average driver total pay at different time of day
    # grouping dataframe by time of day and applying avg aggregate function and sorting in descending order
    avg_driver_total_pay = rideshare_data.groupby("time_of_day").agg(avg("driver_total_pay").alias("average_driver_total_pay"))\
    .sort(col("average_driver_total_pay").desc())
    avg_driver_total_pay.show()
    avg_driver_total_pay.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/average_driver_total_pay.csv')

    # 2. Average trip length at different time of day
    # grouping dataframe by time of day and applying avg aggregate function and sorting in descending order
    avg_trip_length = rideshare_data.groupby("time_of_day").agg(avg("trip_length").alias("average_trip_length"))\
    .sort(col("average_trip_length").desc())
    avg_trip_length.show()
    avg_trip_length.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/average_trip_length.csv')

    # 3. Average earning per mile at different time of day
    # joining avg_driver_total_pay dataframe with avg_trip_length on time of day column
    avg_earning_per_mile = avg_driver_total_pay.join(avg_trip_length, avg_driver_total_pay.time_of_day == avg_trip_length.time_of_day)
    # Adding a new column average_earning_per_mile obtained from dividing average_driver_total_pay by average_trip_length and dropping those columns followed
    # by dropping duplicate time of day column due to join operation
    avg_earning_per_mile = avg_earning_per_mile.withColumn("average_earning_per_mile", col("average_driver_total_pay") / col("average_trip_length"))\
    .drop("average_driver_total_pay", "average_trip_length").drop(avg_trip_length["time_of_day"])
    avg_earning_per_mile.show()
    avg_earning_per_mile.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/average_earning_per_mile.csv')
    
    spark.stop()