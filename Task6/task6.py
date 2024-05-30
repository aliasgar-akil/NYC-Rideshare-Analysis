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
from pyspark.sql.functions import to_date, count, col
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

    # 1. Calculating Trip counts greater than 0 and less than 1000 for different Pickup Borough at different time of day
    # Performing join operation 
    df = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    # Grouping DataFrame by Pickup_Borough and time_of_day, performing aggregated count, and filtering the records where trip_count>0 and trip_count<1000 using where() function
    df = df.groupby("Pickup_Borough", "time_of_day").count().withColumnRenamed("count", "trip_count")\
    .where((col("trip_count")>0) & (col("trip_count")<1000))
    df.show()
    df.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/trip_count_less_than_1000.csv')

    # 2. Calculating Trip counts for each Pickup Borough in the evening time
    # Performing join operation
    df2 = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    # Grouping DataFrame by Pickup_Borough and time_of_day, performing aggregated count, and filtering the records where time_of_day is 'evening'
    df2 = df2.groupby("Pickup_Borough", "time_of_day").count().withColumnRenamed("count", "trip_count")\
    .where(col("time_of_day") == 'evening')
    df2.show()
    df2.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/trip_count_evening.csv')

    # 3. Calculating the number of trips that started in Brooklyn and ended in Staten Island
    # Performing join operations
    df3 = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    df3 = df3.join(taxi_zone_lookup_df, df3.dropoff_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")
    # Selecting Pickup_Borough, Dropoff_Borough, Pickup_Zone columns and filtering the records where Pickup_Borough is 'Brooklyn' and Dropoff_Borough is 'Staten Island'
    df3 = df3.select(col("Pickup_Borough"), col("Dropoff_Borough"), col("Pickup_Zone"))\
    .where((col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island"))
    df3.show(10, truncate=False)

    # # Printing the count of trips starting in Brooklyn and ending in Staten Island
    print("Number of trips that started in Brooklyn and ended in Staten Island:", df3.count())

    spark.stop()