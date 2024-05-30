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
from pyspark.sql.functions import count, col, sum, rank, concat, lit
from pyspark.sql.window import Window  # API used to perform operations on row partitions
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

    # Performing join operation on dataframe and renaming columns and storing in a temp object
    temp = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")

    # Performing join operation on temp dataframe and renaming columns to give final dataframe with six additional columns
    merged_df = temp.join(taxi_zone_lookup_df, temp.dropoff_location == taxi_zone_lookup_df.LocationID).withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")
    
    # Converting UNIX Timestamp to "MM" format
    merged_df = merged_df.withColumn("date", date_format(from_unixtime("date"), "MM")).withColumnRenamed("date", "Month").withColumn("Month", col("Month").cast("Integer"))

    #  1. Identifying the top 5 popular pickup boroughs each month
    # obtaining a grouped dataset grouped by Month and Pickup_Borough and renaming count column
    grouped_df = merged_df.groupby("Month", "Pickup_Borough").count().withColumnRenamed("count", "trip_count")

    # creating partitions on Month using Window function and sorting by trip_count in descending order
    window_partition = Window.partitionBy("Month").orderBy(col("trip_count").desc())
    # ranking rows within a window partition
    ranked_df = grouped_df.withColumn("rank", rank().over(window_partition))
    
    # Selecting the top 5 pickup boroughs for each month
    final_df1 = ranked_df.filter(col("rank") <= 5).orderBy("Month", "rank").drop("rank").select(col("Pickup_Borough"), col("Month"), col("trip_count"))
    
    final_df1.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/pickup_borough_count.csv')

    #  2. Identifying the top 5 popular dropoff boroughs each month
    # obtaining a grouped dataset grouped by Month and Dropoff_Borough and renaming count column
    grouped_df = merged_df.groupby("Month", "Dropoff_Borough").count().withColumnRenamed("count", "trip_count")

    # ranking rows within a window partition
    ranked_df = grouped_df.withColumn("rank", rank().over(window_partition))
    
    # Selecting the top 5 dropoff boroughs for each month
    final_df2 = ranked_df.filter(col("rank") <= 5).orderBy("Month", "rank").drop("rank").select(col("Dropoff_Borough"), col("Month"), col("trip_count"))
    
    final_df2.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/dropoff_borough_count.csv')

    #  3. Identifying top 30 earnest routes
    # Adding a new column Route showing '<Pickup_Borough> to <Dropoff_Borough>' using concat function 
    concat_df = merged_df.withColumn("Route", concat(col("Pickup_Borough"), lit(" to "), col("Dropoff_Borough")))
    # Grouping DataFrame by Route and performing aggregated sum on driver_total_pay, renaming to total_profit and sorting by total_profit in descending order, retrieving top 30 records using limit
    grouped_df = concat_df.groupby("Route").agg(sum("driver_total_pay").alias("total_profit")).sort(col("total_profit").desc()).limit(30)

    final_df1.show(25, truncate=False)
    final_df2.show(25, truncate=False)
    grouped_df.show(30, truncate=False)

    grouped_df.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/earnest_route.csv')    
    
    spark.stop()