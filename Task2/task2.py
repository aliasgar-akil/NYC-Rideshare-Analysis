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
from pyspark.sql.functions import count, col, sum
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
    # Converting UNIX Timestamp to "yyyy-MM" format
    rideshare_data = rideshare_data.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM"))

    # 1. Getting aggregated DataFrame of trips count of each business in each month and writing to a csv file in personal bucket
    # grouping dataframe by business and date
    business_count = rideshare_data.groupby("business","date").count()
    # writing aggregated dataframe to csv file
    business_count.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/business_counts.csv')

    # 2. Getting aggregated DataFrame of Platform Profit of each business in each month and writing to a csv file in personal bucket
    # converting rideshare_profit column datatype from string to float
    rideshare_data = rideshare_data.withColumn("rideshare_profit",col("rideshare_profit").cast("float"))
    # Grouping DataFrame on business and date and performing aggregated sum on rideshare_profit
    rideshare_platform_profit = rideshare_data.groupby("business","date").agg(sum("rideshare_profit").alias("platform_profit"))
    rideshare_platform_profit.show()
    # writing aggregated dataframe to csv file
    rideshare_platform_profit.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/rideshare_profit.csv')

    # 3. Getting aggregated DataFrame of Driver Total Pay of each business in each month and writing to a csv file in personal bucket
    # converting driver_total_pay column datatype from string to float
    rideshare_data = rideshare_data.withColumn("driver_total_pay",col("driver_total_pay").cast("float"))
    # Grouping DataFrame on business and date and performing aggregated sum on driver_total_pay
    rideshare_driver_pay = rideshare_data.groupby("business","date").agg(sum("driver_total_pay").alias("driver_total_pay"))
    rideshare_driver_pay.show()
    # writing aggregated dataframe to csv file
    rideshare_driver_pay.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/driver_pay.csv')
    
    spark.stop()