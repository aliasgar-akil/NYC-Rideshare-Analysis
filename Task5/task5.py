import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, month
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
    # Filtering only January days records using month function of pyspark.sql.functions API
    rideshare_data = rideshare_data.where(month(from_unixtime('date'))==1)
    # Converting Unix timestamp to 'dd' format and casting to 'Integer' type
    rideshare_data = rideshare_data.withColumn("date", date_format(from_unixtime('date'), 'dd')).withColumn("date", col("date").cast("Integer"))
    # Grouping DataFrame by date and applying avg aggregate function on request_to_pickup and sorting by days
    rideshare_data = rideshare_data.groupby('date').agg(avg("request_to_pickup").alias("average_request_to_pickup")).sort(col('date'))
    rideshare_data.show()
    rideshare_data.coalesce(1).write.option("header", "true").csv('s3a://' + s3_bucket + '/coursework/average_request_to_pickup.csv')
    
    spark.stop()