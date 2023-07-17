"""Lab 3. Olympic Tweet Analysis
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import time

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("transact")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            int(fields[7])
            return True
        except:
            return False

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

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = lines.filter(good_line)
    
    
    # Total value of transactions per month
    monthVal = clean_lines.map(lambda b: (time.strftime("%b-%y", time.gmtime(int(b.split(',')[11]))), (int(b.split(',')[7]), 1)))
    monthVal = monthVal.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    
    monthCount = monthVal.map(lambda b: (b[0], b[1][1]))
    monthAve = monthVal.map(lambda b: (b[0], b[1][0]/b[1][1]))
    
    monthCount = monthCount.sortBy(lambda x: datetime.strptime(x[0], "%b-%y"), ascending=True)
    monthAve = monthAve.sortBy(lambda x: datetime.strptime(x[0], "%b-%y"), ascending=True)
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'task-1_' + date_time + '/monthCount.txt')
    my_result_object.put(Body=json.dumps(monthCount.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'task-1_' + date_time + '/monthAve.txt')
    my_result_object.put(Body=json.dumps(monthAve.collect()))
    

    spark.stop()