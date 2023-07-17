
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
        .appName("Olympic")\
        .getOrCreate()

    def good_block(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            int(fields[0])
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

    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    
    dataRDD = blocks.map(lambda b: (None, len(b.split(",")[4][2:])*4 + len(b.split(",")[5][2:])*4 + len(b.split(",")[6][2:])*4 + len(b.split(",")[7][2:])*4 + len(b.split(",")[8][2:])*4 + len(b.split(",")[9][2:])*4 + len(b.split(",")[13][2:])*4))
    dataRDD = dataRDD.reduceByKey(operator.add)
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'task4_part-2_' + date_time + '/spaceSaved.txt')
    my_result_object.put(Body=json.dumps(dataRDD.collect()))
    
    

    spark.stop()