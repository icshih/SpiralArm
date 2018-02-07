import boto3
import botocore

BUCKET_NAME = 'aws-logs-823959199013-eu-west-3'
KEY = 'rds.conf'
LOCAL_FILE = 'temp.conf'

s3 = boto3.resource('s3')

try:
    s3.Bucket(BUCKET_NAME).download_file(KEY, LOCAL_FILE)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

with open(LOCAL_FILE) as local:
    for l in local:
        print(l)
