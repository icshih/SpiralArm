import configparser
import sys

import boto3
import botocore
import numpy as np
from para2dis.BayesianDistance import BayesianDistance
from para2dis.Prior import Prior
from pyspark.sql import Row
from pyspark.sql import SparkSession

local_file = 'temp.conf'
main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance_spark'

def get_file_on_s3(s3_key_tag, local_file):
    """s3_key_tag format: bucket_name:key"""
    tag = s3_key_tag.split(':')

    open(local_file, 'w+')

    s3 = boto3.resource('s3')

    try:
        s3.Bucket(tag[0]).download_file(tag[1], local_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


if __name__ == "__main__":
    """Submit Spark application in Step:
       --jars s3_bucket/program/postgresql-42.1.4.jar \
       --py-files s3_bucket/program/para2dis-0.1.egg \
       py/est_distance_spark.py s3_bucket:program/local.conf
   """
    if len(sys.argv) != 2:
        sys.exit(1)
    else:
        # We use a property file to configure the environment
        conf_on_s3 = sys.argv[1]

    spark = SparkSession.Builder().appName('distance').getOrCreate()

    get_file_on_s3(conf_on_s3, local_file)

    config = configparser.ConfigParser()
    config.read(local_file)

    DRIVER = config.get('database', 'driver')
    URL = config.get('database', 'url')
    USER = config.get('database', 'user')
    PASSWORD = config.get('database', 'password')
    TODB = config.get('database', 'isUsed')

    prop = {
        'driver': DRIVER,
        'user': USER,
        'password': PASSWORD
    }

    df = spark.read.jdbc(URL, main_table, properties=prop)

    df.createOrReplaceTempView(main_table)

    distance_range = np.arange(0.01, 20.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    ds = spark.sql('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0 LIMIT 10000')

    out = ds.rdd \
        .map(lambda d: BayesianDistance(d.gaia_source_id, d.parallax, d.parallax_error, p, distance_range, 4)) \
        .map(lambda b: b.calculate()) \
        .map(lambda b: Row(gaia_source_id=b[0], moment=float(b[1]), distance=float(b[2]), distance_lower=float(b[3]),
                           distance_upper=float(b[4])))

    # Write to database or parquet? Depending on the host
    if TODB.upper() is 'TRUE':
        spark.createDataFrame(out).repartition(10).write.mode('append').jdbc(URL, distance_table, properties=prop)
    else:
        S3_BUCKET = config.get('data', 'output.s3bucket')
        OUTPUT = config.get('data', 'output.parquet')
        spark.createDataFrame(out).write.mode('overwrite').parquet('s3://' + S3_BUCKET + '/data/' + OUTPUT,
                                                                   compression='snappy')

    spark.stop()
