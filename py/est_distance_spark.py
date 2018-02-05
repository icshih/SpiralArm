import configparser
import os
import re
import sys

import boto3
import numpy as np
# sys.path.append('/Users/icshih/Documents/Research/SpiralArm/py/lib')
from para2dis.distance.BayesianDistance import BayesianDistance
from para2dis.distance.Prior import Prior
from pyspark.sql import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """Submit Spark job:
    1. Using spark-submit:
       PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib spark-submit --master local[4] \
       --conf spark.driver.extraClassPath=resources/postgresql-42.1.4.jar \
       --conf spark.network.timeout=10000 \
       py/est_distance_spark.py conf/sa.conf
    2. Self-contained:
    #  PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_spark.py /path/to/sa.conf
    """
    if len(sys.argv) != 2:
        print('Usage: est_distance_spark.py /path/to/sa.conf')
        sys.exit(1)
    else:
        # We use a property file to configure the environment
        conf = sys.argv[1]

    os.environ['PYSPARK_PYTHON'] = 'python3'
    main_table = 'gaia_ucac4_colour'
    distance_table = 'gaia_distance_spark'

    spark = SparkSession.Builder() \
        .appName('distance') \
        .getOrCreate()
        # .master("local[4]") \
        # .config('spark.driver.extraClassPath', 'resources/postgresql-42.1.4.jar') \
        # .config('spark.network.timeout', '10000') \

    config = configparser.ConfigParser()
    config.read(conf)

    DRIVER = config.get('database', 'driver')
    URL = config.get('database', 'url')
    USER = config.get('database', 'user')
    TODB = config.get('database', 'isUsed')

    prop = {
        'driver': DRIVER,
        'user': USER
    }

    df = spark.read.jdbc(URL, main_table, properties=prop)

    df.createOrReplaceTempView('gaia_ucac4_colour')

    distance_range = np.arange(0.01, 20.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    ds = spark.sql('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0')

    out = ds.rdd \
        .map(lambda d: BayesianDistance(d.gaia_source_id, d.parallax, d.parallax_error, p, distance_range, 4)) \
        .map(lambda b: b.calculate()) \
        .map(lambda b: Row(gaia_source_id=b[0], moment=float(b[1]), distance=float(b[2]), distance_lower=float(b[3]),
                           distance_upper=float(b[4])))

    # Write to database or parquet? Depending on the host
    if re.search('compute.amazonaws.com', os.environ['HOST']) and TODB is False:
        S3 = config.get('data', 'output.s3bucket')
        OUTPUT = config.get('data', 'output.parquet')
        # spark.createDataFrame(out).write.mode('overwrite').parquet('s3://' + S3 + '/data/' + OUTPUT, compression='snappy')
        spark.createDataFrame(out).write.mode('overwrite').parquet(OUTPUT, compression='snappy')
        s3 = boto3.resource('s3')
        with open(OUTPUT, 'rb') as p:
            s3.Bucket(S3).put_object(Key='data/' + OUTPUT, Body=p)
    else:
        spark.createDataFrame(out).repartition(10).write.mode('append').jdbc(URL, distance_table, properties=prop)

    spark.stop()
