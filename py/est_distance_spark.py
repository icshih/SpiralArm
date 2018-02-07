import configparser
import sys

import numpy as np
from para2dis.BayesianDistance import BayesianDistance
from para2dis.Prior import Prior
from pyspark.sql import Row
from pyspark.sql import SparkSession

main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance_spark'

if __name__ == "__main__":
    """Submit Spark job:
    1. Using spark-submit:
       spark-submit --master local[*] \
       --jars resources/postgresql-42.1.4.jar \
       --py-files py/lib/para2dis/dist/para2dis-0.1.tar.gz \
       py/est_distance_spark.py conf/local.conf
    """
    if len(sys.argv) != 2:
        print('Usage: est_distance_spark.py /path/to/local.conf')
        sys.exit(1)
    else:
        # We use a property file to configure the environment
        conf = sys.argv[1]

    spark = SparkSession.Builder().appName('distance').getOrCreate()

    config = configparser.ConfigParser()
    config.read(conf)

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
        OUTPUT = config.get('data', 'output.parquet')
        spark.createDataFrame(out).write.mode('overwrite').parquet(OUTPUT, compression='snappy')

    spark.stop()
