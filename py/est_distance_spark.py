import os

import numpy as np
import psycopg2
# import sys
# sys.path.append('/Users/icshih/Documents/Research/SpiralArm/py/lib')
from para2dis.distance.BayesianDistance import BayesianDistance
from para2dis.distance.Prior import Prior
from pyspark.sql import Row
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = 'python3'
spark = SparkSession.Builder() \
    .appName('test') \
    .master('local[2]') \
    .config('spark.driver.extraClassPath', '../resources/postgresql-42.1.4.jar') \
    .getOrCreate()

main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance'


def db_connect():
    URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
    conn = psycopg2.connect(URI)
    return conn


def db_create_table(conn):
    create = conn.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS gaia_distance ('
                   'gaia_source_id bigint NOT NULL,'
                   'moment real,'
                   'distance real,'
                   'distance_lower real,'
                   'distance_upper real);')
    conn.commit()


def saveToDb(partition):
    pdf = spark.createDataFrame(partition)
    pdf.write.mode('append').jdbc(jdbcUrl, distance_table, properties={
        'driver': 'org.postgresql.Driver',
        'user': 'postgres'})


if __name__ == "__main__":
    # > PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_spark.py
    jdbcUrl = 'jdbc:postgresql://0.0.0.0:10000/postgres'

    distance_range = np.arange(0.01, 20.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    df = spark.read.jdbc(url=jdbcUrl, table=main_table, properties={
        'driver': 'org.postgresql.Driver',
        'user': 'postgres'})

    df.createOrReplaceTempView('gaia_ucac4_colour')
    ds = spark.sql('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0')

    out = ds.rdd \
        .map(lambda d: BayesianDistance(d.gaia_source_id, d.parallax, d.parallax_error, p, distance_range, 4)) \
        .map(lambda b: b.calculate()) \
        .map(lambda b: Row(gaia_source_id=b[0], moment=float(b[1]), distance=float(b[2]), distance_lower=float(b[3]),
                           distance_upper=float(b[4]))) \
        .repartition(10) \
        .foreachPartition(saveToDb)

    ## Write to database or parquet?
    # outDF = spark.createDataFrame(out)
    # outDF.repartition(10).write.mode('append').jdbc(jdbcUrl, distance_table, properties={
    #     'driver': 'org.postgresql.Driver',
    #     'user': 'postgres'})

    spark.stop()
