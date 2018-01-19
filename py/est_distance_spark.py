import os

import numpy as np
import psycopg2
# import sys
# sys.path.append('/Users/icshih/Documents/Research/SpiralArm/py/lib')
from para2dis.distance.BayesianDistance import BayesianDistance
from pyspark.sql import Row
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = 'python3'
spark = SparkSession.Builder() \
    .appName('test') \
    .master('local[2]') \
    .config('spark.driver.extraClassPath', '../resources/postgresql-42.1.4.jar') \
    .getOrCreate()

jdbcUrl = 'jdbc:postgresql://0.0.0.0:10000/postgres'
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


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return idx


def cal_bd(data):
    '''daemonic processes are not allowed to have children'''
    b = BayesianDistance(data.gaia_source_id, data.parallax, data.parallax_error)
    b.get_distance_posterior()
    b.get_result()
    print(b.source_id, b.moment, b.mean, b.lower, b.upper)


if __name__ == "__main__":
    # > PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_spark.py
    distance_range = np.arange(0.01, 20.0, 0.01)

    df = spark.read.jdbc(url=jdbcUrl, table=main_table, properties={
        'driver': 'org.postgresql.Driver',
        'user': 'postgres'})

    df.createOrReplaceTempView('gaia_ucac4_colour')
    ds = spark.sql('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0 LIMIT 1')

    out = ds.rdd \
        .map(lambda d: BayesianDistance(d.gaia_source_id, d.parallax, d.parallax_error, distance_range, 4)) \
        .map(lambda b: b.calculate()) \
        .map(lambda b: Row(gaia_source_id=b[0], moment=float(b[1]), distance=float(b[2]), distance_lower=float(b[3]), distance_upper=float(b[4])))


    ## Write to database or parquet?
    outDF = spark.createDataFrame(out)
    outDF.printSchema()
    outDF.write.mode(saveMode='Overwrite').jdbc(jdbcUrl, distance_table, properties={
        'driver': 'org.postgresql.Driver',
        'user': 'postgres'})

    spark.stop()



    # for record in cur:
    #     parallax_ = record[1]
    #     parallax_error_ = record[2]
    #     d = Distance(parallax_, parallax_error_)
    #     d.get_distance_prob(distance_range)
    #     d.get_result()
    #     print(record[0], parallax_, parallax_error_, d.moment, d.mean, d.lower, d.upper)
    #     insert.execute('INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
    #                (record[0], float(moment), float(dist), float(lower), float(upper)))
    #     count = count + 1
    #     if (count >= 500):
    #         conn_.commit()
    #         count = 0
    # conn_.commit()
