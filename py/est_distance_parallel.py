import configparser
import multiprocessing
import os
import sys

import numpy as np
import psycopg2
from para2dis.BayesianDistance import BayesianDistance
from para2dis.Prior import Prior

main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance'


def db_connect(host, port, db_name, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)


def db_create_table(conn):
    create = conn.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS gaia_distance ('
                   'gaia_source_id bigint NOT NULL,'
                   'moment real,'
                   'distance real,'
                   'distance_lower real,'
                   'distance_upper real);')
    conn.commit()

def worker(id, parallax, parallax_error, p, distance_range):
    print('{0} processes {1}'.format(os.getpid(), id))
    bd = BayesianDistance(id, parallax, parallax_error, p, distance_range, 1)
    bd.get_distance_posterior()
    return bd.get_result()

def worker2(record):
    bd = BayesianDistance(record[0], record[1], record[2], p, distance_range, 1)
    print('{0} processes {1}'.format(os.getpid(), record[0]))
    return bd.calculate()

if __name__ == "__main__":
    """# bash>PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_parallel.py /path/to/local.conf"""
    if len(sys.argv) != 2:
        print('Usage: est_distance_spark.py /path/to/local.conf')
        sys.exit(1)
    else:
        # We use a property file to configure the environment
        conf = sys.argv[1]

    config = configparser.ConfigParser()
    config.read(conf)

    HOST = config.get('database', 'host')
    PORT = config.get('database', 'port')
    DB = config.get('database', 'dbname')
    USER = config.get('database', 'user')
    PWORD = config.get('database', 'password')

    distance_range = np.arange(0.01, 10.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    conn_ = db_connect(HOST, PORT, DB, USER, PWORD)
    db_create_table(conn_)
    cur = conn_.cursor()
    cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')

    with multiprocessing.Pool(os.cpu_count()) as pool:
        jobs = pool.map(worker2, cur.fetchall())

    insert = conn_.cursor()
    count = 0
    for j in jobs:
        insert.execute(
        'INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
        (j[0], float(j[1]), float(j[2]), float(j[3]), float(j[4])))
    count = count + 1
    if count >= 10000:
        print('insert data...')
        conn_.commit()
        count = 0
    conn_.commit()
    conn_.close()