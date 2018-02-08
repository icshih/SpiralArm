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


def worker_a(record):
    bd = BayesianDistance(record[0], record[1], record[2], p, distance_range, 1)
    print('Process {0} handles {1}'.format(os.getpid(), record[0]))
    q.put(bd.calculate())


def worker_b(queue, connection):
    insert = connection.cursor()
    count = 0
    not_complete = True
    while not_complete:
        try:
            j = queue.get(timeout=30)
            print('Process {0} inserts {1}'.format(os.getpid(), j[0]))
            insert.execute(
                'INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
                (j[0], float(j[1]), float(j[2]), float(j[3]), float(j[4])))
            count = count + 1
            if count >= 1000:
                print('inserting data to database...')
                connection.commit()
                count = 0
        except:
            print('Does the queue remain empty after 30 seconds? {}'.format(queue.empty()))
            not_complete = False
    connection.commit()


if __name__ == "__main__":
    """# bash>PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_parallel.py /path/to/local.conf"""
    if len(sys.argv) != 2:
        print('Usage: est_distance_parallel_queue.py /path/to/db.conf')
        sys.exit(1)
    else:
        conf = sys.argv[1]

    config = configparser.ConfigParser()
    config.read(conf)

    HOST = config.get('database', 'host')
    PORT = config.get('database', 'port')
    DB = config.get('database', 'dbname')
    USER = config.get('database', 'user')
    PWORD = config.get('database', 'password')

    distance_range = np.arange(0.01, 15.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    conn_ = db_connect(HOST, PORT, DB, USER, PWORD)
    db_create_table(conn_)
    cur = conn_.cursor()
    cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')

    q = multiprocessing.Queue(os.cpu_count() + 10)
    r = multiprocessing.Process(target=worker_b, args=(q, conn_))
    r.start()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        pool.map(worker_a, cur.fetchall())
    r.join()
    conn_.close()
