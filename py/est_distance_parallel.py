import configparser
import sys

import numpy as np
import psycopg2
# sys.path.append('py/lib/para2dis/distance')
from para2dis.distance.BayesianDistance import BayesianDistance
from para2dis.distance.Prior import Prior

main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance'


def db_connect(url_string):
    return psycopg2.connect(url_string)


def db_create_table(conn):
    create = conn.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS gaia_distance ('
                   'gaia_source_id bigint NOT NULL,'
                   'moment real,'
                   'distance real,'
                   'distance_lower real,'
                   'distance_upper real);')
    conn.commit()


if __name__ == "__main__":
    """# bash>PYTHONPATH=/Users/icshih/Documents/Research/SpiralArm/py/lib python3 est_distance_parallel.py /path/to/sa.conf"""
    if len(sys.argv) != 2:
        print('Usage: est_distance_spark.py /path/to/sa.conf')
        sys.exit(1)
    else:
        # We use a property file to configure the environment
        conf = sys.argv[1]

    config = configparser.ConfigParser()
    config.read(conf)

    HOST = config.get('database', 'host')
    USER = config.get('database', 'user')
    PWORD = config.get('database', 'password')
    PORT = config.get('database', 'port')
    DBNAME = config.get('database', 'db')
    TODB = bool(config.get('database', 'isUsed'))

    url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(USER, PWORD, HOST, PORT, DBNAME)

    distance_range = np.arange(0.01, 20.0, 0.01)
    pri = Prior()
    pri.set_r_lim(10.0)
    p = pri.proper_uniform

    conn_ = db_connect(url)
    db_create_table(conn_)
    cur = conn_.cursor()
    cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')

    insert = conn_.cursor()
    count = 0
    for record in cur:
        iden = record[0]
        parallax_ = record[1]
        parallax_error_ = record[2]
        d = BayesianDistance(iden, parallax_, parallax_error_, p, distance_range, 4)
        d.get_distance_posterior()
        d.get_result()
        insert.execute(
            'INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
            (iden, float(d.moment), float(d.distance), float(d.distance_lower), float(d.distance_upper)))
        count = count + 1
        if count >= 1000:
            conn_.commit()
            count = 0
    conn_.commit()