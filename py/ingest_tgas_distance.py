import configparser
import gzip
import sys

import psycopg2

distance_table = 'tgas_distance_mk_prior'


def db_connect(host, port, db_name, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)


def db_create_table(connection):
    create = connection.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS {0} ('
                   'gaia_source_id bigint NOT NULL,'
                   'mode real,'
                   'distance_5 real,'
                   'distance_50 real,'
                   'distance_95 real, '
                   'distance_error real);'.format(distance_table))
    connection.commit()
    create.close()


def ingest_data_file(compressed_csv, connection):
    insert = connection.cursor()
    with gzip.open(compressed_csv, 'rt') as t:
        header = 0
        count = 0
        for line in t:
            if header > 0:
                data = line.split(',')
                print(data[3], data[18], data[19], data[20], data[21], data[22])
                insert.execute(
                    'INSERT INTO {0} (gaia_source_id, mode, distance_5, distance_50, distance_95, distance_error)'
                    'VALUES (%s, %s, %s, %s, %s, %s);'.format(distance_table), (int(data[2]), float(data[18]),
                    float(data[19]), float(data[20]), float(data[21]), float(data[22])))
                count = count + 1
                if count >= 1000:
                    print('inserting data to database...')
                    connection.commit()
                    count = 0
            header = header + 1
    connection.commit()
    insert.close()


if __name__ == '__main__':
    """"""
    if len(sys.argv) != 3:
        print('Usage: ingest_tgas_distance.py /path/to/db.conf')
        sys.exit(1)
    else:
        conf = sys.argv[1]
        file = sys.argv[2]

    config = configparser.ConfigParser()
    config.read(conf)

    HOST = config.get('database', 'host')
    PORT = config.get('database', 'port')
    DB = config.get('database', 'dbname')
    USER = config.get('database', 'user')
    PWORD = config.get('database', 'password')

    conn_ = db_connect(HOST, PORT, DB, USER, PWORD)
    db_create_table(conn_)
    ingest_data_file(file, conn_)
    conn_.close()
