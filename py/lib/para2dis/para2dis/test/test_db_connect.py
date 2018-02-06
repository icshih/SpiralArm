import configparser
import sys

import psycopg2


def db_connect(host, port, db_name, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: test_db_connect.py /path/to/test.conf')
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
    OUTPUT = config.get('data', 'output.votable')

    conn = db_connect(HOST, PORT, DB, USER, PWORD)
    conn.close()