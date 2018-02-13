import configparser
import math
import sys

import matplotlib.pyplot as plt
import psycopg2

main_table = 'gaia_ucac4_colour'
distance_table = 'gaia_distance_uniform'
tags_distance_table = 'tgas_distance_mk_prior'


def db_connect(host, port, db_name, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)


def distance_estimate(connection):
    query = 'SELECT u.gaia_source_id, u.moment AS un_mode, u.distance AS un_distance, m.gaia_source_id, m.mode/1000.0 AS mk_mode, m.distance_50/1000.0 AS mk_distance' \
            ' FROM {0} AS u, {1} AS m ' \
            'WHERE u.gaia_source_id = m.gaia_source_id'.format(distance_table, tags_distance_table);
    cur = connection.cursor()
    cur.execute(query)
    um = list()
    ud = list()
    mm = list()
    md = list()
    for record in cur:
        um.append(record[1])
        ud.append(record[2])
        mm.append(record[4])
        md.append(record[5])
    fig = plt.figure(figsize=(12, 12))
    ax1 = fig.add_subplot(111)
    ax1.set_title('Distance Estimation')
    ax1.set_xlabel('Distance')
    ax1.set_ylabel('Mode')
    ax1.scatter(um, ud, s=0.1, c='b', marker='.')
    ax1.scatter(mm, md, s=0.1, c='g', marker='*')
    plt.savefig('distance_distribution_comparison.png')
    cur.close()


def selection(connection):
    query = 'SELECT g.source_id, c.l, c.b, c.pmra, c.pmdec, g.distance, c.b_mag, c.v_mag ' \
            'FROM (SELECT u.gaia_source_id AS source_id, m.mode AS mode, m.distance_50 AS distance ' \
            'FROM {0} AS u, {1} AS m WHERE u.gaia_source_id = m.gaia_source_id) AS g, {2} AS c ' \
            'WHERE g.source_id = c.gaia_source_id;'.format(distance_table, tags_distance_table, main_table)
    cur = connection.cursor()
    cur.execute(query)
    colour = list()
    magnit = list()
    for record in cur:
        magnit.append(5.0 + float(record[7]) - 5*math.log10(float(record[5])))
        colour.append(float(record[6]) - float(record[7]))
    fig = plt.figure(figsize=(12, 12))
    ax1 = fig.add_subplot(111)
    ax1.set_title('Colour-magnitude diagram')
    ax1.set_xlabel('B-V')
    ax1.set_ylabel('M_v')
    ax1.set_xlim(-1.0, 2.0)
    ax1.set_ylim(14, -2.0)
    ax1.scatter(colour, magnit, s=0.1, c='black', marker='.')
    plt.savefig('colour_magnitude.png')
    cur.close()


if __name__ == "__main__":
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

    conn = db_connect(HOST, PORT, DB, USER, PWORD)
    # distance_estimate(conn)
    selection(conn)
    conn.close()
