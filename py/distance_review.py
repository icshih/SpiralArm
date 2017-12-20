import astropy.units as u
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
import psycopg2
import astropy.coordinates as coord


def db_connect():
    URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
    conn = psycopg2.connect(URI)
    return conn


def distance_estimate(conn):
    cur = conn.cursor()
    cur.execute('SELECT moment, distance FROM gaia_distance')
    m = list()
    d = list()
    for record in cur:
        m.append(record[0])
        d.append(record[1])
    fig = plt.figure(figsize=(12, 12))
    ax1 = fig.add_subplot(111)
    ax1.set_title('Distance Estimation')
    ax1.set_xlabel('Distance')
    ax1.set_ylabel('Moment')
    ax1.scatter(m, d, s=0.1)
    plt.show()



conn_ = db_connect()
distance_estimate(conn_)