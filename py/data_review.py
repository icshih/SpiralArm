import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import astropy.coordinates as coord


def db_connect():
    URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
    conn = psycopg2.connect(URI)
    return conn


def sky_distribution(conn):
    cur = conn.cursor()
    cur.execute('SELECT l, b FROM gaia_ucac4_colour')
    l = list()
    b = list()
    for record in cur:
        l.append(record[0]-180.0)
        b.append(record[1])
    l = np.array(l)
    b = np.array(b)
    gal = coord.SkyCoord(l=l, b=b, unit=(u.deg, u.deg), frame='galactic')
    fig = plt.figure(figsize=(16,9))
    ax1 = fig.add_subplot(111, projection='aitoff')
    ax1.scatter(gal.l.wrap_at(180*u.deg).radian, gal.b.radian, s=1)
    ax1.grid(True)
    plt.show()


def data_distribution(conn):
    None

sky_distribution(db_connect())










# gala = coord.Galactic(l*u.degree, b*u.degree)
#
# fig = plt.figure(figsize=(8,6))
#
# ax = fig.add_subplot(111, projection="mollweide")
#
# ll = coord.Angle(gala.l)
# bb = coord.Angle(gala.b)
# ax.scatter(ll.radian, bb.radian)

