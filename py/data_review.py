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


def sky_distribution(conn):
    cur = conn.cursor()
    cur.execute('SELECT l, b FROM gaia_ucac4_colour WHERE b_mag != \'nan\' AND v_mag != \'nan\'')
    l = list()
    b = list()
    for record in cur:
        l.append(record[0])
        b.append(record[1])
    l = np.array(l)
    b = np.array(b)
    gal = coord.SkyCoord(l=l, b=b, unit=(u.deg, u.deg), frame='galactic')
    fig = plt.figure(figsize=(16, 9))
    ax1 = fig.add_subplot(111, projection='aitoff')
    ax1.set_title('Object Distribution ({:,})'.format(l.size))
    ax1.set_xlabel('Galactic Lon.')
    ax1.set_ylabel('Galactic Lat.')
    ax1.scatter(gal.l.wrap_at(180 * u.deg).radian, gal.b.radian, s=0.1)
    ax1.grid(True)
    plt.show()


def data_distribution(conn):
    cur = conn.cursor()
    p = list()
    g = list()
    b = list()
    v = list()
    cur.execute(
        'SELECT parallax, phot_g_mean_mag, b_mag, v_mag FROM gaia_ucac4_colour WHERE b_mag != \'nan\' AND v_mag != \'nan\'')
    for record in cur:
        p.append(record[0])
        g.append(record[1])
        b.append(record[2])
        v.append(record[3])
    data = np.array([p, g, b, v])
    fig = plt.figure(figsize=(12, 12))

    ax1 = fig.add_subplot(221)
    ax1.set_title('parallax (Gaia DR1)')
    ax1.set_xlabel('mas/y')
    ax1.set_xlim(-15, 30)
    ax1.hist(data[0], bins=20)

    ax2 = fig.add_subplot(222)
    ax2.set_title('G mean mag (Gaia DR1)')
    ax2.set_xlabel('mag')
    ax2.set_xlim(5, 15)
    ax2.hist(data[1])

    ax3 = fig.add_subplot(223)
    ax3.set_title('B mag (2MASS)')
    ax3.set_xlabel('mag')
    ax3.set_xlim(5, 15)
    ax3.hist(data[2])

    ax4 = fig.add_subplot(224)
    ax4.set_title('V mag (2MASS)')
    ax4.set_xlabel('mag')
    ax4.set_xlim(5, 15)
    ax4.hist(data[3])
    plt.show()


def data_correlation(data):
    fig = plt.figure(figsize=(16, 16))
    ax1 = fig.add_subplot(121)
    ax1.set_xlabel('parallax (mas/y)')
    ax1.set_ylabel('G mean (mag)')
    ax1.scatter(data[0], data[1], s=0.1)
    ax2 = fig.add_subplot(122)
    ax2.set_xlabel('Colour (B-V)')
    ax2.set_ylabel('G mean (mag)')
    ax2.set_xlim(8, -6)
    ax2.set_ylim(20, 0)
    ax2.scatter((data[2] - data[3]), data[1], s=0.1)
    plt.show()


def parallax_distribution(conn):
    cur = conn.cursor()
    p = list()
    g = list()
    pe = list()
    cur.execute(
        'SELECT parallax, parallax_error, phot_g_mean_mag FROM gaia_ucac4_colour WHERE parallax > 0 AND b_mag != \'nan\' AND v_mag != \'nan\'')
    for record in cur:
        p.append(record[0])
        pe.append(record[1])
        g.append(record[2])
    data = np.array([p, pe, g])
    fig = plt.figure(figsize=(12, 12))
    ax1 = fig.add_subplot(111)
    ax1.set_xlabel('parallax (mas/y)')
    ax1.set_ylabel('fractional parallax error')

    colr = cm.afmhot
    norm = cm.colors.Normalize(vmin=np.min(data[2]), vmax=np.max(data[2]))
    dist = ax1.scatter(data[0], data[1] / data[0], c=data[2], cmap=colr, norm=norm, s=0.3)
    cb = fig.colorbar(dist, orientation='vertical')
    cb.set_label('Phot G mean mag')
    plt.show()


conn_ = db_connect()

# sky_distribution(conn_)
# data_distribution(conn_)
parallax_distribution(conn_)
