import re

import numpy as np
import numpy.ma as ma
import psycopg2
from astroquery.gaia import Gaia
from astroquery.vizier import Vizier


def db_connect():
    URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
    conn = psycopg2.connect(URI)
    return conn


def db_create_table(conn):
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS gaia_ucac4_colour ('
                'gaia_source_id bigint NOT NULL, '
                'l double precision,'
                'b double precision,'
                'ra double precision,'
                'dec double precision,'
                'pmra double precision,'
                'pmdec double precision,'
                'ucac4_id text NOT NULL,'
                'b_mag float,'
                'v_mag float);')
    conn.commit()


# alternative query
query = 'SELECT g.source_id, g.l, g.b, g.ra, g.dec, g.pmra, g.pmdec, u.ucac4_id ' \
        'FROM (' \
        'SELECT source_id, original_ext_source_id AS ucac4_id ' \
        'FROM gaiadr1.ucac4_best_neighbour ' \
        'WHERE source_id IN (' \
        'SELECT source_id ' \
        'FROM gaiadr1.gaia_source ' \
        'WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222)' \
        ') AND number_of_mates = 0 ' \
        ') AS u ' \
        'JOIN gaiadr1.gaia_source AS g ON (g.source_id = u.source_id)' \
        'WHERE g.pmra IS NOT null AND g.pmdec IS NOT null'

# from astropy.io.votable import parse
# votable = parse('/Users/icshih/Documents/Research/SpiralArm/data/saved/gaia_source_xm_ucac4.vot')
# data = votable.get_first_table()


job = Gaia.launch_job_async(query, output_file='/Users/icshih/Documents/Research/SpiralArm/data/saved/'
                                               'gaia_source_xm_ucac4.vot', dump_to_file=True)
data = job.get_results()

conn_ = db_connect()
insert = conn_.cursor()
count = 0
constraint = '<<'
id_dict = dict()
for d in data:
    f = ma.filled(d)
    u_id = re.sub('UCAC4-', '', f['ucac4_id'].item(0).decode(encoding='UTF-8'))
    g_id = f['source_id'].item(0)
    l = f['l'].item(0)
    b = f['b'].item(0)
    ra = f['ra'].item(0)
    dec = f['dec'].item(0)
    pmra = f['pmra'].item(0)
    pmdec = f['pmdec'].item(0)
    id_dict[u_id] = g_id, l, b, ra, dec, pmra, pmdec
    constraint = constraint + ';' + u_id
    count = count + 1
    if (count >= 500):
        result = Vizier.query_constraints(catalog='I/322A/out', UCAC4=constraint)
        data = result[0]
        for i in np.arange(len(data)):
            ucac4 = data.field('UCAC4')[i]
            bmag = float(data.field('Bmag')[i])
            vmag = float(data.field('Vmag')[i])
            g_tuple = id_dict[ucac4]
            insert.execute(
                'INSERT INTO gaia_ucac4_colour (gaia_source_id, l, b, ra, dec, pmra, pmdec, ucac4_id, b_mag, v_mag) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                (g_tuple[0], g_tuple[1], g_tuple[2], g_tuple[3], g_tuple[4], g_tuple[5], g_tuple[6], ucac4, bmag, vmag))
        conn_.commit()
        count = 0
        constraint = '<<'
        id_dict.clear()


conn_.commit()
