from astroquery.gaia import Gaia
# query = 'SELECT source_id, original_ext_source_id AS ucac4_id FROM gaiadr1.ucac4_best_neighbour ' \
#         'WHERE source_id IN (SELECT source_id FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222))'

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
'JOIN gaiadr1.gaia_source AS g ' \
'ON (g.source_id = u.source_id)'

job = Gaia.launch_job_async(query, output_file='/Users/icshih/Documents/Research/SpiralArm/data/saved/'
                                               'gaia_source_xm_ucac4.vot', dump_to_file=True)

# from astropy.io.votable import parse
# votable = parse('/Users/icshih/Documents/Research/SpiralArm/data/saved/gaia_source_xm_ucac4.vot')
# data = votable.get_first_table()

data = job.get_results()

import re
import numpy.ma as ma
id_table = dict()
for d in data:
    u_id = re.sub('UCAC4-', '', d['ucac4_id'].decode(encoding='UTF-8'))
    f = ma.filled(d, -999)
    g_id = f['source_id'].item(0)
    l = f['l'].item(0)
    b = f['b'].item(0)
    ra = f['ra'].item(0)
    dec = f['dec'].item(0)
    pmra = f['pmra'].item(0)
    pmdec = f['pmdec'].item(0)
    print('{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}'.format(u_id, g_id, l, b, ra, dec, pmra, pmdec))
    id_table[u_id] = g_id, l, b, ra, dec, pmra, pmdec

## Create a table: Gaia_UCAC4_COLOUR {STRING gaia_source_id NOT NULL, STRING ucac4_id NOT NULL, DOUBLE b_mag, DOUBLE v_mag}
import psycopg2
URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
conn = psycopg2.connect(URI)
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

from astroquery.vizier import Vizier
ucac4_table = 'I/322A/out'
for u_id, value in id_table.items():
    result = Vizier.query_constraints(catalog=ucac4_table, UCAC4='="{0}"'.format(u_id))
    data = result['I/322A/out']
    b = float(data.field('Bmag').data[0])
    # eb = data.field('e_Bmag').data[0]
    v = float(data.field('Vmag').data[0])
    # ev = data.field('e_Vmag').data[0]
    print('{0} {1} {2} {3}'.format(value[0], u_id, b, v))
    cur.execute('INSERT INTO gaia_ucac4_colour (gaia_source_id, l, b, ra, dec, pmra, pmdec, ucac4_id, b_mag, v_mag) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', (value[0], value[1], value[2], value[3], value[4], value[5], value[6], u_id, b, v))

#conn.commit()
cur.close()
conn.close()