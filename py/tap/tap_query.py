from astroquery.gaia import Gaia
query = 'SELECT source_id, original_ext_source_id AS ucac4_id FROM gaiadr1.ucac4_best_neighbour ' \
        'WHERE source_id IN (SELECT source_id FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222))'
job = Gaia.launch_job_async(query, output_file='/Users/icshih/Documents/Research/SpiralArm/data/saved/'
                                               'gaia_source_xm_ucac4.vot', dump_to_file=True)

# alternative query
# SELECT g.source_id, g.l, g.b, g.ra, g.dec, g.pmra, g.pmdec, u.ucac4_id
# FROM (
#     SELECT source_id, original_ext_source_id AS ucac4_id
#     FROM gaiadr1.ucac4_best_neighbour
#     WHERE source_id IN (
#         SELECT source_id
#         FROM gaiadr1.gaia_source
#         WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222)
#     )
# ) AS u
# JOIN gaiadr1.gaia_source AS g
# ON (g.source_id = u.source_id)

# from astropy.io.votable import parse
# votable = parse('/Users/icshih/Documents/Research/SpiralArm/data/saved/gaia_source_xm_ucac4.vot')
# data = votable.get_first_table()

data = job.get_results()

import re
id_table = dict()
for d in data:
    g_id = str(d['source_id'])
    u_id = re.sub('UCAC4-', '', d['ucac4_id'].decode(encoding='UTF-8'))
    print('{0}, {1}'.format(u_id, g_id))
    id_table[u_id] = g_id

## Create a table: Gaia_UCAC4_COLOUR {STRING gaia_source_id NOT NULL, STRING ucac4_id NOT NULL, DOUBLE b_mag, DOUBLE v_mag}
import psycopg2
URI = 'postgresql://{}@{}/{}'.format('postgres', 'localhost', 'postgres')
conn = psycopg2.connect(URI)
cur = conn.cursor()
cur.execute('CREATE TABLE gaia_ucac4_colour ('
            'gaia_source_id bigint NOT NULL, '
            'ucac4_id text NOT NULL,'
            'b_mag real,'
            'v_mag real);')

from astroquery.vizier import Vizier
ucac4_table = 'I/322A/out'
for u_id, g_id in id_table.items():
    result = Vizier.query_constraints(catalog=ucac4_table, UCAC4='="{0}"'.format(u_id))
    data = result['I/322A/out']
    b = data.field('Bmag').data[0]
    # eb = data.field('e_Bmag').data[0]
    v = data.field('Vmag').data[0]
    # ev = data.field('e_Vmag').data[0]
    print('{0} {1} {2} {3}'.format(g_id, u_id, b, v))