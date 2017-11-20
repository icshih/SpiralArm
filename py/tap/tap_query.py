from astropy.io.votable import parse

votable = parse('/Users/icshih/Documents/Research/SpiralArm/data/saved/gaia_source_xm_ucac4.vot')
table = votable.get_first_table()
import re

uid = list()
import numpy as np

for u in np.asarray(table.array['ucac4_id'], dtype='str'):
    uid.append(re.sub('UCAC4-', '', u))

from astroquery.vizier import Vizier
ucac4_table = 'I/322A/out'
for id in uid:
    result = Vizier.query_constraints(columns=["UCAC4", "Bmag", "e_Bmag", "Vmag", "e_Vmag"], catalog=ucac4_table, UCAC4='="{0}"'.format(id))
    data = result['I/322A/out']
    b = data.field('Bmag').data[0]
    eb = data.field('e_Bmag').data[0]
    v = data.field('Vmag').data[0]
    ev = data.field('e_Vmag').data[0]
    print('{0} {1} {2} {3} {4}'.format(id, b, eb, v, ev))