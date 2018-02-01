import configparser
import re
import sys

import numpy as np
import numpy.ma as ma
import psycopg2
from astroquery.gaia import Gaia
from astroquery.vizier import Vizier

query = 'SELECT g.source_id, g.l, g.b, g.ra, g.dec, g.pmra, g.pmdec, g.parallax, g.parallax_error, g.phot_g_mean_mag, g.phot_g_mean_flux, u.ucac4_id ' \
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
        'WHERE g.pmra IS NOT null AND g.pmdec IS NOT null AND g.parallax_error/g.parallax < 0.2'


def db_connect(host, port, db_name, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)


def db_create_table(connection):
    """Create Gaia - External Catalogue cross-matching table"""
    create = connection.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS gaia_ucac4_colour ('
                   'gaia_source_id bigint NOT NULL, '
                   'l double precision,'
                   'b double precision,'
                   'ra double precision,'
                   'dec double precision,'
                   'pmra double precision,'
                   'pmdec double precision,'
                   'parallax double precision,'
                   'parallax_error double precision,'
                   'phot_g_mean_mag double precision,'
                   'phot_g_mean_flux double precision,'
                   'ucac4_id text NOT NULL,'
                   'b_mag float,'
                   'v_mag float);')
    connection.commit()


def get_and_ingest(connection, id_dict, constraint):
    insert = connection.cursor()
    result = Vizier.query_constraints(catalog='I/322A/out', UCAC4=constraint)
    dataset = result[0]
    for i in np.arange(len(dataset)):
        ucac4 = dataset.field('UCAC4')[i]
        bmag = float(dataset.field('Bmag')[i])
        vmag = float(dataset.field('Vmag')[i])
        g_tuple = id_dict[ucac4]
        insert.execute(
            'INSERT INTO gaia_ucac4_colour (gaia_source_id, l, b, ra, dec, pmra, pmdec, parallax, parallax_error, phot_g_mean_mag, phot_g_mean_flux, ucac4_id, b_mag, v_mag) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
            (g_tuple[0], g_tuple[1], g_tuple[2], g_tuple[3], g_tuple[4], g_tuple[5], g_tuple[6], g_tuple[7], g_tuple[8],
             g_tuple[9], g_tuple[10], ucac4, bmag, vmag))
    connection.commit()


def process(connection, dataset):
    count = 0
    constraint = '<<'
    id_dict = dict()
    for d in dataset:
        f = ma.filled(d)
        u_id = re.sub('UCAC4-', '', f['ucac4_id'].item(0).decode(encoding='UTF-8'))
        g_id = f['source_id'].item(0)
        l = f['l'].item(0)
        b = f['b'].item(0)
        ra = f['ra'].item(0)
        dec = f['dec'].item(0)
        pmra = f['pmra'].item(0)
        pmdec = f['pmdec'].item(0)
        parallax = f['parallax'].item(0)
        parallax_error = f['parallax_error'].item(0)
        gmag = f['phot_g_mean_mag'].item(0)
        gflux = f['phot_g_mean_flux'].item(0)
        id_dict[u_id] = g_id, l, b, ra, dec, pmra, pmdec, parallax, parallax_error, gmag, gflux
        constraint = constraint + ';' + u_id
        count = count + 1
        if count >= 500:
            get_and_ingest(connection, id_dict, constraint)
            count = 0
            constraint = '<<'
            id_dict.clear()
    get_and_ingest(connection, id_dict, constraint)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: sa_cross_match.py /path/to/sa.conf')
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

    conn = db_connect(HOST, PORT, DB, USER, PWORD)
    job = Gaia.launch_job_async(query, output_file=OUTPUT, dump_to_file=True)
    job_result = job.get_results()
    # using constraint in VizieR, http://vizier.cfa.harvard.edu/vizier/vizHelp/cst.htx#char
    db_create_table(conn)
    process(conn, job_result)
    conn.close()
