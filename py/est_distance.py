import numpy as np
import psycopg2
from scipy.stats import norm


# import numpy.ma as ma
# import matplotlib.pyplot as plt

def prob_parallax(parallax, parallax_error, distance_range):
    x = np.zeros(distance_range.size, dtype={'names':['dist', 'prob'], 'formats':['f4','f8']})
    for i, d in enumerate(distance_range):
        p = norm.pdf(parallax, loc=1.0/d, scale=parallax_error)
        x[i] = (d, p)
    return x


def cumulate(prob_value):
    cum_prob = np.empty(prob_value.size)
    cum = 0;
    for j, p in enumerate(prob_value):
        cum = cum + p
        cum_prob[j] = cum
    return (cum_prob/np.sum(prob_value))*100.0


def find_nearest(array, value):
    idx = (np.abs(array-value)).argmin()
    return idx


def db_connect():
    URI = 'postgresql://{}@{}:{}/{}'.format('postgres', 'localhost', '10000', 'postgres')
    conn = psycopg2.connect(URI)
    return conn


def db_create_table(conn):
    create = conn.cursor()
    create.execute('CREATE TABLE IF NOT EXISTS gaia_distance ('
                   'gaia_source_id bigint NOT NULL,'
                   'moment real,'
                   'distance real,'
                   'distance_lower real,'
                   'distance_upper real);')
    conn.commit()




def get_distance(parallax_, parallax_error_, distance_kpc_):
    probability = prob_parallax(parallax_, parallax_error_, distance_kpc_)
    max_prob = np.max(probability['prob'])
    ind_moment = find_nearest(probability['prob'], max_prob)
    cum_prob = cumulate(probability['prob'])
    ind_5 = find_nearest(cum_prob, 5.0)
    ind_50 = find_nearest(cum_prob, 50.0)
    ind_95 = find_nearest(cum_prob, 95.0)
    moment_ = probability['dist'][ind_moment]
    dist_ = probability['dist'][ind_50]
    lower_ = probability['dist'][ind_50] - probability['dist'][ind_5]
    upper_ = probability['dist'][ind_95] - probability['dist'][ind_50]
    return moment_, dist_, lower_, upper_

if __name__ == "__main__":

    distance_kpc = np.arange(0.01, 20.0, 0.01)

    conn_ = db_connect()
    db_create_table(conn_)
    cur = conn_.cursor()
    cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')

    insert = conn_.cursor()
    count = 0
    for record in cur:
        parallax = record[1]
        parallax_error = record[2]
        moment, dist, lower, upper = get_distance(parallax, parallax_error, distance_kpc)
        print(record[0], parallax, parallax_error, moment, dist, lower, upper)
        insert.execute('INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
                   (record[0], float(moment), float(dist), float(lower), float(upper)))
        count = count + 1
        if (count >= 500):
            conn_.commit()
            count = 0
    conn_.commit()


