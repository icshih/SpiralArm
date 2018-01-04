import timeit
from multiprocessing import Pool

# import numpy.ma as ma
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
from scipy.stats import norm

import sys
sys.path.append('py/lib/para2dis/distance')



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


def distance_range(max_distance, step):
    d = 0.01
    while d <= max_distance:
        yield d
        d += step


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return idx


class Distance(object):
    pool_size = 1

    def __init__(self, parallax, parallax_error):
        self.parallax = parallax
        self.parallax_error = parallax_error
        self.dist_prob = None
        self.dist_cumu = None
        self.moment = None
        self.mean = None
        self.lower = None
        self.upper = None

    def likelihood(self, distance):
        return norm.pdf(self.parallax, loc=1.0 / distance, scale=self.parallax_error)

    def get_distance_prob(self, distances):
        self.dist_prob = np.zeros(distances.size, dtype={'names': ['dist', 'prob'], 'formats': ['f4', 'f8']})
        start = timeit.default_timer()
        with Pool(self.pool_size) as P:
            p_list = P.map(self.likelihood, distances)
        stop = timeit.default_timer()
        for i, d in enumerate(distances):
            self.dist_prob[i] = (d, p_list[i])
        print(stop - start)

    def get_distance_cum(self):
        cum_prob = np.empty(self.dist_prob.size)
        cum = 0;
        for j, p in enumerate(self.dist_prob['prob']):
            cum = cum + p
            cum_prob[j] = cum
        self.dist_cumu = cum_prob

    def normalise(self):
        return (self.dist_cumu / np.sum(self.dist_prob['prob'])) * 100.0

    def get_result(self):
        max_prob = np.max(self.dist_prob['prob'])
        ind_moment = find_nearest(self.dist_prob['prob'], max_prob)
        self.get_distance_cum()
        cum_prob = self.normalise()
        ind_5 = find_nearest(cum_prob, 5.0)
        ind_50 = find_nearest(cum_prob, 50.0)
        ind_95 = find_nearest(cum_prob, 95.0)
        self.moment = self.dist_prob['dist'][ind_moment]
        self.mean = self.dist_prob['dist'][ind_50]
        self.lower = self.dist_prob['dist'][ind_50] - self.dist_prob['dist'][ind_5]
        self.upper = self.dist_prob['dist'][ind_95] - self.dist_prob['dist'][ind_50]

    def display(self):
        fig = plt.figure(figsize=(12, 12))
        ax1 = fig.add_subplot(211)
        ax1.set_title('parallax {:.4f}, fraction: {:.2f}'.format(self.parallax, self.parallax_error / self.parallax))
        ax1.set_xlabel('distance (kpc)')
        ax1.set_ylabel('probability')
        ax1.set_xlim(self.dist_prob['dist'][0], self.dist_prob['dist'][-1])
        ax1.plot(self.dist_prob['dist'], self.dist_prob['dist']['prob'])

        ax2 = fig.add_subplot(212)
        ax2.set_xlabel('distance (kpc)')
        ax2.set_ylabel('percentile')
        ax2.set_xlim(self.dist_prob['dist'][0], self.dist_prob['dist'][-1])
        ax2.plot(self.dist_prob['dist'], self.normalise())
        plt.show()


if __name__ == "__main__":
    distance_range = np.arange(0.01, 20.0, 0.01)
    Distance.pool_size = 4

    conn_ = db_connect()
    db_create_table(conn_)
    cur = conn_.cursor()
    cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')

    insert = conn_.cursor()
    count = 0
    for record in cur:
        parallax_ = record[1]
        parallax_error_ = record[2]
        d = Distance(parallax_, parallax_error_)
        d.get_distance_prob(distance_range)
        d.get_result()
        print(record[0], parallax_, parallax_error_, d.moment, d.mean, d.lower, d.upper)
    #     insert.execute('INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
    #                (record[0], float(moment), float(dist), float(lower), float(upper)))
    #     count = count + 1
    #     if (count >= 500):
    #         conn_.commit()
    #         count = 0
    # conn_.commit()
