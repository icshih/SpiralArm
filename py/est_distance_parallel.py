import psycopg2
from scipy.stats import norm
import numpy as np
# import numpy.ma as ma
# import matplotlib.pyplot as plt
from multiprocessing import Pool


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


class Distance:

    def __init__(self, parallax, parallax_error):
        self.parallax = parallax
        self.parallax_error = parallax_error
        self.pool_size = 1
        self.dist_prob = None

    def get_likelihood(self, distance):
        return norm.pdf(self.parallax, loc=1.0 / distance, scale=self.parallax_error)

    def get_distance_prob(self, distances):
        d_list = list(distances)
        print(d_list)
        with Pool(self.pool_size) as P:
            p_list = P.map(self.get_likelihood, d_list)
            print(p_list)
        self.dist_prob = np.asarray([d_list, p_list], dtype={'names': ['dist', 'prob'], 'formats': ['f4', 'f8']})

    def get_distance_cum(self):
        cum_prob = np.empty(self.dist_prob.size)
        cum = 0;
        for j, p in enumerate(self.dist_prob['prob']):
            cum = cum + p
            cum_prob[j] = cum
        return (cum_prob / np.sum(self.dist_prob['prob'])) * 100.0

    def get_result(self):
        max_prob = np.max(self.dist_prob['prob'])
        ind_moment = find_nearest(self.dist_prob['prob'], max_prob)
        cum_prob = self.get_distance_cum()
        ind_5 = find_nearest(cum_prob, 5.0)
        ind_50 = find_nearest(cum_prob, 50.0)
        ind_95 = find_nearest(cum_prob, 95.0)
        moment = self.dist_prob['dist'][ind_moment]
        mean = self.dist_prob['dist'][ind_50]
        lower = self.dist_prob['dist'][ind_50] - self.dist_prob['dist'][ind_5]
        upper = self.dist_prob['dist'][ind_95] - self.dist_prob['dist'][ind_50]
        return moment, mean, lower, upper


if __name__ == "__main__":
    parallax_ = 2.3537642724378127
    parallax_error_ = 0.07797686605256408
    distance_kpc = distance_range(20.0, 0.01)

    test = Distance(parallax_, parallax_error_)
    test.pool_size = 5
    test.get_distance_prob(distance_kpc)
    print(test.dist_prob)

    # conn_ = db_connect()
    # db_create_table(conn_)
    # cur = conn_.cursor()
    # cur.execute('SELECT gaia_source_id, parallax, parallax_error FROM gaia_ucac4_colour WHERE parallax > 0;')
    #
    # insert = conn_.cursor()
    # count = 0
    # for record in cur:
    #     parallax = record[1]
    #     parallax_error = record[2]
    #     moment, dist, lower, upper = get_distance(parallax, parallax_error, distance_kpc)
    #     print(record[0], parallax, parallax_error, moment, dist, lower, upper)
    #     insert.execute('INSERT INTO gaia_distance (gaia_source_id, moment, distance, distance_lower, distance_upper) VALUES (%s, %s, %s, %s, %s);',
    #                (record[0], float(moment), float(dist), float(lower), float(upper)))
    #     count = count + 1
    #     if (count >= 500):
    #         conn_.commit()
    #         count = 0
    # conn_.commit()

# distance from 0 to 20 kpc
# providing the parallax_error and for each distance r

# mas
# parallax = 2.3537642724378127
# parallax_error = 0.07797686605256408
# f = parallax_error/parallax
# pc

# dist_m = probability['dist']
#
# cum_prob = cumulate(probability['prob'])
# ind_25 = find_nearest(cum_prob, 25.0)
# ind_50 = find_nearest(cum_prob, 50.0)
# ind_75 = find_nearest(cum_prob, 75.0)
# print('distance: {:.3f} kpc [{:.3f},{:.3f}]'.format(dist_m[ind_50], dist_m[ind_25]-dist_m[ind_50], dist_m[ind_75]-dist_m[ind_50]));
#
# fig = plt.figure(figsize=(12, 12))
# ax1 = fig.add_subplot(211)
# ax1.set_title('parallax {:.4f}, fraction: {:.2f}'.format(parallax, f))
# ax1.set_xlabel('distance (kpc)')
# ax1.set_ylabel('probability')
# ax1.set_xlim(dist_m[0], dist_m[-1])
# ax1.plot(dist_m, probability['prob'])
#
# ax2 = fig.add_subplot(212)
# ax2.set_xlabel('distance (kpc)')
# ax2.set_ylabel('percentile')
# ax2.set_xlim(dist_m[0], dist_m[-1])
# ax2.plot(dist_m, cum_prob)
# plt.show()
