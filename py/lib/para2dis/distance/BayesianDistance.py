import timeit
from multiprocessing import Pool

import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return idx


class BayesianDistance(object):
    """Determination of the distance from the observed parallax and parallax error based on Bayesian Statistics"""
    pool_size = 1

    def __init__(self, parallax, parallax_error):
        self.parallax = parallax
        self.parallax_error = parallax_error
        self.prior = None
        self.__dist_prob = None
        self.__dist_cumu = None
        self.moment = None
        self.mean = None
        self.lower = None
        self.upper = None

    def likelihood(self, distance):
        return norm.pdf(self.parallax, loc=1.0 / distance, scale=self.parallax_error)

    def set_prior(self, prior):
        self.prior = prior

    def multi_prior_likelihood(self, distance):
        return self.prior(distance) * self.likelihood(distance)

    def get_distance_posterior(self, distances):
        dist_prob = np.zeros(distances.size, dtype={'names': ['dist', 'prob'], 'formats': ['f4', 'f8']})
        start = timeit.default_timer()
        with Pool(self.pool_size) as P:
            p_list = P.map(self.multi_prior_likelihood, distances)
        stop = timeit.default_timer()
        for i, d in enumerate(distances):
            dist_prob[i] = (d, p_list[i])
        print('cal. time: {:.2f} sec.'.format(stop - start))
        self.__dist_prob = dist_prob

    def get_distance_cum(self):
        cum_prob = np.empty(self.__dist_prob.size)
        cum = 0
        for j, p in enumerate(self.__dist_prob['prob']):
            cum = cum + p
            cum_prob[j] = cum
        self.__dist_cumu = cum_prob

    def normalise(self):
        return (self.__dist_cumu / np.sum(self.__dist_prob['prob'])) * 100.0

    def get_result(self):
        max_prob = np.max(self.__dist_prob['prob'])
        ind_moment = find_nearest(self.__dist_prob['prob'], max_prob)
        self.get_distance_cum()
        cum_prob = self.normalise()
        ind_5 = find_nearest(cum_prob, 5.0)
        ind_50 = find_nearest(cum_prob, 50.0)
        ind_95 = find_nearest(cum_prob, 95.0)
        self.moment = self.__dist_prob['dist'][ind_moment]
        self.mean = self.__dist_prob['dist'][ind_50]
        self.lower = self.__dist_prob['dist'][ind_50] - self.__dist_prob['dist'][ind_5]
        self.upper = self.__dist_prob['dist'][ind_95] - self.__dist_prob['dist'][ind_50]

    def display_distance_distribution(self):
        fig = plt.figure(figsize=(12, 12))
        ax1 = fig.add_subplot(211)
        ax1.set_title('parallax {:.4f}, fraction: {:.2f}'.format(self.parallax, self.parallax_error / self.parallax))
        ax1.set_xlabel('distance (kpc)')
        ax1.set_ylabel('probability')
        ax1.set_xlim(self.__dist_prob['dist'][0], self.__dist_prob['dist'][-1])
        ax1.plot(self.__dist_prob['dist'], self.__dist_prob['prob'])

        ax2 = fig.add_subplot(212)
        ax2.set_xlabel('distance (kpc)')
        ax2.set_ylabel('percentile')
        ax2.set_xlim(self.__dist_prob['dist'][0], self.__dist_prob['dist'][-1])
        ax2.plot(self.__dist_prob['dist'], self.normalise())
        plt.show()
