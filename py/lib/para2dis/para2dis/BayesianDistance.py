from multiprocessing import Pool

# import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return idx


class BayesianDistance(object):
    """Determination of the distance from the observed parallax and parallax error based on Bayesian Statistics"""
    pool_size = 1
    distance_range = np.arange(0.01, 20.0, 0.01)

    def __init__(self, source_id, parallax, parallax_error, prior, distances, pool_size):
        self.gaia_source_id = source_id
        self.parallax = parallax
        self.parallax_error = parallax_error
        self.distance_range = distances
        self.pool_size = pool_size
        self.prior = prior
        self.__dist_prob = None
        self.__dist_cumu = None
        self.moment = None
        self.distance = None
        self.distance_lower = None
        self.distance_upper = None

    def likelihood(self, distances):
        return norm.pdf(self.parallax, loc=1.0 / distances, scale=self.parallax_error)

    def set_prior(self, prior):
        self.prior = prior

    def multi_prior_likelihood(self, distances):
        if self.prior is None:
            return self.likelihood(distances)
        else:
            return self.prior(distances) * self.likelihood(distances)

    def sequence(self, distance_range):
        dist_prob = np.zeros(distance_range.size, dtype={'names': ['dist', 'prob'], 'formats': ['f4', 'f8']})
        for i, d in enumerate(distance_range):
            dist_prob[i] = (d, self.multi_prior_likelihood(d))
        return dist_prob

    def parallel(self, distance_range):
        dist_prob = np.zeros(distance_range.size, dtype={'names': ['dist', 'prob'], 'formats': ['f4', 'f8']})
        with Pool(self.pool_size) as P:
            p_list = P.map(self.multi_prior_likelihood, self.distance_range)
        for i, d in enumerate(self.distance_range):
            dist_prob[i] = (d, p_list[i])
        return dist_prob

    def get_distance_posterior(self):
        if self.pool_size == 1:
            dist_prob = self.sequence(self.distance_range)
        else:
            dist_prob = self.parallel(self.distance_range)
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
        self.distance = self.__dist_prob['dist'][ind_50]
        self.distance_lower = self.__dist_prob['dist'][ind_50] - self.__dist_prob['dist'][ind_5]
        self.distance_upper = self.__dist_prob['dist'][ind_95] - self.__dist_prob['dist'][ind_50]

    def calculate(self):
        self.get_distance_posterior()
        self.get_result()
        return (self.gaia_source_id, self.moment, self.distance, self.distance_lower, self.distance_upper)


    # def display_distance_distribution(self):
    #     fig = plt.figure(figsize=(12, 12))
    #     ax1 = fig.add_subplot(211)
    #     ax1.set_title('parallax {:.4f}, fraction: {:.2f}'.format(self.parallax, self.parallax_error / self.parallax))
    #     ax1.set_xlabel('distance (kpc)')
    #     ax1.set_ylabel('probability')
    #     ax1.set_xlim(self.__dist_prob['dist'][0], self.__dist_prob['dist'][-1])
    #     ax1.plot(self.__dist_prob['dist'], self.__dist_prob['prob'])
    #
    #     ax2 = fig.add_subplot(212)
    #     ax2.set_xlabel('distance (kpc)')
    #     ax2.set_ylabel('percentile')
    #     ax2.set_xlim(self.__dist_prob['dist'][0], self.__dist_prob['dist'][-1])
    #     ax2.plot(self.__dist_prob['dist'], self.normalise())
    #     plt.show()
