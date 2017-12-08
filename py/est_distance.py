from scipy.stats import norm
import numpy as np
import numpy.ma as ma
import matplotlib.pyplot as plt

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

# distance from 0 to 5 kpc
# providing the parallax_error and for each distance r

# mas
parallax = 2.3537642724378127
parallax_error = 0.07797686605256408
f = parallax_error/parallax
# pc
distance = np.arange(0.01, 20.0, 0.001)

probability = prob_parallax(parallax, parallax_error, distance)
dist_m = probability['dist']

cum_prob = cumulate(probability['prob'])
ind_25 = find_nearest(cum_prob, 25.0)
ind_50 = find_nearest(cum_prob, 50.0)
ind_75 = find_nearest(cum_prob, 75.0)
print('distance: {:.3f} kpc [{:.3f},{:.3f}]'.format(dist_m[ind_50], dist_m[ind_25]-dist_m[ind_50], dist_m[ind_75]-dist_m[ind_50]));

fig = plt.figure(figsize=(12, 12))
ax1 = fig.add_subplot(211)
ax1.set_title('parallax {:.4f}, fraction: {:.2f}'.format(parallax, f))
ax1.set_xlabel('distance (kpc)')
ax1.set_ylabel('probability')
ax1.set_xlim(dist_m[0], dist_m[-1])
ax1.plot(dist_m, probability['prob'])

ax2 = fig.add_subplot(212)
ax2.set_xlabel('distance (kpc)')
ax2.set_ylabel('percentile')
ax2.set_xlim(dist_m[0], dist_m[-1])
ax2.plot(dist_m, cum_prob)
plt.show()



