# Data Selection

## Position

Galatic longitudes: l < 17, or l > 285, or (l > 72 && l < 222)

## Colour

Because Gaia DR1 does not have colour information, it is necessary to cross-match external catalogue(s) containing the photometry info.

We choose the external catalogues which have been cross-matched with Gaia DR1 by Gaia DPAC[1]. The externals must cover the galactic plane region, so the below catalogues are selected for further study.

* UCAC4, [Zacharias et al. 2013](https://ui.adsabs.harvard.edu/#abs/2013AJ....145...44Z/abstract):
The photometry data is from 2MASS (J, K, B, V) and APASS (r, i)

* GSC 2.3, [Lasker et al. 2008](https://ui.adsabs.harvard.edu/#abs/2008AJ....136..735L/abstract): 
The photometry bands: Bj, B, V, U, N (IR)

* PPMXL, [Roeser et al. 2010](https://ui.adsabs.harvard.edu/#abs/2010AJ....139.2440R/abstract):
The Photometry data is from 2MASS (IR: J, H, Ks) and USNO-B1.0 (Optical: B, R, I)

* 2MASS PSC [Skrutskie et al. 2006](https://ui.adsabs.harvard.edu/#abs/2006AJ....131.1163S/abstract):

* allWISE [Cutri & et al. 2013](https://ui.adsabs.harvard.edu/#abs/2013yCat.2328....0C/abstract):
This is an infra-red survey, so we will not use the data here.

Using "the Best Neighbour" cross-match table

## Distance

We have collected data derived from the cross-matching of Gaia DR1 - UCAC4 catalogues. The data contains **parallax** and **parallax error** from Gaia[2]. Although parallax is the inverse of distance, 1/r, we cannot apply this directly to our data, because what we really have is the *measured* value of the true distance.
We also need to consider the uncertainty which can distort the distance estimation greatly. To properly estimate the distance from parallax, I refer to the methodology of [Bailer-Jones, 2015](https://ui.adsabs.harvard.edu/#abs/2015PASP..127..994B/abstract).

In short, when the ratio of parallax error to parallax (*f*) is smaller enough, the distance and the uncertainty estimated by the measurement model* is quite acceptable, thus that range of *f* is quite limit. 

In this work, we need to select the objects within 3 kpc. This is not a problem in the DR1 as the objects with parallax data are Tycho sources which are generally bright (< 14 G mag) and with good quality of parallax, we can use the model to find out the distance without to much problem.
 
Here, we set *f = 0.2* as the criteria to select the data.

However, we will need a better way to treat the DR2 data as most of them will have parallax values outside the confort zone of the model, this will be the next step.

[1]: https://arxiv.org/abs/1710.06739  "Gaia Data Release 1. Cross-match with external catalogues - Algorithm and results"
[2]: Only the *primary* sources
