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


[1]: https://arxiv.org/abs/1710.06739  "Gaia Data Release 1. Cross-match with external catalogues - Algorithm and results"
