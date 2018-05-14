package ics.astro.spiralarm.dm;

import ics.astro.spiralarm.app.crossGaiaUcac4;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table( name = "gaia_dr2" )
public class gaiaDr2Dm {

    long sourceId;
    double l;
    double b;
    double ra;
    double raError;
    double dec;
    double decError;
    double pmra;
    double pmraError;
    double pmdec;
    double pmdecError;
    double parallax;
    double parallaxError;
    double radialVelocity;
    double radialVelocityError;
    double photGMeanMag;
    double photBpMeanMag;
    double photRpMeanMag;
    double bpRp;
    double bpG;
    double gRp;
}
