package ics.astro.spiralarm.dm;

import ics.astro.spiralarm.app.crossGaiaUcac4;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table( name = "gaia_dr2" )
/**
 * SELECT source_id, l, b, ra, ra_error, dec, dec_error, pmra, pmra_error, pmdec, pmdec_error, parallax, parallax_error, radial_velocity, radial_velocity_error, phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag, bp_rp, bp_g, g_rp FROM gaiadr2.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222) AND pmra IS NOT null AND pmdec IS NOT null AND parallax IS NOT null AND radial_velocity IS NOT null
 */
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

    public gaiaDr2Dm() {
    }

    public gaiaDr2Dm(long sourceId, double l, double b, double ra, double raError, double dec, double decError,
                     double pmra, double pmraError, double pmdec, double pmdecError, double parallax, double parallaxError,
                     double radialVelocity, double radialVelocityError, double photGMeanMag, double photBpMeanMag,
                     double photRpMeanMag, double bpRp, double bpG, double gRp) {
        this.sourceId = sourceId;
        this.l = l;
        this.b = b;
        this.ra = ra;
        this.raError = raError;
        this.dec = dec;
        this.decError = decError;
        this.pmra = pmra;
        this.pmraError = pmraError;
        this.pmdec = pmdec;
        this.pmdecError = pmdecError;
        this.parallax = parallax;
        this.parallaxError = parallaxError;
        this.radialVelocity = radialVelocity;
        this.radialVelocityError = radialVelocityError;
        this.photGMeanMag = photGMeanMag;
        this.photBpMeanMag = photBpMeanMag;
        this.photRpMeanMag = photRpMeanMag;
        this.bpRp = bpRp;
        this.bpG = bpG;
        this.gRp = gRp;
    }

    @Id
    public long getSourceId() {
        return sourceId;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public double getRa() {
        return ra;
    }

    public void setRa(double ra) {
        this.ra = ra;
    }

    public double getRaError() {
        return raError;
    }

    public void setRaError(double raError) {
        this.raError = raError;
    }

    public double getDec() {
        return dec;
    }

    public void setDec(double dec) {
        this.dec = dec;
    }

    public double getDecError() {
        return decError;
    }

    public void setDecError(double decError) {
        this.decError = decError;
    }

    public double getPmra() {
        return pmra;
    }

    public void setPmra(double pmra) {
        this.pmra = pmra;
    }

    public double getPmraError() {
        return pmraError;
    }

    public void setPmraError(double pmraError) {
        this.pmraError = pmraError;
    }

    public double getPmdec() {
        return pmdec;
    }

    public void setPmdec(double pmdec) {
        this.pmdec = pmdec;
    }

    public double getPmdecError() {
        return pmdecError;
    }

    public void setPmdecError(double pmdecError) {
        this.pmdecError = pmdecError;
    }

    public double getParallax() {
        return parallax;
    }

    public void setParallax(double parallax) {
        this.parallax = parallax;
    }

    public double getParallaxError() {
        return parallaxError;
    }

    public void setParallaxError(double parallaxError) {
        this.parallaxError = parallaxError;
    }

    public double getRadialVelocity() {
        return radialVelocity;
    }

    public void setRadialVelocity(double radialVelocity) {
        this.radialVelocity = radialVelocity;
    }

    public double getRadialVelocityError() {
        return radialVelocityError;
    }

    public void setRadialVelocityError(double radialVelocityError) {
        this.radialVelocityError = radialVelocityError;
    }

    public double getPhotGMeanMag() {
        return photGMeanMag;
    }

    public void setPhotGMeanMag(double photGMeanMag) {
        this.photGMeanMag = photGMeanMag;
    }

    public double getPhotBpMeanMag() {
        return photBpMeanMag;
    }

    public void setPhotBpMeanMag(double photBpMeanMag) {
        this.photBpMeanMag = photBpMeanMag;
    }

    public double getPhotRpMeanMag() {
        return photRpMeanMag;
    }

    public void setPhotRpMeanMag(double photRpMeanMag) {
        this.photRpMeanMag = photRpMeanMag;
    }

    public double getBpRp() {
        return bpRp;
    }

    public void setBpRp(double bpRp) {
        this.bpRp = bpRp;
    }

    public double getBpG() {
        return bpG;
    }

    public void setBpG(double bpG) {
        this.bpG = bpG;
    }

    public double getgRp() {
        return gRp;
    }

    public void setgRp(double gRp) {
        this.gRp = gRp;
    }
}
