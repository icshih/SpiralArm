package ics.astro.spiralarm.dm;

import ics.astro.spiralarm.app.crossGaiaUcac4;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table( name = crossGaiaUcac4.tableName )
public class crossGaiaUcac4Dm {

    long sourceId;
    float l;
    float b;
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
    double photGMeanMag;
    double ucac4Id;
    double bMag;
    double vMag;

    @Id
    public long getSourceId() {
        return sourceId;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public float getL() {
        return l;
    }

    public void setL(float l) {
        this.l = l;
    }

    public float getB() {
        return b;
    }

    public void setB(float b) {
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

    public double getPhotGMeanMag() {
        return photGMeanMag;
    }

    public void setPhotGMeanMag(double photGMeanMag) {
        this.photGMeanMag = photGMeanMag;
    }

    public double getUcac4Id() {
        return ucac4Id;
    }

    public void setUcac4Id(double ucac4Id) {
        this.ucac4Id = ucac4Id;
    }

    public double getbMag() {
        return bMag;
    }

    public void setbMag(double bMag) {
        this.bMag = bMag;
    }

    public double getvMag() {
        return vMag;
    }

    public void setvMag(double vMag) {
        this.vMag = vMag;
    }




}
