package ics.astro.spiralarm.dm;

import ics.astro.spiralarm.app.crossGaiaUcac4;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table( name = crossGaiaUcac4.ucac4TableName )
public class ucac4Dm {

    String ucac4Id;
    float bMag;
    float vMag;

    public ucac4Dm(String ucac4Id, float bMag, float vMag) {
        this.ucac4Id = ucac4Id;
        this.bMag = bMag;
        this.vMag = vMag;
    }

    @Id
    public String getUcac4Id() {
        return ucac4Id;
    }

    public void setUcac4Id(String ucac4Id) {
        this.ucac4Id = ucac4Id;
    }

    public float getbMag() {
        return bMag;
    }

    public void setbMag(float bMag) {
        this.bMag = bMag;
    }

    public float getvMag() {
        return vMag;
    }

    public void setvMag(float vMag) {
        this.vMag = vMag;
    }

}
