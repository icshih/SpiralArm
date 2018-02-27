package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import ics.astro.tap.TapGacs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Field;

/**
 * SELECT g.source_id, u.original_ext_source_id AS ucac4_id
 FROM gaiadr1.ucac4_best_neighbour AS u, (SELECT * FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222) AND pmra IS NOT null AND pmdec IS NOT null AND parallax_error/parallax < 0.2) AS g
 WHERE g.source_id = u.source_id AND u.number_of_mates = 0
 */
public class crossGaiaUcac4 {

    private static final Logger logger = LoggerFactory.getLogger(crossGaiaUcac4.class);

    public static final String tableName = "CROSS_GAIA_UCAC4";

    public String query = String.format("SELECT %s " +
            "FROM (" +
            "SELECT source_id, original_ext_source_id AS ucac4_id " +
            "FROM gaiadr1.ucac4_best_neighbour " +
            "WHERE source_id IN (" +
            "SELECT source_id " +
            "FROM gaiadr1.gaia_source " +
            "WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222)) AND number_of_mates = 0) AS u " +
            "JOIN gaiadr1.gaia_source AS g " +
            "ON (g.source_id = u.source_id) " +
            "WHERE g.pmra IS NOT null AND g.pmdec IS NOT null AND g.parallax_error/g.parallax < 0.2", getSelect("g"));

    /**
     * Set up and override the default query to extract data from main Gaia and the crossed UCAC4 tables
     * @param query
     */
    public void setQuery(String query) {
        this.query = query;
    }

    public static String getSelect(String prefix) {
        StringBuilder builder = new StringBuilder();
        for (Field f : crossGaiaUcac4Dm.class.getDeclaredFields()) {
            builder.append(prefix).append(".").append(f.getName()).append(",");
        }
        return builder.substring(0, builder.length() - 1);
    }

    /**
     * Runs asynchronous job and waiting for the final result
     * @return inputStream of the job result
     */
    public InputStream query() {
        InputStream is = null;
        TapGacs gacs = new TapGacs();
        try {
            String jobId = gacs.runAsynchronousJob(this.query);
            String result = gacs.updateJobPhase(jobId, 1000);
            if (result.equals("COMPLETED")) {
                is = gacs.getJobResult(jobId);
            } else
                logger.error(gacs.getJobError(jobId));
        } catch (Exception e) {
           logger.error("", e);
        }
        return is;
    }
}
