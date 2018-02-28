package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import ics.astro.spiralarm.dm.ucac4Dm;
import ics.astro.tap.TapException;
import ics.astro.tap.TapGacs;
import ics.astro.tap.TapVIzieR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StarTableFactory;
import uk.ac.starlink.votable.VOTableBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * SELECT g.source_id, u.original_ext_source_id AS ucac4_id
 FROM gaiadr1.ucac4_best_neighbour AS u, (SELECT * FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222) AND pmra IS NOT null AND pmdec IS NOT null) AS g
 WHERE g.source_id = u.source_id AND u.number_of_mates = 0
 */
public class crossGaiaUcac4 {

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
    TapVIzieR vizier;

    private static final Logger logger = LoggerFactory.getLogger(crossGaiaUcac4.class);

    public crossGaiaUcac4() {
        vizier = new TapVIzieR();
    }
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

    /**
     * Gets UCAC4 B, V Magnitudes
     * @param cguTable
     * @param indexUcac4Id
     * @throws IOException
     * @throws TapException
     * @throws InterruptedException
     */
    public void getUcac4Photometry(StarTable cguTable, int indexUcac4Id) throws IOException, TapException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (long l = 0; l < cguTable.getRowCount(); l++) {
                builder.append("'").append(String.valueOf(cguTable.getCell(l, indexUcac4Id)).replace("UCAC4-", "")).append("'").append(",");
            if ((l+1)%1000 == 0) {
                queryVizieR(builder);
                builder = new StringBuilder();
            }
        }
        if (builder.length() != 0) {
            queryVizieR(builder);
        }
    }

    /**
     * Queries UCAC4 table
     * @param builder
     * @return
     * @throws IOException
     * @throws TapException
     * @throws InterruptedException
     */
    List<ucac4Dm> queryVizieR(StringBuilder builder) throws IOException, TapException, InterruptedException {
        List<ucac4Dm> temp = null;
        String query = "SELECT ucac4, bmag, vmag FROM \"I/322A/out\" WHERE UCAC4 IN (%s)";
        String jobId = vizier.runAsynchronousJob(String.format(query, builder.substring(0, builder.length()-1)), "votable");
        String phase = vizier.updateJobPhase(jobId, 1000);
        if (phase.equals("COMPLETED")) {
            InputStream is = vizier.getJobResult(jobId);
            StarTable st = this.setStarTable(is);
            RowSequence rows = st.getRowSequence();
            Object[] row;
            temp = new ArrayList<>();
            ucac4Dm data;
            while (rows.next()) {
                row = rows.getRow();
                data = new ucac4Dm(String.valueOf(row[0]), (float) row[1], (float) row[2]);
                temp.add(data);
            }
        } else {
            logger.error("The query job ended in {}", phase);
        }
        return temp;
    }

    void insertTable() {

    }

    /**
     * Get StarTable of STIL
     */
    StarTable setStarTable(InputStream is) throws IOException {
        // initiate row counting
        StarTableFactory factory = new StarTableFactory();
        return factory.makeStarTable(is, new VOTableBuilder());
    }

    /**
     * Get StarTable of STIL
     */
    StarTable setStarTable(Path path) throws IOException {
        InputStream is = new FileInputStream(path.toFile());
        return setStarTable(is);
    }

    public static void main(String[] args) {

        crossGaiaUcac4 app = new crossGaiaUcac4();
        StarTable st = null;
        try {
            st = app.setStarTable(app.query());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Object[] obj;
        for (long l = 0; l < st.getRowCount(); l++) {
            try {
                obj = st.getRow(l);

            } catch (IOException e) {
                logger.error("", e);
            }
        }
    }
}
