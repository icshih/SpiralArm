package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import ics.astro.spiralarm.dm.ucac4Dm;
import ics.astro.tap.TapException;
import ics.astro.tap.TapGacs;
import ics.astro.tap.TapVIzieR;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StarTableFactory;
import uk.ac.starlink.votable.VOTableBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * SELECT g.source_id, u.original_ext_source_id AS ucac4_id
 FROM gaiadr1.ucac4_best_neighbour AS u, (SELECT * FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222) AND pmra IS NOT null AND pmdec IS NOT null) AS g
 WHERE g.source_id = u.source_id AND u.number_of_mates = 0
 */
public class crossGaiaUcac4H {

    public static final String crossTableName = "CROSS_GAIA_UCAC4";
    public static final String ucac4TableName = "UCAC4";
    public String query;
    private TapVIzieR vizier;
    private SessionFactory sessionFactory;;

    private static final Logger logger = LoggerFactory.getLogger(crossGaiaUcac4H.class);

    public crossGaiaUcac4H() {
        StandardServiceRegistry standardRegistry = new StandardServiceRegistryBuilder()
                .configure( "hibernate.cfg.xml" )
                .build();
        sessionFactory = new MetadataSources(standardRegistry).buildMetadata().buildSessionFactory();
            vizier = new TapVIzieR();
    }

    /**
     * Closes Entity Manager Factory
     */
    public void closeEntityManager() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
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
    InputStream query() {
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
     * Gets main Gaia-UCAC4 crossed table
     * @return
     * @throws IOException
     */
    public StarTable getCrossGaiaUCAC4(Path path) throws IOException {
        StarTable st;
        if (path == null)
            st = setStarTable(query());
        else
            st = setStarTable(path);
        RowSequence rows = st.getRowSequence();
        Object[] row;
        List<crossGaiaUcac4Dm> data = new ArrayList<>();
        crossGaiaUcac4Dm d;
        while (rows.next()) {
            row = rows.getRow();
            System.out.println(String.format("%s %s", row[0], String.valueOf(row[14])).replace("UCAC4-", ""));
            d = new crossGaiaUcac4Dm();
            d.setSourceId((long) row[0]);
            d.setL((double) row[1]);
            d.setB((double) row[2]);
            d.setRa((double) row[3]);
            d.setRaError((double) row[4]);
            d.setDec((double) row[5]);
            d.setRaError((double) row[6]);
            d.setPmra((double) row[7]);
            d.setPmraError((double) row[8]);
            d.setPmdec((double) row[9]);
            d.setPmdecError((double) row[10]);
            d.setParallax((double) row[11]);
            d.setParallaxError((double) row[12]);
            d.setPhotGMeanMag((double) row[13]);
            d.setUcac4Id(String.valueOf(row[14]).replace("UCAC4-", ""));
            data.add(d);
            if (data.size() >= 1000) {
                insertTable(data);
                data.clear();
            }
        }
        if (!data.isEmpty())
            insertTable(data);
        return st;
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
        List<ucac4Dm> temp;
        StringBuilder builder = new StringBuilder();
        for (long l = 0; l < cguTable.getRowCount(); l++) {
                builder.append("'").append(String.valueOf(cguTable.getCell(l, indexUcac4Id)).replace("UCAC4-", "")).append("'").append(",");
            if ((l+1)%400 == 0) {
                temp = synchQueryVizieR(builder);
                if (temp != null)
                    insertTable(temp);
                else
                    logger.error("Query returns null.");
                builder = new StringBuilder();
            }
        }
        if (builder.length() != 0) {
            temp = synchQueryVizieR(builder);
            if (temp != null)
                insertTable(temp);
            else
                logger.error("Query returns null.");
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
    List<ucac4Dm> asynchQueryVizieR(StringBuilder builder) throws IOException, TapException, InterruptedException {
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
        vizier.deleteJob(jobId);
        return temp;
    }

    /**
     * Queries UCAC4 table
     * @param builder
     * @return
     * @throws IOException
     */
    List<ucac4Dm> synchQueryVizieR(StringBuilder builder) throws IOException {
        List<ucac4Dm> temp = null;
        String query = "SELECT ucac4, bmag, vmag FROM \"I/322A/out\" WHERE UCAC4 IN (%s)";
        InputStream is = vizier.runSynchronousQuery(String.format(query, builder.substring(0, builder.length()-1)), "votable");
        if (is != null) {
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
            logger.error("The query returns null");
        }
        return temp;
    }

    void insertTable(List<?> objects) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        for (Object o : objects)
            session.persist(o);
        session.getTransaction().commit();
        session.close();
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

    static void run() {
        crossGaiaUcac4H app = new crossGaiaUcac4H();
        try {
            StarTable st = app.getCrossGaiaUCAC4(Paths.get("/Users/icshih/Documents/Research/SpiralArm/data/sa_crossGaiaUcac4.vot"));
            app.getUcac4Photometry(st, 14);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TapException e) {
            e.printStackTrace();
        }
        app.closeEntityManager();
    }

    /**
     * Annotation with Hibernate API
     *
     * JPA has split module issue in Java 9, see https://stackoverflow.com/questions/48244184/java-9-hibernate-and-java-sql-javax-transaction
     * @param args
     */
    public static void main(String[] args) {
        crossGaiaUcac4H app = new crossGaiaUcac4H();
    }
}
