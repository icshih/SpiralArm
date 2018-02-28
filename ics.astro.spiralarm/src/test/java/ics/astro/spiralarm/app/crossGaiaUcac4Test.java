package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import ics.astro.spiralarm.dm.ucac4Dm;
import ics.astro.tap.TapException;
import ics.astro.tap.TapVIzieR;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class crossGaiaUcac4Test {

    String testQuery = "SELECT TOP 10 g.source_id, g.l, g.b, g.ra, g.ra_error, g.dec, g.dec_error, g.pmra, g.pmra_error, g.pmdec, g.pmdec_error, g.parallax, g.parallax_error, g.phot_g_mean_mag, u.original_ext_source_id AS ucac4_id " +
            "FROM gaiadr1.ucac4_best_neighbour AS u, (SELECT * FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222)) AS g " +
            "WHERE g.source_id = u.source_id AND g.pmra IS NOT null AND g.pmdec IS NOT null AND g.parallax IS NOT null AND u.number_of_mates = 0";

    crossGaiaUcac4 test = new crossGaiaUcac4();

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testGetSelect() {
        System.out.print(crossGaiaUcac4.getSelect("g"));
    }

    @Test
    void testSetStarTable1() throws IOException {
        test.setQuery(testQuery);
        InputStream is = test.query();
        assertNotNull(is);
        StarTable st = test.setStarTable(is);
        assertEquals(10, st.getRowCount());
        assertEquals(15, st.getColumnCount());
    }

    @Test
    void testSetStarTable2() throws IOException {
        StarTable st = test.setStarTable(Paths.get(Paths.get(System.getProperty("user.dir")).getParent().toString(), "data/sa_cgu_test.vot"));
        assertEquals(10, st.getRowCount());
        assertEquals(15, st.getColumnCount());
    }

    @Test
    void testQuery() throws IOException {
        StarTable st = test.setStarTable(Paths.get(Paths.get(System.getProperty("user.dir")).getParent().toString(), "data/sa_cgu_test.vot"));
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
        }
        assertEquals(10, data.size());
    }

    @Test
    void testQueryVizieR() throws IOException, TapException, InterruptedException {
        StarTable st = test.setStarTable(Paths.get(Paths.get(System.getProperty("user.dir")).getParent().toString(), "data/sa_cgu_test.vot"));
        StringBuilder builder = new StringBuilder();
        for (long l = 0; l < st.getRowCount(); l++) {
                builder.append("'").append(String.valueOf(st.getCell(l, 14)).replace("UCAC4-", "")).append("'").append(",");
        }
        List<ucac4Dm> temp = test.queryVizieR(builder);
        assertEquals(10, temp.size());
    }

    @Test
    void test() {
        System.out.println(1998%1000);
        System.out.println(2000%1000);
        System.out.println(2021%1000);
    }
}
