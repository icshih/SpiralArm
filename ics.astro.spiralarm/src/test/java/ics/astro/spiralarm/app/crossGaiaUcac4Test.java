package ics.astro.spiralarm.app;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class crossGaiaUcac4Test {

    String testQuery = "SELECT TOP 10 g.source_id, u.original_ext_source_id AS ucac4_id" +
            " FROM gaiadr1.ucac4_best_neighbour AS u, (SELECT * FROM gaiadr1.gaia_source WHERE l < 17 OR l > 285 OR (l > 72 AND l < 222) AND pmra IS NOT null AND pmdec IS NOT null) AS g" +
            " WHERE g.source_id = u.source_id AND u.number_of_mates = 0";

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
    void testQuery() {
        test.setQuery(testQuery);
        InputStream is = test.query();
        assertNotNull(is);

    }
}
