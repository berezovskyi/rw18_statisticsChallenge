package rw2018.statistics.impl;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TBD
 *
 * @version $version-stub$
 * @since FIXME
 */
public class StatisticsMapDBImplTest {

    @Test
    public void testBits() {
        final StatisticsMapDBImpl db = new StatisticsMapDBImpl();
        assertEquals(5, db.chunkBits(17));
        assertEquals(4, db.chunkBits(16));
    }
}
