package org.cloudname.mon;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit test for AverageLong.
 * @author espen
 *
 */
public class AverageLongTest {

    private static AverageLong avgVar = AverageLong.getAverageLong("test.cloudname.average");
    
    @Test
    public void testSimple() {
        //record some values
        avgVar.record(0);
        avgVar.record(10);
        avgVar.record(20);
        avgVar.record(10);
        avgVar.record(10);
        
        AverageLongData records = avgVar.getRecords();
        
        assertEquals(50, records.getAggregated());
        assertEquals(5, records.getCount());
    }
    
}
