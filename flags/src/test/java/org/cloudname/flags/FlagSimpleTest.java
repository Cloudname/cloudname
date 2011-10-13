package org.cloudname.flags;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

/**
 * 
 * @author acidmoose
 *
 */
public class FlagSimpleTest {
    
    @Test
    public void testSimpleRun() {
        Flags flags = new Flags()
            .loadOpts(FlagSimpleTestOptions.class)
            .parse(new String[]{"--active", "true", "--times", "5", "--text", "testSimpleRun"});
        
        Assert.assertFalse("Help seems to have been called. It should not have been.", flags.helpCalled());
        
        List<String> results = new ArrayList<String>();
        if (FlagSimpleTestOptions.active) {
            for (int i = 0; i < FlagSimpleTestOptions.times; i++) {
                results.add(FlagSimpleTestOptions.text);
            }
        }
        
        Assert.assertEquals(5, results.size());
        for (String string : results) {
            Assert.assertEquals("testSimpleRun", string);
        }
    }
    
    //TODO: add more tests

}
