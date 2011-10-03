package org.cloudname.mon;

import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for the MonitorManager class.  Since the class we are
 * testing is a singleton and we have not (yet) fouled the API with an
 * explicit factory method for creating MonitorManager instances there
 * are certain limits to what we bother testing here.
 *
 * @author borud
 */
public class MonitorManagerTest {

    @Test
    public void tesGetInstance() {
        assertNotNull(MonitorManager.getInstance());

        // Should return a List regardless, but since this is a
        // singleton and tests run in parallell the list may or may
        // not contain anything.
        List<String> countNames = MonitorManager.getCounterNames();
        assertNotNull(countNames);
        List<String> varNames = MonitorManager.getVariableNames();
        assertNotNull(varNames);
    }

    /**
     * Try to add a counter with the same name twice.  Should result
     * in exception.
     */
    @Test (expected = IllegalStateException.class)
    public void testDuplicateAddCounter() {
        Counter c = Counter.getCounter("this.should.collide");
        MonitorManager.getInstance().addCounter(c.getName(), c);
    }
    
    /**
     * Try to add a variable with the same name twice.  Should result
     * in exception.
     */
    @Test (expected = IllegalStateException.class)
    public void testDuplicateAddVariable() {
        Variable v = Variable.getVariable("this.should.collide");
        MonitorManager.getInstance().addVariable(v.getName(), v);
    }

    /**
     * Make sure that we can find counters through the
     * getCounterNames() method.  Somewhat limited what we can test
     * here.
     */
    @Test
    public void testGetCounterNames() {
        String name = "the.counter.we.use.for.testGetCounterNames";

        // Make sure we have one known name in the counter list
        Counter c = Counter.getCounter(name);
        assertNotNull(c);

        boolean found = false;
        for (String n : MonitorManager.getCounterNames()) {
            if (name.equals(n)) {
                found = true;
            }
        }

        assertTrue(found);
    }
    
    /**
     * Make sure that we can find variables through the
     * getVariableNames() method.  Somewhat limited what we can test
     * here.
     */
    @Test
    public void testGetVariableNames() {
        String name = "the.variable.we.use.for.testGetVariableNames";

        // Make sure we have one known name in the counter list
        Variable v = Variable.getVariable(name);
        assertNotNull(v);

        boolean found = false;
        for (String n : MonitorManager.getVariableNames()) {
            if (name.equals(n)) {
                found = true;
            }
        }

        assertTrue(found);
    }
}