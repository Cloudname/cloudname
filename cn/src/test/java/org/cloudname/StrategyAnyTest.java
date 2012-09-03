package org.cloudname;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparisons.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for StrategyAny.
 * @author dybdahl
 */
public class StrategyAnyTest {
    private List<Endpoint> endpoints;

    /**
     * Adds a list endpoints with even instance number to the endpoints list.
     */
    @Before
    public void setup() {
        endpoints = new ArrayList<Endpoint>();
        // Only even instance numbers.
        for (int i = 0; i < 100; i+= 2) {
            endpoints.add(new Endpoint(Coordinate.parse(String.valueOf(i) + ".foo.bar.zot"),
                    "rest-api",
                    "somehost",
                    4711,
                    "http",
                    null));
        }
    }

    /**
     * Different clients should have different lists.
     */
    @Test
    public void testDifferentLists() {
        StrategyAny strategyAny = new StrategyAny();

        List<Endpoint> sortedResult = strategyAny.order(new ArrayList<Endpoint>(endpoints));

        // Try with up tp 150 clients, if they all have the same first element, something is wrong.
        // In each iteration there is 1/50 probability for this. For 150 runs, the probability for
        // false negative is 1,42724769 × 10^-255 (e.g. zero).
        for (int z = 0; z < 150; z++) {
            StrategyAny strategyAny2 = new StrategyAny();
            List<Endpoint> sortedResult2 = strategyAny2.order(new ArrayList<Endpoint>(endpoints));
            if (sortedResult.get(0).getCoordinate().getInstance() !=
                    sortedResult2.get(0).getCoordinate().getInstance()) {
                return;
            }
        }
        assertTrue(false);
    }

    /**
     * Test that insertion does only create a new first element now and then.
     */
    @Test
    public void testInsertions() {
        StrategyAny strategyAny = new StrategyAny();

        List<Endpoint> sortedResult = strategyAny.order(new ArrayList<Endpoint>(endpoints));
        int newFrontEndpoint = 0;
        for (int c = 1; c < 30; c +=2) {
            int headInstance = sortedResult.get(0).getCoordinate().getInstance();
            sortedResult.add(new Endpoint(Coordinate.parse(String.valueOf(c) + ".foo.bar.zot"),
                    "rest-api",
                    "somehost",
                    4711,
                    "http",
                    null));
            sortedResult = strategyAny.order(sortedResult);
            if (headInstance != sortedResult.get(0).getCoordinate().getInstance()) {
                ++newFrontEndpoint;
            }
        }
        // For each insertion it a probability of less than 1/50 that front element is changed. The probability
        // that more than 10 front elements are changed should be close to zero.
        assertThat(newFrontEndpoint, is(lessThan(10)));
    }
}
