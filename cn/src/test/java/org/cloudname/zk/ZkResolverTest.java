package org.cloudname.zk;

import java.util.regex.Matcher;

import org.junit.*;
import static org.junit.Assert.*;


/**
 * This class contains the unit tests for the ZkResolver class.
 *
 * TODO(borud): add tests for when the input is a coordinate.
 *
 * @author borud
 */
public class ZkResolverTest {

    // Valid endpoints.
    public static final String[] validEndpointPatterns = new String[] {
        "http.1.service.user.cell",
        "foo-bar.3245.service.user.cell",
        "foo_bar.3245.service.user.cell",
        "foo_bar.3245.service.user.cell",
    };

    // Valid strategy.
    public static final String[] validStrategyPatterns = new String[] {
        "any.service.user.cell",
        "all.service.user.cell",
        "somestrategy.service.user.cell",
    };

    // Valid endpoint strategy.
    public static final String[] validEndpointStrategyPatterns = new String[] {
        "http.any.service.user.cell",
        "thrift.all.service.user.cell",
        "some-endpoint.somestrategy.service.user.cell",
    };


    @Test
    public void testEndpointPatterns() throws Exception {
        // Test input that should match
        for (String s : validEndpointPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.endpointPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validEndpointStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }
    }

    @Test
    public void testStrategyPatterns() throws Exception {
        // Test input that should match
        for (String s : validStrategyPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.strategyPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validEndpointPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.strategyPattern.matcher(s).matches());
        }
        // Test input that should not match
        for (String s : validEndpointStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }
    }

    @Test
    public void testEndpointStrategyPatterns() throws Exception {
        // Test input that should match
        for (String s : validEndpointStrategyPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }


        // Test input that should not match
        for (String s : validEndpointPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }
    }
}