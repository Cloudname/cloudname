package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;


/**
 * This class contains the unit tests for the ZkResolver class.
 *
 * TODO(borud): add tests for when the input is a coordinate.
 *
 * @author borud
 */
public class ZkResolverTest {
    private Resolver resolver;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Set up an embedded ZooKeeper instance backed by a temporary
     * directory.  The setup procedure also allocates a port that is
     * free for the ZooKeeper server so that you should be able to run
     * multiple instances of this test.
     */
    @Before
    public void setup() throws Exception {
        resolver = new ZkResolver.Builder()
                .addStrategy(new StrategyAll())
                .addStrategy(new StrategyAny())
                .build();
    }

    // Valid endpoints.
    public static final String[] validEndpointPatterns = new String[] {
            "http.1.service.user.cell",
            "foo-bar.3245.service.user.cell",
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

    @Test(expected=IllegalArgumentException.class)
    public void testRegisterSameListenerTwice() throws Exception {
        Resolver.ResolverListener resolverListener = new Resolver.ResolverListener() {
            @Override
            public void endpointEvent(Event event, Endpoint endpoint) {

            }
        };
        resolver.addResolverListener("foo.all.service.user.cell", resolverListener);
        resolver.addResolverListener("bar.all.service.user.cell", resolverListener);
    }

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