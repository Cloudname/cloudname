package org.cloudname.timber.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a reconnect delay manager.  The purpose of
 * this class is to implement exponential backoff per target address
 * when doing reconnects.
 *
 * The reconnect delay for an address will double on every call until
 * it reaches a defined maximum.  After that it will produce reconnect
 * delays that are constant in size.  If reconnect delay has not been
 * requested for a defined time period, the delay will reset to the
 * initial value.
 *
 * This class is thread safe.
 *
 * @author borud
 */
public class ReconnectDelayManager {
    /**
     * The default initial reconnect delay.  On the first reconnect
     * this is the number of milliseconds we will wait for.  This
     * default can be overridden in the constructor.
     */
    public static final int DEFAULT_RECONNECT_DELAY_INITIAL = 500;

    /**
     * The default maximum reconnect delay.  As the reconnect time
     * increases it will never go above this delay.  This default can
     * be overridden in the constructor.
     */
    public static final int DEFAULT_RECONNECT_MAX_DELAY = 30000;

    /**
     * The default time before the delay time is reset to the initial
     * value.
     */
    public static final int DEFAULT_RECONNECT_DELAY_RESET_TIME = 60000;

    // Settings
    private final int initialReconnectDelay;
    private final int maxReconnectDelay;
    private final int reconnectDelayReset;
    private final TimeProvider timeProvider;

    // State
    private final ConcurrentHashMap<InetAddress, ReconnectItem> addressMap
        = new ConcurrentHashMap<InetAddress, ReconnectItem>();

    // Interface to make time provider pluggable
    public static interface TimeProvider {
        public long currentTimeMillis();
    }

    // Default TimeProvider implementation
    public static TimeProvider DEFAULT_TIMEPROVIDER = new TimeProvider() {
            @Override public long currentTimeMillis() {
                return System.currentTimeMillis();
            }
        };

    /**
     * This class manages the reconnect information for a given
     * InetAddress.  Note that this class is not static -- it accesses
     * the reconnection settings from ReconnectDelayManager.
     *
     * This class is thread safe.
     *
     * @author borud
     */
    private class ReconnectItem {
        private long lastReconnectTime = 0;
        private int lastReconnectDelay = initialReconnectDelay;

        /**
         * Get the reconnection delay.  Whenever you call this method
         * it updates the internal state of the ReconnectItem.
         */
        public synchronized int getReconnectDelay() {
            long now = timeProvider.currentTimeMillis();
            long diff = now - lastReconnectTime;

            // If the last reconnect was longer than reconnectDelayReset
            // milliseconds ago, we reset the reconnect delay to
            // initialReconnectDelay
            if (diff >= reconnectDelayReset) {
                lastReconnectTime = now;
                lastReconnectDelay = initialReconnectDelay;
                return lastReconnectDelay;
            }

            // Double the last delay.
            lastReconnectDelay *= 2;

            // Cap delay to maxReconnectDelay
            if (lastReconnectDelay > maxReconnectDelay) {
                lastReconnectDelay = maxReconnectDelay;
            }

            lastReconnectTime = now;
            return lastReconnectDelay;
        }
    }

    /**
     * Create a ReconnectDelayManager with default settings.
     */
    public ReconnectDelayManager() {
        this(DEFAULT_RECONNECT_DELAY_INITIAL,
             DEFAULT_RECONNECT_MAX_DELAY,
             DEFAULT_RECONNECT_DELAY_RESET_TIME,
             DEFAULT_TIMEPROVIDER);
    }

    /**
     * Create a ReconnectDelayManager.
     *
     * @param initialReconnectDelay initial delay in milliseconds.
     * @param maxReconnectDelay maximum delay in milliseconds.
     * @param reconnectDelayReset number of milliseconds before the
     *   delay is reset to {@code initialReconnectDelay}
     * @param timeProvider the time provider
     */
    public ReconnectDelayManager(final int initialReconnectDelay,
                                 final int maxReconnectDelay,
                                 final int reconnectDelayReset,
                                 final TimeProvider timeProvider)
    {
        if (initialReconnectDelay < 0) {
            throw new IllegalArgumentException("Initial Reconnect Delay cannot be negative");
        }

        if (maxReconnectDelay < initialReconnectDelay) {
            throw new IllegalArgumentException("Max reconnect Delay cannot be smaller than Initial Reconnect Delay");
        }

        if (reconnectDelayReset < maxReconnectDelay) {
            throw new IllegalArgumentException("Reconnect Delay Reset cannot be smaller than Max Reconnect Delay");
        }

        this.initialReconnectDelay = initialReconnectDelay;
        this.maxReconnectDelay = maxReconnectDelay;
        this.reconnectDelayReset = reconnectDelayReset;
        this.timeProvider = timeProvider;
    }

    /**
     * Get the reconnect delay for a given SocketAddress.  Note that
     * this method disregards the port number so that the reconnect
     * delay is only for the InetAddress portion of the SocketAddress.
     *
     * @param socketAddress the SocketAddress for which we want the
     *   reconnect timeout.
     * @return the time to delay reconnect in milliseconds.
     * @see #getReconnectDelayForAddress
     */
    public int getReconnectDelayForSocketAddress(InetSocketAddress socketAddress) {
        return getReconnectDelayForAddress(socketAddress.getAddress());
    }

    /**
     * Get the reconnect delay for a given InetAddress.
     *
     * @param address the InetAddress for which we want the reconnect
     *   timeout.
     * @return the time to delay reconnect in milliseconds.
     */
    public int getReconnectDelayForAddress(InetAddress address) {
        ReconnectItem item = addressMap.get(address);
        if (null == item) {
            item = new ReconnectItem();
            addressMap.putIfAbsent(address, item);
        }

        assert(null != item);

        return item.getReconnectDelay();
    }
}

