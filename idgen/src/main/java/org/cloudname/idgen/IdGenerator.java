package org.cloudname.idgen;

import java.util.logging.Logger;

/**
 * Simple ID generator that will produce unique IDs given that no ID
 * generator instances with the same worker ID exist at the same time.
 *
 * This ID generator uses a fixed number of bits for timestamp (40
 * bits), worker ID (12 bits) and sequence (12 bits).  These numbers
 * translate to the following limitations:
 *
 * <ul>
 *   <li> the timer will wrap every 34.87 years, meaning that after
 *        that after 2045 the IdGenerator will produce IDs that are
 *        no longer unique.
 *
 *   <li> there can at most be 4096 workers active at any given time.
 *
 *   <li> if more than 4096 IDs are generated per millisecond the time
 *        component of the ID will be pushed up by 1ms for every 4096
 *        IDs generated.  In extreme cases this might be a problem.
 * </ul>
 *
 * TODO(borud): In order to ensure that no two IdGenerator instances
 *   share the same worker-id we need to add a factory/manager for the
 *   IdGenerator which takes care of allocating the worker-id.
 *
 * @author borud
 */
public class IdGenerator {
    private static final Logger log = Logger.getLogger(IdGenerator.class.getName());

    // Create a singleton default time provider.
    private static final TimeProvider defaultTimeProvider = new TimeProvider() {
            @Override
            public long getTimeInMillis() {
                return System.currentTimeMillis();
            }
        };

    // Set how many bits we use for each field.
    private static final int NUM_BITS_TIMESTAMP = 40;
    private static final int NUM_BITS_WORKER_ID = 12;
    private static final int NUM_BITS_SEQUENCE  = 12;

    // Calculate the bitmasks
    private static final long timestampBitMask = makeLongBitMask(NUM_BITS_TIMESTAMP);
    private static final long workerIdBitMask  = makeLongBitMask(NUM_BITS_WORKER_ID);
    private static final long sequenceBitMask  = makeLongBitMask(NUM_BITS_SEQUENCE);

    // Calculate shift lengths
    private static final int timestampLeftShiftBy = NUM_BITS_WORKER_ID + NUM_BITS_SEQUENCE;
    private static final int workerLeftShiftBy    = NUM_BITS_SEQUENCE;

    // Set the default time provider -- ie. the system clock.
    private TimeProvider timeProvider;

    // State variables
    private long workerId = 0L;
    private long prevTimestamp = Long.MIN_VALUE;
    private long sequence = 0L;

    // Sync object
    private Object syncObject = new Object();

    /**
     * Create new IdGenerator.  To ensure that this ID generator
     * generates unique IDs there has to be some guarantee that the
     * worker ID can only be used by one ID-generator at any given
     * time.
     *
     * @param workerId the worker id of the id generator.
     */
    public IdGenerator(long workerId) {
        this(workerId, defaultTimeProvider);
    }

    /**
     * Create new IdGenerator.  To ensure that this ID generator
     * generates unique IDs there has to be some guarantee that the
     * worker ID can only be used by one ID-generator at any given
     * time.
     *
     * @param workerId the worker id of the id generator.
     * @param timeProvider override default time provider.
     */
    public IdGenerator(long workerId, TimeProvider timeProvider) {
        this.workerId = workerId;
        this.timeProvider = timeProvider;
    }


    /**
     * Generate next unique ID.
     *
     * @throws IllegalStateException if the clock has gone backwards
     *   by more than {@code maxWaitForClockCatchupInMilliseconds}
     * @return the next unique ID as a long value.
     */
    public long getNextId() {
        long timestamp = timeProvider.getTimeInMillis();

        synchronized(syncObject) {
            // Deal with the simple case first.
            if (prevTimestamp < timestamp) {
                sequence = 0L;
                prevTimestamp = timestamp;
                return buildKey(timestamp, workerId, sequence);
            }

            // TRICK: If the clock has gone backwards we can still use
            // the sequence counter to generate unique IDs, so we
            // reset the timestamp to prevTimestamp and try our luck
            // with the sequence counter
            timestamp = prevTimestamp;

            // Invariant: we have handed out an ID for this timestamp

            // Increment and wrap
            sequence = ((sequence + 1) & sequenceBitMask);
            if (0L == sequence) {
                // The sequence has wrapped so we cheat and advance
                // the timestamp by 1ms
                timestamp++;
                log.info("Cheating, advancing timestamp by 1ms. workerId = " + workerId);
                prevTimestamp = timestamp;
            }

            return buildKey(timestamp, workerId, sequence);
        }
    }

    public String getNextIdHex() {
        return Long.toString(getNextId(), 16);
    }

    /**
     * Construct a long key from the values. The values are masked and
     * OR'ed together into a long value according to the bit layout of
     * the generator.
     *
     * @param timestamp Timestamp to use
     * @param workerId Worker ID to use
     * @param sequence Sequence counter
     */
    private long buildKey(long timestamp, long workerId, long sequence) {
        return
            ((timestamp & timestampBitMask) << (long) timestampLeftShiftBy)
            | ((workerId & workerIdBitMask) << (long) workerLeftShiftBy)
            | (sequence & sequenceBitMask);
    }

    /**
     * Make a bit mask with the specified number of bits set.
     * The rightmost (LSB) bits are set; {@code makeLongBitMask(1)}
     * returns {@code 1L}
     *
     * @param bitsToSet
     */
    private static long makeLongBitMask(int bitsToSet) {
        return (long) (0xFFFFFFFFFFFFFFFFL >>> (64 - bitsToSet));
    }
}
