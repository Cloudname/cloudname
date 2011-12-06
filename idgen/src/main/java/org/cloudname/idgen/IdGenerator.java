package org.cloudname.idgen;

import java.util.logging.Logger;

/**
 * Simple ID generator that will produce unique IDs given that no ID
 * generator instances with the same worker ID exist at the same time.
 *
 * This ID generator uses a fixed number of bits for timestamp (40
 * bits), worker ID (12 bits) and sequence (12 bits).
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

    /**
     * If the clock goes backwards, and it will from time to time, how
     * long should we wait before we give up and throw an exception?
     * In Amazon EC2 we have experienced clocks that jump back by as
     * much as 700ms.  There is really no correct answer to this, so
     * have picked a value that appears reasonable to us.
     */
    public static final int maxWaitForClockCatchupInMilliseconds = 2000;

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
    private long lastTimestamp = Long.MIN_VALUE;
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
            if (lastTimestamp < timestamp) {
                sequence = 0L;
                lastTimestamp = timestamp;
                return buildKey(timestamp, workerId, sequence);
            }

            // TRICK: If the clock has gone backwards we can still use
            // the sequence counter to generate unique IDs, so we
            // reset the timestamp to lastTimestamp and try our luck
            // with the sequence counter
            timestamp = lastTimestamp;

            // Invariant: we have handed out an ID for this timestamp

            // Increment and wrap
            sequence = ((sequence + 1) & sequenceBitMask);
            if (0L == sequence) {
                log.info("Requesting unique IDs faster than we can make them; pausing. workerId = " + workerId);
                // busy-wait until clock has progressed by one millisecond
                while (lastTimestamp >= timestamp) {
                    timestamp = timeProvider.getTimeInMillis();

                    // If the clock skew is unacceptably bad it is
                    // better to give up and throw an exception.
                    if ((lastTimestamp - timestamp) > maxWaitForClockCatchupInMilliseconds) {
                        throw new IllegalStateException("Clock too far behind, not bothering to catch up "
                                                        + (lastTimestamp - timestamp)
                                                        + "ms");
                    }
                }
                lastTimestamp = timestamp;
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
