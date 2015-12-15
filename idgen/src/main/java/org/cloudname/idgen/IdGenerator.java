package org.cloudname.idgen;

import java.util.logging.Logger;

/**
 * Simple ID generator that will produce unique IDs given that no ID
 * generator instances with the same worker ID exist at the same time.
 *
 * <p>The default instance of the ID generator uses 40 bits for the
 * timestamp, 12 bits for the worker ID and 12 bits for the sequence.
 * These numbers translate to the following limitations:
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
 * <p>TODO(borud): In order to ensure that no two IdGenerator instances
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

    // The defaults
    private static final int DEFAULT_NUM_BITS_WORKER_ID = 12;
    private static final int DEFAULT_NUM_BITS_SEQUENCE  = 12;

    // The bit masks
    private final long timestampBitMask;
    private final long workerIdBitMask;
    private final long sequenceBitMask;

    // Shift lengths
    private final int timestampLeftShiftBy;
    private final int workerLeftShiftBy;

    // Set the default time provider -- ie. the system clock.
    private TimeProvider timeProvider;

    // State variables
    private long workerId = 0L;
    private long prevTimestamp = Long.MIN_VALUE;
    private long sequence = 0L;

    // Sync object
    private final Object syncObject = new Object();

    /**
     * Create new IdGenerator.  To ensure that this ID generator
     * generates unique IDs there has to be some guarantee that the
     * worker ID can only be used by one ID-generator at any given
     * time.
     *
     * @param workerId the worker id of the id generator.
     */
    public IdGenerator(final long workerId) {
        this(workerId, defaultTimeProvider, DEFAULT_NUM_BITS_WORKER_ID, DEFAULT_NUM_BITS_SEQUENCE);
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
    public IdGenerator(final long workerId, final TimeProvider timeProvider) {
        this(workerId, timeProvider, DEFAULT_NUM_BITS_WORKER_ID, DEFAULT_NUM_BITS_SEQUENCE);
    }

    /**
     * Create new IdGenerator with the specified bit layout.
     *
     * @param workerId The worker ID.
     * @param numBitsWorkerId The number of bits to use for the worker ID.
     * @param numBitsSequence The number of bits to use for the sequence.
     */
    public IdGenerator(final long workerId, final int numBitsWorkerId, final int numBitsSequence) {
        this(workerId, defaultTimeProvider, numBitsWorkerId, numBitsSequence);
    }
    /**
     * Create new IdGenerator with custom time provider and id layout.
     * The layout allows you to create id generators with different
     * properties. A short epoch will give IDs that may clash at
     * regular intervals but you'll be able to create a lot of IDs
     * in a short time. Similarly, a long epoch ensures there will
     * be no collisions for a long time but the total throughput
     * will be lower. If in doubt use the defaults.
     *
     * @param workerId the worker id of the id generator
     * @param timeProvider The time provider to use.
     * @param numBitsWorkerId The number of bits to use for the worker id.
     *                        (ie the max number of simultaneous workers)
     * @param numBitsSequence The number of bits to use for the sequence.
     *                        (ie the maximum throughput per id generator
     *                        per second)
     */
    public IdGenerator(final long workerId, final TimeProvider timeProvider,
                       final int numBitsWorkerId, final int numBitsSequence) {
        // Do some sanity checks on the layout
        if ((numBitsWorkerId + numBitsSequence) > 63) {
            throw new IllegalArgumentException("The number of bits for the "
                    + "id generator cannot exceed 64 bits");
        }
        final int numBitsTimestamp = Long.SIZE - numBitsWorkerId - numBitsSequence;

        if (Math.pow(2, numBitsWorkerId) < workerId) {
            throw new IllegalArgumentException("There isn't enough room to fit the worker ID "
            + "(" + workerId + ") into the allocated bits (" + numBitsWorkerId + ")");
        }
        this.timestampLeftShiftBy = numBitsWorkerId + numBitsSequence;
        this.workerLeftShiftBy = numBitsSequence;
        this.timestampBitMask = makeLongBitMask(numBitsTimestamp);
        this.workerIdBitMask = makeLongBitMask(numBitsWorkerId);
        this.sequenceBitMask = makeLongBitMask(numBitsSequence);
        this.workerId = workerId;
        this.timeProvider = timeProvider;
    }


    /**
     * Generate next unique ID.
     *
     * @return the next unique ID as a long value.
     * @throws IllegalStateException if the clock has gone backwards
     *     by more than {@code maxWaitForClockCatchupInMilliseconds}
     */
    public long getNextId() {

        synchronized (syncObject) {
            long timestamp = timeProvider.getTimeInMillis();

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

    /**
     * Get next id as a hex string.
     */
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
    private long buildKey(final long timestamp, final long workerId, final long sequence) {
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
     * @param bitsToSet the number of bits to set in the bit mask.
     */
    private static long makeLongBitMask(final int bitsToSet) {
        return (0xFFFFFFFFFFFFFFFFL >>> (64 - bitsToSet));
    }

    /**
     * Get the worker id.
     */
    public long getWorkerId() {
        return workerId;
    }
}
