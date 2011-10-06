package org.cloudname.mon;

/**
 * A simple holder class for data from AverageLong
 * 
 * @author espen
 *
 */
public class AverageLongData {
    private long aggregated;
    private long count;

    /**
     * Set the aggregated value and the number of times it has been aggregated.
     * @param aggregated
     * @param count
     */
    public AverageLongData(long aggregated, long count) {
      this.aggregated = aggregated;
      this.count = count;
    }

    /**
     * 
     * @return long - the aggregated value
     */
    public long getAggregated() { return aggregated; }
    
    /**
     * 
     * @return long - the number of times aggregated
     */
    public long getCount() { return count; }

    /**
     * Record a new value
     * @param value
     */
    public void record(long value) {
        aggregated += value;
        count++;
    }

}
