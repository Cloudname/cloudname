package org.cloudname.mon;

/**
 * A simple holder class for data from AverageLong
 * 
 * @author espen
 *
 */
public class AverageLongData {
    private final long aggergated;
    private final long count;

    /**
     * Set the aggregated value and the number of times it has been aggregated.
     * @param aggregated
     * @param count
     */
    public AverageLongData(long aggregated, long count) {
      this.aggergated = aggregated;
      this.count = count;
    }

    /**
     * 
     * @return long - the aggregated value
     */
    public long getAggregated() { return aggergated; }
    
    /**
     * 
     * @return long - the number of times aggregated
     */
    public long getCount() { return count; }

}
