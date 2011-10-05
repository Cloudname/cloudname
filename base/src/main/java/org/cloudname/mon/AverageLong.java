package org.cloudname.mon;


/**
 * This class handles numeric values. It aggregates the data in a variable
 * called "AGGREGATED", and increments a counter called "COUNT".
 * 
 * May for instance be used to graph a load for a certain method by setting
 * the graph value to:
 * 
 * (previousAggregated - currentAggregated) / (previousCount - currentCount)
 * 
 * AverageLongs are named with dot separated components like package
 * names. In fact you may use package and class names if you like or you may
 * want to use a naming scheme that is more to your pleasing. The main
 * thing is that you try to be consistent.
 *
 * Concurrency is dealt with by the MonitorManager, and by the use of
 * semaphores.
 * 
 * typical way to use AverageLong in code would be:
 *
 * <pre>
 *   private static AverageLong fooVar = AverageLong.getAverageLong("foo.long");
 *   intVar.record(10L);
 *   intVar.record(new Long(20));
 *   intVar.record(new Integer(10));
 *   intVar.record(40);
 * </pre>
 *
 * @author espen
 */
public class AverageLong {
    /**
     * The data of the class. Containing an aggregated value and a count.
     */
    AverageLongData data = new AverageLongData(0L, 0L);

    /**
     * Variables should be instantiated by users using the getVariable()
     * factory method and not the constructor.
     */
    private AverageLong() {}

    /**
     * The factory method used for instantiating and/or fetching a
     * named variable.
     *
     * @param name the name of the variable we wish to create or look
     *   up.
     * @return the variable with name {@code name}
     */
    public static synchronized AverageLong getAverageLong(String name) {
        MonitorManager manager = MonitorManager.getInstance();
        AverageLong v = manager.getAverageLong(name);
        if (null == v) {
            v = new AverageLong();
            manager.addAverageLong(name, v);
        }
        return v;
    }

    /**
     * Record a number of type Long
     */
    public void record(long value) {
        synchronized (data) {
            data.record(value);
        }
    }

    /**
     * Record a number of type Integer
     */
    public void record(int value) {
        synchronized (data) {
            data.record(value);
        }
    }

    /**
     * @return AverageLongData containing the aggregated value and the count
     */
    public AverageLongData getRecords() {
        synchronized (data) {
            return data;
        }
    }

}