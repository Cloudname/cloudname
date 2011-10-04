package org.cloudname.mon;

import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class for counting positive long values in a histogram.
 *
 * Requires no synchronization.
 *
 * @author borud, espen
 */
public class HistogramCounter {
    private String name;
    private TreeMap<Long, AtomicLong> histogram = null;

    /**
     * The factory method used for instantiating and/or fetching a
     * named histogram counter.
     *
     * @param name the name of the histogram counter we wish to create or look
     *   up.
     * @return the histogram counter with name {@code name}
     */
    public static synchronized HistogramCounter getHistogramCounter(String name) {
        MonitorManager manager = MonitorManager.getInstance();
        HistogramCounter c = manager.getHistogramCounter(name);
        if (null == c) {
            c = new HistogramCounter();
            c.name = name;
            manager.addHistogramCounter(name, c);
        }
        return c;
    }
    
    /**
     * Create a histogram given a list of ceiling values. This will
     * make a histogram that has N+1 elements where the last element
     * is used for anything that is larger than the last ceiling
     * value.
     * @param ceilings the ceiling values for the histogram.
     */
    public void setCeilings(Collection<Long> ceilings) {
        histogram = new TreeMap<Long, AtomicLong>();
        for (Long ceiling : ceilings) {
            histogram.put(ceiling, new AtomicLong(0L));
        }

        // Add element for Long.MAX_VALUE
        histogram.put(Long.MAX_VALUE, new AtomicLong(0L));
    }
    
    /**
     * HistogramCounters should be instantiated by users using the
     * getHistogramCounter() factory method and not the constructor.
     * Remember to use the setCeilings(...) method.
     */
    public HistogramCounter() {}

    /**
     * Count an observed value in the correct interval of the
     * histogram.  Quirk: will only observe positive long values. If
     * we get a negative long value we will silently disregard it.
     *
     * @param value a long value between 0 and Long.MAX_VALUE
     */
    public void count(long value) {
        if (value < 0) {
            return;
        }

        // First we need to find the correct interval in the
        // histogram.
        Map.Entry<Long, AtomicLong> ent = histogram.ceilingEntry(value);

        // Now we can increment the counter
        ent.getValue().incrementAndGet();
    }

    /**
     * Return an ordered list of Map.Entry instances with the ceiling
     * values as key and count as value.  Defensive copying and type
     * simplification.
     *
     * @return an ordered list of ceiling/count Map.Entry instances.
     */
    public List<Map.Entry<Long, Long>> getEntries() {
        List<Map.Entry<Long, Long>> entries = new ArrayList<Map.Entry<Long, Long>>(histogram.entrySet().size());

        for (Map.Entry<Long, AtomicLong> ent : histogram.entrySet()) {
            entries.add(new AbstractMap.SimpleEntry<Long, Long>(ent.getKey(), ent.getValue().get()));
        }

        return entries;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Long, AtomicLong> ent : histogram.entrySet()) {
            if (first) {
                first = false;
            } else {
                buff.append(", ");
            }

            buff.append(ent.getKey() + " : " + ent.getValue().get());
        }

        return buff.toString();
    }
    
    /**
     * @return name of the histogram counter.
     */
    public String getName() {
        return name;
    }
}