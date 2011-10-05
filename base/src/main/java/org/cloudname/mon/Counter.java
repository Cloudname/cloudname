package org.cloudname.mon;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter.  This class is intended to be used in much the same
 * manner as you would use Loggers.  Counters are named with dot
 * separated components like package names.  In fact you may use
 * package and class names if you like or you may want to use a naming
 * scheme that is more to your pleasing.  The main thing is that you
 * try to be consistent.
 *
 * typical way to use Counter in code would be:
 *
 * <pre>
 *    private static final Counter fooCount = Counter.getCounter("myapp.foo.count");
 * </pre>
 *
 * Concurrency is dealt with by the MonitorManager and the fact that
 * we use an AtomicLong to keep track of the counts.
 *
 * @author borud
 */
public class Counter {
    private final AtomicLong count = new AtomicLong();

    /**
     * Counters should be instantiated by users using the getCounter()
     * factory method and not the constructor.
     */
    private Counter() {}

    /**
     * The factory method used for instantiating and/or fetching a
     * named counter.
     *
     * @param name the name of the counter we wish to create or look
     *   up.
     * @return the counter with name {@code name}
     */
    public static synchronized Counter getCounter(String name) {
        MonitorManager manager = MonitorManager.getInstance();
        Counter c = manager.getCounter(name);
        if (null == c) {
            c = new Counter();
            manager.addCounter(name, c);
        }
        return c;
    }

    /**
     * Increment counter by 1 and return its value.
     *
     * @return the value of the counter after increment.
     */
    public long inc() {
        return count.incrementAndGet();
    }

    /**
     * Increment counter by some value {@code delta} and return its value.
     *
     * @param delta the value we wish to increment the counter by.
     * @return the value after being incremented by {@code delta}
     */
    public long inc(long delta) {
        return count.addAndGet(delta);
    }


    /**
     * @return the value of the counter
     */
    public long getCount() {
        return count.get();
    }
    
    /**
     * resets the counter to 0
     */
    public void reset() {
        count.getAndSet(0);
    }
}