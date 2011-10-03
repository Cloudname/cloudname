package org.cloudname.mon;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A variable.  This class is intended to be used as a guage. 
 * Variables are named with dot separated components like package names.
 * In fact you may use package and class names if you like or you may
 * want to use a naming scheme that is more to your pleasing.
 * The main thing is that you try to be consistent.
 *
 * typical way to use Variable in code would be:
 *
 * <pre>
 *    private static final Variable fooVar = Variable.getVariable("myapp.foo.var");
 * </pre>
 *
 * Concurrency is dealt with by the MonitorManager.
 *
 * @author borud
 */
public class Variable {
    private final AtomicLong var = new AtomicLong();
    private String name;

    /**
     * Variables should be instantiated by users using the getVariable()
     * factory method and not the constructor.
     */
    private Variable() {}

    /**
     * The factory method used for instantiating and/or fetching a
     * named variable.
     *
     * @param name the name of the variable we wish to create or look
     *   up.
     * @return the variable with name {@code name}
     */
    public static synchronized Variable getVariable(String name) {
        MonitorManager manager = MonitorManager.getInstance();
        Variable v = manager.getVariable(name);
        if (null == v) {
            v = new Variable();
            v.name = name;
            manager.addVariable(name, v);
        }
        return v;
    }

    /**
     * Set the variable to a fiven value.
     */
    public void set(long value) {
        var.set(value);
    }

    /**
     * @return the value of the counter
     */
    public long getValue() {
        return var.get();
    }

    /**
     * @return name of the counter.
     */
    public String getName() {
        return name;
    }
    
}