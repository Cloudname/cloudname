package org.cloudname.mon;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * The MonitorManager is a singleton which keeps track of all the
 * monitors that have been defined.
 *
 * @author borud
 */
public class MonitorManager {
    public static Logger log = Logger.getLogger(MonitorManager.class.getName());

    // The singleton instance
    private static MonitorManager manager;

    // Map of counters
    private Map<String, Counter> counters = new HashMap<String, Counter>();
    
    // Map of variables
    private Map<String, Variable> variables = new HashMap<String, Variable>();

    // Static initialization
    static {
        manager = new MonitorManager();
    }

    /**
     * @return the singleton instance of MonitorManager
     */
    public static MonitorManager getInstance() {
        return manager;
    }

    /**
     * You should not use this method directly. Use Counter.getCounter() instead.
     *
     * @param monitor the counter we wish to add to the
     *   MonitorManager.
     * @throws IllegalStateException if the name has already been
     *   registered.
     */
    public MonitorManager addCounter(String name, Counter counter) {
        // if we add a counter more than once the code using the
        // monitor is being inconsistent and we have to flag this.
        synchronized(counters) {
            if (counters.containsKey(name)) {
                throw new IllegalStateException("counter with name '" + name + "' already added");
            }
            counters.put(name, counter);
        }
        return this;
    }
    
    /**
     * You should not use this method directly. Use Variable.getVariable() instead.
     *
     * @param monitor the variable we wish to add to the
     *   MonitorManager.
     * @throws IllegalStateException if the name has already been
     *   registered.
     */
    public MonitorManager addVariable(String name, Variable variable) {
        // if we add a variable more than once the code using the
        // monitor is being inconsistent and we have to flag this.
        synchronized(variables) {
            if (variables.containsKey(name)) {
                throw new IllegalStateException("variable with name '" + name + "' already added");
            }
            variables.put(name, variable);
        }
        return this;
    }

    /**
     * @param name the name of the counter.
     * @return counter specified by {@code name}
     */
    public Counter getCounter(String name) {
        synchronized(counters) {
            return counters.get(name);
        }
    }
    
    /**
     * @param name the name of the variable.
     * @return variable specified by {@code name}
     */
    public Variable getVariable(String name) {
        synchronized(variables) {
            return variables.get(name);
        }
    }

    /**
     * @return a list of the counter names that have been defined.
     */
    public static List<String> getCounterNames() {
        return getInstance().getCounterNamesInternal();
    }

    /**
     * @return a list of the variable names that have been defined.
     */
    public static List<String> getVariableNames() {
        return getInstance().getVariableNamesInternal();
    }

    /**
     * @return a list of the counter names that have been defined.
     */
    private List<String> getCounterNamesInternal() {
        synchronized(counters) {
            return new ArrayList(counters.keySet());
        }
    }
    
    /**
     * @return a list of the variable names that have been defined.
     */
    private List<String> getVariableNamesInternal() {
        synchronized(variables) {
            return new ArrayList(variables.keySet());
        }
    }

}