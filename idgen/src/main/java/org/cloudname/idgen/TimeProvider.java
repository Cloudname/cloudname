package org.cloudname.idgen;

/**
 * Interface for providers of time.
 *
 * @author borud
 */
public interface TimeProvider {
    /**
     * Number of milliseconds since epoch.  Epoch is Jan 1 1970 UTC.
     *
     * @return number of milliseconds since epoch.
     */
    long getTimeInMillis();
}
