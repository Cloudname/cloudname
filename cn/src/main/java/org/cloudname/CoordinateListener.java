package org.cloudname;

/**
 * Interface for listening to status on a claimed coordinate.
 * @author dybdahl
 */
public interface CoordinateListener {
    /**
     * Events that can be triggered when monitoring a coordinate.
     */
    public enum Event {

        /**
         * Everything is fine.
         */
        COORDINATE_OK,

        /**
         * Connection lost to storage, no more events will occur.
         */
        NO_CONNECTION_TO_STORAGE,

        /**
         * Problems with parsing the data in ZooKeeper for this coordinate.
         */
        COORDINATE_CORRUPTED,

        /**
         * The data in the storage and memory is out of sync.
         */
        COORDINATE_OUT_OF_SYNC,

        /**
         * No longer the owner of the coordinate.
         */
        NOT_OWNER,
    }



    /**
     * Implement this function to receive the events.
     * Return false if no more events are wanted, will stop eventually.
     * @param event the event that happened.
     * @param message some message associated with the event.
     */
    public boolean onCoordinateEvent(Event event, String message);
}
