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
         * This is the last event. You need to reconnect.
         */
        LOST_CONNECTION_TO_STORAGE,

        /**
         * This is the last event. You need to claim again.
         */
        COORDINATE_VANISHED,

        /**
         * Problems with parsing the data in ZooKeeper.
         */
        COORDINATE_CORRUPTED,

        /**
         * The data in the storage and memory is out of sync, system is corrupted.
         */
        COORDINATE_OUT_OF_SYNC,

        /**
         * No longer the owner of the coordinate.
         */
        NOT_OWNER
    }
    public void onConfigEvent(Event event, String message);
}
