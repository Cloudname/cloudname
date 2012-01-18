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
        LOST_CONNECTION_TO_STORAGE,

        /**
         * Coordinate ownership is list, nobody owns it. No more events will occur.
         */
        COORDINATE_VANISHED,

        /**
         * Problems with parsing the data in ZooKeeper for this coordinate.
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

    /**
     * The action that can be taken in case of events. Panic auto recovery is more desperate than normal auto recovery.
     * E.g. in ZooKeeper implementation it will try to create the coordinate if it does not exist.
     */
    public enum Action {
        /**
         * Just continue.
         */
        DO_NOTHING,
        /**
         * System.exit is called.
         */
        DIE_NOW,
        /**
         * Try auto recovery, if it fails, die.
         */
        DIE_IF_NORMAL_AUTO_RECOVERY_FAILS,
        /**
         * Try panic auto recovery, if it fails, die.
         */
        DIE_IF_PANIC_AUTO_RECOVERY_FAILS,
    }

    /**
     * Implement this function to receive the events and trigger action.
     * @param event the event that happened.
     * @param message some message associated with the event.
     */
    public Action onConfigEvent(Event event, String message);
}
