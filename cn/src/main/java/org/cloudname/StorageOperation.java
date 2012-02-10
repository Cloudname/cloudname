package org.cloudname;


/**
 * StorageOperation are returned on various asynchronous operations like modify endpoints etc.
 * This object makes it possible to wait for the operation to finish with a timeout, or register a listener
 * that is called when it is done.
 *
 * @auther dybdahl
 */
public interface StorageOperation {
    /**
     * Waits for the operation to finish up to some tine limit.
     * @return true if operation was finished withing timeout period, false if it timeout or failed in any other way.
     */
    boolean waitForCompletionMillis(int milliSeconds);

    /**
     * The interface for receiving a callback when the operation is finished, used by registerCallback().
     */
    public interface Future {
        /**
         * Operation finished successfully.
         */
        void success();

        /**
         * Operation failed for some reason.
         * @param message indicates what went wrong.
         */
        void failure(String message);
    }

    /**
     * Registers a new future. It is allowed to register multiple futures.
     */
    public void registerCallback(Future future);

    /**
     * For checking if operation was finished.
     */
    public boolean isDone();
}
