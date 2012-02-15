package org.cloudname;


/**
 * Instances of StorageFuture are returned on various asynchronous operations like modify endpoints etc.
 * This object makes it possible to wait for the operation to finish with a timeout, or register a listener
 * that is called when it is done. If the event happens before you start waiting or register a listener,
 * the event is sent, e.g. you are guaranteed to be notified.
 *
 * @auther dybdahl
 */
public interface StorageFuture {
    /**
     * Waits for the operation to finish up to some time limit.
     * @return true if operation was finished withing timeout period, false if it timeout or failed in any other way.
     */
    boolean waitForCompletionMillis(int milliSeconds);

    /**
     * The interface for receiving a callback when the operation is finished, used by registerListener().
     */
    public interface Listener {
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
     * Registers a new listener. It is allowed to register multiple futures.
     */
    public void registerListener(Listener listener);

    /**
     * For checking if operation was finished.
     */
    public boolean isDone();
}
