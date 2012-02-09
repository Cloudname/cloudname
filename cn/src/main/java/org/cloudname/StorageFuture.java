package org.cloudname;


public interface StorageFuture {
    boolean waitForCompletionMillis(int milliSeconds);

    public interface Callback {
        void success();
        void failure(String message);
    }

    public void registerCallback(Callback callback);

    public boolean isDone();
}
