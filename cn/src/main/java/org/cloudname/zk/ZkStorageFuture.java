package org.cloudname.zk;

import org.cloudname.StorageFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper implementation of StorageFuture.
 */
public class ZkStorageFuture implements StorageFuture {
    /**
     * isDone is true when the storage operation is finished.
     */
    private boolean isDone = false;

    /**
     * The client can register several listeners to the same operation.
     */
    List<Listener> listeners = Collections.synchronizedList(new ArrayList());

    /**
     * In case the operation did not work out well, keep a message to send to the client.
     * It might look at bit funny, but it is final. This means that in this implementation we already
     * know at creation time if the operation went well; except for timeouts. This makes sense because we retry
     * the operation until it times out or finishes. However, there are errors that might occur early which we then
     * report early.
     */
    final String errorMessage;
    
    public ZkStorageFuture() {
        errorMessage = null;
    }

    public ZkStorageFuture(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    @Override
    public boolean waitForCompletionMillis(int milliSeconds) {
        synchronized (this) {
            if (isDone()) {
                return true;
            }
            if (errorMessage != null) {
                return false;
            }
        }
        final CountDownLatch latch = new CountDownLatch(1);
        registerListener(new Listener() {
            @Override
            public void success() {
                latch.countDown();
            }

            @Override
            public void failure(String message) {
                // not used
            }

        });
        try {
            return latch.await(milliSeconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public void registerListener(Listener listener) {
        if (errorMessage !=  null) {
            listener.failure(errorMessage);
            return;
        }

        boolean runCallback;
        synchronized (this) {
            runCallback = isDone;
            if (!runCallback) {
                listeners.add(listener);
            }
        }
        if (runCallback)  {
            listener.success();
        }
    }

    @Override
    public boolean isDone() {
        synchronized (this) {
            return isDone;
        }
    }

    public Listener getSystemCallback() {

        return new Listener() {
            @Override
            public void success() {
                synchronized (this) {
                    if (isDone) {
                        return;
                    }
                    isDone = true;
                }
                for (Listener listener : listeners) {
                    listener.success();
                }
            }

            @Override
            public void failure(String message) {
                // not used
            }
        };
    }
}
