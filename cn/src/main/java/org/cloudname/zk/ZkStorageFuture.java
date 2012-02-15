package org.cloudname.zk;

import org.cloudname.StorageFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ZkStorageFuture implements StorageFuture {
    private boolean isDone = false;
    List<Listener> listeners = Collections.synchronizedList(new ArrayList());
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
