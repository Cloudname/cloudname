package org.cloudname.zk;

import org.cloudname.StorageFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 03.02.12
 * Time: 12:31
 * To change this template use File | Settings | File Templates.
 */
public class ZkStorageOperation implements StorageFuture {
    private boolean isDone = false;
    List<Callback> callbacks = Collections.synchronizedList(new ArrayList());
    final String errorMessage;
    
    public ZkStorageOperation() {
        errorMessage = null;
    }

    public ZkStorageOperation(String errorMessage) {
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
        registerCallback(new Callback() {
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
    public void registerCallback(Callback callback) {
        if (errorMessage !=  null) {
            callback.failure(errorMessage);
            return;
        }

        boolean runCallback;
        synchronized (this) {
            runCallback = isDone;
            if (!runCallback) {
                callbacks.add(callback);
            }
        }
        if (runCallback)  {
            callback.success();
        }
    }

    @Override
    public boolean isDone() {
        synchronized (this) {
            return isDone;
        }
    }

    public Callback getSystemCallback() {

        return new Callback() {
            @Override
            public void success() {
                synchronized (this) {
                    if (isDone) {
                        return;
                    }
                    isDone = true;
                }
                for (Callback callback : callbacks) {
                    callback.success();
                }
            }

            @Override
            public void failure(String message) {
                // not used
            }
        };
    }
}
