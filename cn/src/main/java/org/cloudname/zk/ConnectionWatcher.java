package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 11.01.12
 * Time: 11:46
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionWatcher implements Watcher {

    private ZkConnectionListener listener;
    private ZkServiceHandle serviceHandle;

    public ConnectionWatcher(ZkConnectionListener listener, ZkServiceHandle serviceHandle) {
        this.listener = listener;
        this.serviceHandle = serviceHandle;
    }

    class Callbackrunner extends Thread {
        ZkConnectionListener listener;
        org.apache.zookeeper.WatchedEvent event;
        ZkServiceHandle serviceHandle;
        ConnectionWatcher watcher;

        public Callbackrunner(ZkConnectionListener listener, WatchedEvent event, ZkServiceHandle serviceHandle, ConnectionWatcher watcher) {
            this.event = event;
            this.listener = listener;
            this.serviceHandle = serviceHandle;
            this.watcher = watcher;
        }

        public void run() {
            if (event.getState() == Event.KeeperState.SyncConnected ||
                    event.getType() == Event.EventType.NodeDeleted ||
                    (event.getType() == Event.EventType.None &&
                            (event.getState() == Event.KeeperState.Disconnected
                                    || event.getState() == Event.KeeperState.Expired
                                    || event.getState() == Event.KeeperState.AuthFailed))) {
                // TODO(dybdahl, borud): Try to reconnect, if ok, ignore error.
                ZkStatusAndEndpoints.StateError error = serviceHandle.verifyStatusAndEndpoints();
                switch (error) {
                    case OK:
                        listener.connectionOk();
                        break;
                    case NOT_LOADING:
                    case CORRUPT_STATE:
                        listener.lostConnectionToZooKeeper();
                        serviceHandle.recoverStatusAndEndpoints();
                        break;
                    case WRONG_STATE:
                        if (listener.recreateCoordinateAfterTotalBlackout()) {

                        }
                        break;
                }
                serviceHandle.registerWatcher(watcher);
            }
        }
    }

    @Override public void process(WatchedEvent event) {
        // Run the callback in a new thread to avoid deadlocks. Who knows what thread is calling this method and if
        // it can be blocked or not.
        Callbackrunner runner = new Callbackrunner(listener, event, serviceHandle, this);
        runner.start();
    }
}
