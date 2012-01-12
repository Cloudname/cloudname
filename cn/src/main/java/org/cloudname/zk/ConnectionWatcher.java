package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.cloudname.CoordinateListener;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 11.01.12
 * Time: 11:46
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionWatcher implements Watcher {

    private ZkStatusAndEndpoints statusAndEndpoints;

    public ConnectionWatcher(ZkStatusAndEndpoints statusAndEndpoints) {
        this.statusAndEndpoints = statusAndEndpoints;
    }

    class CallbackRunner extends Thread {
        CoordinateListener listener;
        org.apache.zookeeper.WatchedEvent event;
        ZkStatusAndEndpoints statusAndEndpoints;
        ConnectionWatcher watcher;

        public CallbackRunner(WatchedEvent event, ZkStatusAndEndpoints statusAndEndpoints, ConnectionWatcher watcher) {
            this.event = event;
            this.statusAndEndpoints = statusAndEndpoints;
            this.watcher = watcher;
        }

        public void run() {
            if (listener == null) {
                return;
            }
            if (event.getState() == Event.KeeperState.SyncConnected ||
                    event.getType() == Event.EventType.NodeDeleted ||
                    (event.getType() == Event.EventType.None &&
                            (event.getState() == Event.KeeperState.Disconnected
                                    || event.getState() == Event.KeeperState.Expired
                                    || event.getState() == Event.KeeperState.AuthFailed))) {
                statusAndEndpoints.sendCoordinateEvents(statusAndEndpoints.verifyState());
                statusAndEndpoints.registerWatcher(watcher);
            }
        }
    }

    @Override public void process(WatchedEvent event) {
        // Run the callback in a new thread to avoid deadlocks. Who knows what thread is calling this method and if
        // it can be blocked or not.
        CallbackRunner runner = new CallbackRunner(event, statusAndEndpoints, this);
        runner.start();
    }
}
