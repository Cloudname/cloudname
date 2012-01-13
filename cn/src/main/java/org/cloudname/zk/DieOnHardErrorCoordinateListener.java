package org.cloudname.zk;

import org.cloudname.CoordinateListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of CoordinateListener.
 * It will terminate the process on "bad" errors. If connection to ZooKeeper goes down it will do nothing.
 * @author dybdahl
 */
public class DieOnHardErrorCoordinateListener implements CoordinateListener {

    private static final Logger log = Logger.getLogger(DieOnHardErrorCoordinateListener.class.getName());

    @Override
    public void onConfigEvent(Event event, String message) {
        switch (event) {
            case COORDINATE_OUT_OF_SYNC:
                log.log(Level.SEVERE, "Someone else might have claimed my coordinate, out of sync.  " +
                        "Taking down the process with System.exit(). " + message);
                System.exit(0);
                break;
            case LOST_CONNECTION_TO_STORAGE:
                log.log(Level.SEVERE, "Lost connection to ZooKeeper. Pretending everything is fine." + message);
                break;
            case COORDINATE_OK:
                log.log(Level.SEVERE, "Connection to ZooKeeper is good, confirmed ownership." + message);
                break;
            case COORDINATE_VANISHED:
                log.log(Level.SEVERE, "Coordinate does not exist anymore. ZooKeeper blackout? Calling system.exit()"
                        + message);
                System.exit(0);
                break;
            case COORDINATE_CORRUPTED:
                log.log(Level.SEVERE, "Problems parsing data from ZooKeeper. Calling system.exit()" + message);
                System.exit(0);
                break;
            case NOT_OWNER:
                log.log(Level.SEVERE, "Session id does not match node in ZooKeeper. Giving up. " + message);
                System.exit(0);
                break;
        }
    }
}
