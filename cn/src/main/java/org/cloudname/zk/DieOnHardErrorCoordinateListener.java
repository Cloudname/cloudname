package org.cloudname.zk;

import org.cloudname.CoordinateListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of CoordinateListener.
 * It will KILL the process if someone else claims the coordinate. This should of course never happen.
 * It will try to recreate the coordinate if ZooKeeper experience a total black out. This should also never happen.
 * If connection to ZooKeeper goes down it will ignore it.
 * @author dybdahl
 */
public class DieOnHardErrorCoordinateListener implements CoordinateListener {

    private static final Logger log = Logger.getLogger(DieOnHardErrorCoordinateListener.class.getName());

    @Override
    public void onConfigEvent(Event event) {
        switch (event) {
            case LOST_OWNERSHIP:
                log.log(Level.SEVERE, "Someone else claimed my coordinate probably while connection to ZooKeeper " +
                        " was down. Taking down the process with System.exit().");
                System.exit(0);
                break;
            case LOST_CONNECTION_TO_STORAGE:
                log.log(Level.SEVERE, "Lost connection to ZooKeeper. Pretending everything is fine.");
                break;
            case COORDINATE_CONFIRMED:
                log.log(Level.SEVERE, "Connection to ZooKeeper is good, confirmed ownership.");
                break;
            case COORDINATE_VANISHED:
                log.log(Level.SEVERE, "Coordinate does not exist anymore. ZooKeeper blackout? Calling system.exit()");
                System.exit(0);
                break;
            case COORDINATE_CORRUPTED:
                log.log(Level.SEVERE, "Problems parsing data from ZooKeeper. Calling system.exit()");
                System.exit(0);
                break;
        }
    }
}
