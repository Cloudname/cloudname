package org.cloudname;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of CoordinateListener. It will terminate the process on errors.
 * @author dybdahl
 */
public class DieHardOnErrorCoordinateListener implements CoordinateListener {

    private static final Logger log = Logger.getLogger(DieHardOnErrorCoordinateListener.class.getName());

    @Override
    public void onConfigEvent(Event event, String message) {
        switch (event) {
            case COORDINATE_OUT_OF_SYNC:
                log.log(Level.SEVERE, "Someone else might have claimed my coordinate, out of sync.  " +
                        "Taking down the process with System.exit(). " + message);
                System.exit(0);
                break;
            case LOST_CONNECTION_TO_STORAGE:
                log.log(Level.SEVERE, "Lost connection to storage. Pretending everything is fine." + message);
                System.exit(0);
                break;
            case COORDINATE_OK:
                log.log(Level.SEVERE, "Connection to storage is good, confirmed ownership." + message);
                break;
            case COORDINATE_VANISHED:
                log.log(Level.SEVERE, "Coordinate does not exist anymore. Storage blackout? Calling system.exit()"
                        + message);
                System.exit(0);
                break;
            case COORDINATE_CORRUPTED:
                log.log(Level.SEVERE, "Problems parsing data from storage. Calling system.exit()" + message);
                System.exit(0);
                break;
            case NOT_OWNER:
                log.log(Level.SEVERE, "Storage reports different ownership. Giving up. " + message);
                System.exit(0);
                break;
        }
    }
}
