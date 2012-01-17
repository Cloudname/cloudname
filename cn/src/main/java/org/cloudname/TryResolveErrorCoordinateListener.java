package org.cloudname;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Trying to fix any issue except from different ownership.
 */
public class TryResolveErrorCoordinateListener implements CoordinateListener{
    private static final Logger log = Logger.getLogger(DieHardOnErrorCoordinateListener.class.getName());

    @Override
    public Action onConfigEvent(Event event, String message) {
        switch (event) {
            case COORDINATE_OUT_OF_SYNC:
                log.log(Level.SEVERE, "Someone else might have claimed my coordinate, out of sync.  " +
                        "Trying to fix it. " + message);
                return Action.DIE_IF_PANIC_AUTO_RECOVERY_FAILS;
            case LOST_CONNECTION_TO_STORAGE:
                log.log(Level.SEVERE, "Lost connection to storage. Trying to reconnect." + message);
                return Action.DIE_IF_PANIC_AUTO_RECOVERY_FAILS;
            case COORDINATE_OK:
                return Action.DO_NOTHING;
            case COORDINATE_VANISHED:
                log.log(Level.SEVERE, "Coordinate does not exist anymore. Storage blackout? Trying to fix it."
                        + message);
                return Action.DIE_IF_PANIC_AUTO_RECOVERY_FAILS;
            case COORDINATE_CORRUPTED:
                log.log(Level.SEVERE, "Problems parsing data from storage. Trying to fix it. " + message);
                return Action.DIE_IF_PANIC_AUTO_RECOVERY_FAILS;
            case NOT_OWNER:
                log.log(Level.SEVERE, "Storage reports different ownership. Giving up. " + message);
                return Action.DIE_NOW;
            default:
                log.log(Level.SEVERE, "Unknown event. Trying to fix it. " + message);
                return Action.DIE_IF_PANIC_AUTO_RECOVERY_FAILS;
        }
    }
}
