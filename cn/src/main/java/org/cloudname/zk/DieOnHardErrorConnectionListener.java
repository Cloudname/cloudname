package org.cloudname.zk;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of ZkConnectionListener.
 * It will KILL the process if someone else claims the coordinate. This should of course never happen.
 * It will try to recreate the coordinate if ZooKeeper experience a total black out. This should also never happen.
 * If connection to ZooKeeper goes down it will ignore it.
 * @author dybdahl
 */
public class DieOnHardErrorConnectionListener implements ZkConnectionListener {

    private static final Logger log = Logger.getLogger(DieOnHardErrorConnectionListener.class.getName());

    @Override
    public void someoneElseClaimedTheCoordinate() {
        log.log(Level.SEVERE, "Someone else claimed my coordinate probably while connection to ZooKeeper was down. " +
                "Giving up and taking down the process with System.exit().");
        System.exit(0);
    }

    @Override
    public void lostConnectionToZooKeeper() {
        log.log(Level.SEVERE, "Lost connection to ZooKeeper. Pretending everything is fine.");
    }

    @Override
    public void connectionOk() {
        log.log(Level.SEVERE, "Connection to ZooKeeper is good, confirmed ownership, puuh. Life back to normal.");
    }

    @Override
    public boolean recreateCoordinateAfterTotalBlackout() {
        log.log(Level.SEVERE, "ZooKeeper blackout. This is very serious, " +
                "we try to recreate and reclaim the coordinate as a last defence. We need to clean up. " +
                "Config is lost if process is restarted. This is not good.");
        return true;
    }
}
