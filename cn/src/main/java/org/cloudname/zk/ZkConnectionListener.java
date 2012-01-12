package org.cloudname.zk;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 11.01.12
 * Time: 08:47
 * To change this template use File | Settings | File Templates.
 */
public interface ZkConnectionListener {
    /**
     * This event will happen if for example this server lost connection to ZooKeeper and someone else
     * snatched the coordinate.
     * Suggested behaviour, close all ports, flush all buffers, and exit.
     */
    public void someoneElseClaimedTheCoordinate();

    /**
     * Connection is lost, this might be temporarily.
     * Suggested behaviour, do nothing, pretend everything is ok.
     */
    public void lostConnectionToZooKeeper();

    /**
     * After problems, the connection is ok, confirmed that we own the coordinate.
     * Suggested behaviour, do nothing.
     */
    public void connectionOk();

    /**
     * If ZooKeeper faces a total black-out (of course this should never happen, that is why it happens),
     * what should happen. A total black-out means that all content is lost.
     *
     * There are three options:
     * 1. Exit the process. The situation is complex, and we can wait for someone to clean up the config.
     * 2. Don't do anything, pretend everything is working. No new process or restarting process can connect to
     *    the process. The process can not get any new configs. The situation is complex, and we can wait for
     *    someone to clean up the config.
     * 3. Let this process create and claim the coordinate if possible. The config will not get updated
     *    and if the process restarts it will not get the config again.
     * @return
     */
    public boolean recreateCoordinateAfterTotalBlackout();
}
