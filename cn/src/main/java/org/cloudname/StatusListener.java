package org.cloudname;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 12/12/11
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public interface class StatusListener {
    public enum Event {
        CLAIM_INVALID,
        LOST_CONNECTION,
        NEW_CONNECTION,
    }

    /**
     * @param name the name of the config node that was updated.
     * @param event the type of event observed on the config node.
     * @param data the contents of the config node
     */
    public void onStatusEvent(Event event, String data);
}

}
