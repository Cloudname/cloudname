package org.cloudname;

/**
 * This interface defines the callback interface used to notify of
 * config node changes.
 *
 * @author borud
 */

public interface ConfigListener {
    public enum Event {
        CREATE,
        UPDATED,
        DELETED,
    }

    /**
     * This method is called whenever the application needs to be
     * notified of events related to configuration.
     *
     * @param event the type of event observed on the config node.
     * @param data the contents of the config node
     */
    public void onConfigEvent(Event event, String data);
}