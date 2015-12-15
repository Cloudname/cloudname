package org.cloudname.samples.service;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple publish/subscribe pattern to distribute events and log entries to the
 * web socket stream.
 *
 * @author stalehd@gmail.com
 */
public class NotificationPublisher {

    private List<String> allNotifications = new ArrayList<>();

    /**
     * Listener for subscriptions.
     */
    public interface SubscriptionListener {
        void onItemPublished(final String item);
    }

    private final List<SubscriptionListener> subscribers = new ArrayList<>();
    private final Object syncObject = new Object();

    /**
     * Publish an item to subscribers.
     */
    public void publish(final String item) {
        synchronized (syncObject) {
            allNotifications.add(item);
            for (final SubscriptionListener listener : subscribers) {
                try {
                    listener.onItemPublished(item);
                } catch (final Exception ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Subscribe to events.
     */
    public void subscribe(final SubscriptionListener listener) {
        synchronized (syncObject) {
            subscribers.add(listener);
            // Resend old notifications
            for (final String str : allNotifications) {
                try {
                    listener.onItemPublished(str);
                } catch (final Exception exception) {
                    // ignore
                }
            }
        }
    }
}
