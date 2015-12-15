package org.cloudname.samples.service;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the websocket class that sends notifications to the web page. Notifications are streamed
 * like a squence of JSON objects, each looking like this:
 * <pre>
 *     {
 *       "coordinate" : "[coordinate string]",
 *       "action": "[created|removed]"
 *     }
 * </pre>
 *
 * @author stalehd@gmail.com
 */
@WebSocket
public class NotificationsWebSocket {
    private static final Logger LOG = Logger.getLogger(NotificationsWebSocket.class.getName());

    /**
     * Connect handler.
     */
    @OnWebSocketConnect
    public void onConnect(final Session session) {
        // Start sending notifications
        LOG.info(session + " connected to web socket");
        PacServer.publisher.subscribe((item) -> {
            try {
                session.getRemote().sendString(item);
                LOG.info("Sent notification " + item + " to remote " + session.getRemoteAddress());
            } catch (final IOException ioe) {
                // ignore
            }
        });
    }

    /**
     * Close handler.
     */
    @OnWebSocketClose
    public void onClose(final Session session, final int statusCode, final String reason) {
        // just ignore it
        LOG.info("Closing socket with session: " + session
                + " statusCode: " + statusCode
                + " reason: " + reason);
    }

    /**
     * Error handler.
     */
    @OnWebSocketError
    public void onError(final Session session, final Throwable reason) {
        LOG.log(Level.INFO, "Got error for " + session + " reason: " + reason);
    }

    /**
     * Message handler.
     */
    @OnWebSocketMessage
    public void onMessage(final Session session, final String message) throws IOException {
        LOG.info("Got text message from " + session + " saying " + message);
    }
}
