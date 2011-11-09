package org.cloudname.timber.logger;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.Converter;

import org.cloudname.timber.client.TimberClient;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * A log handler for use with the java.util.logging package.
 *
 * TODO(borud): decide if this is obsolete and possibly remove it.
 *
 * @author borud
 */
public class TimberHandler extends Handler {
    private TimberClient client;
    private Converter converter;
    private String service;
    private String host;
    private int port;

    /**
     * @param service the service on whose behalf we are logging
     * @param host the host where the log server is running
     * @param port the port the log server listens to
     */
    public TimberHandler(String service, String host, int port) {
        this.service = service;
        this.host = host;
        this.port = port;

        converter = new Converter(service);
    }

    /**
     * Ensure we have a live connection to the log server.
     */
    private void ensureLogserverConnection() {
        // Only checks that we have a client.  Does not check for
        // liveness.  This is done whenever we try to log something.
        if (null != client) {
            System.out.println("**** Already connected to " + host + ", port " + port);
            return;
        }

        System.out.println("**** Connecting to " + host + ", port " + port);
        client = new TimberClient(host, port);
        client.start();
    }

    @Override
    public void close() {
        // Check if we never set up a log connection or if the log
        // connection has died.
        if (null == client) {
            return;
        }

        client.shutdown();
        client = null;
    }

    @Override
    public void flush() {
        // Since log messages are published immediately now there is
        // no flush semantics for this handler.
    }

    @Override
    public void publish(LogRecord record) {
        ensureLogserverConnection();
        client.submitLogEvent(converter.convertFrom(record));
    }
}
