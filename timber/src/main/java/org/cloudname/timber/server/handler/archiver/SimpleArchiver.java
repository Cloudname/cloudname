package org.cloudname.timber.server.handler.archiver;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.archiver.Archiver;
import org.cloudname.log.archiver.Slot;
import org.cloudname.log.archiver.SlotMapper;
import org.cloudname.log.archiver.SlotLruCache;

import org.cloudname.timber.server.handler.LogEventHandler;
import org.cloudname.timber.server.handler.LogEventHandlerException;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This class implements a very simplistic log archiver.
 *
 * @author borud
 */
public class SimpleArchiver implements LogEventHandler {
    private final Archiver archiver;

    public SimpleArchiver(String logPath, String service, long maxFileSize) {
        archiver = new Archiver(logPath, service, maxFileSize);
    }

    /**
     * Initialize the archiver.
     */
    public void init() {
        archiver.init();
    }

    @Override
    public void handle(final Timber.LogEvent logEvent) {
        archiver.handle(logEvent);
    }

    @Override
    public void flush() {
        archiver.flush();
    }

    @Override
    public void close() {
        archiver.close();
    }

    @Override
    public String getName() {
        return SimpleArchiver.class.getName();
    }
}
