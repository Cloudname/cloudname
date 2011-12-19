package org.cloudname.timber.server.handler;

import org.cloudname.log.pb.Timber;

/**
 * This interface defines API for log event handlers.
 *
 * @author borud
 */
public interface LogEventHandler
{
    /**
     * This method is used for delivering a log event to the handler.
     * This method should return as quickly as possible.
     *
     * Implementations are expected to document whether or not they
     * honor the ConsistencyLevel given in the log message.
     *
     * @param logEvent an incoming log event.
     */
    public void handle(Timber.LogEvent logEvent)
        throws LogEventHandlerException;

    /**
     * Ensure that any log messages that have been received but which
     * have not yet been acted upon are dealt with.  For instance, if
     * the LogEventHandler is a disk-writer, that the log records are
     * flushed to disk.
     */
    public void flush();

    /**
     * Close the LogEventHandler.  Implies flush().
     */
    public void close();

    /**
     * @return the name of the LogEventHandler.
     */
    public String getName();
}
