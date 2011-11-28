package org.cloudname.log.format;

import org.cloudname.log.pb.Timber;

/**
 * This interface defines an API for formatters of the Timber.LogEvent
 * message format.  Generally these formatters are used to produce
 * text instances of Timber.LogEvent messages.
 *
 * @author borud
 */
public interface LogEventFormatter {
    /**
     * Format the log event as a String.
     */
    public String format(Timber.LogEvent logEvent);
}
