package org.cloudname.log.format;

import org.cloudname.log.pb.Timber;

/**
 * Format Timber.LogEvent messages to a single line using NL to
 * separate log records and TAB to separate the fields.  The fields
 * are in order:
 *
 *  <ul>
 *   <li> timestamp in ISO8601 format
 *   <li> hostname
 *   <li> process id/thread id
 *   <li> service name
 *   <li> class
 *   <li> type of log message
 *   <li> log level
 *   <li> consistency level
 *   <li> log message payload
 *  </ul>
 *
 * This formatter is quite a bit slower than the SingleLineFormatter,
 * but it is perhaps more pleasing to look at.
 *
 * @author borud
 */
public class FullFormatter implements LogEventFormatter {
    @Override
    public String format(Timber.LogEvent logEvent) {
        StringBuilder buff = new StringBuilder(200);
        Util.formatTimeISO(logEvent.getTimestamp(), buff);

        buff.append('\t')
            .append(logEvent.getHost())
            .append('\t')

            // Add process- and thread id if applicable
            .append((logEvent.hasPid() ? logEvent.getPid() : "-"))
            .append("/")
            .append((logEvent.hasTid() ? logEvent.getTid() : "-"))
            .append('\t')

            // Add service name
            .append(logEvent.getServiceName())
            .append('\t')

            // Add source
            .append(classFromSource(logEvent.getSource()))
            .append('\t')

            // Type of log message
            .append(logEvent.getType())
            .append('\t')

            // Level of log message
            .append(Util.logLevelNameForValue(logEvent.getLevel()))
            .append('\t')

            // Add consistency level
            .append(logEvent.getConsistencyLevel().toString().substring(0,2))
            .append('\t');

        // Add the payloads
        boolean first = true;
        for (Timber.Payload payload : logEvent.getPayloadList()) {
            String s = payload.getPayload().toStringUtf8();

            buff.append((first?"":" | "))
                .append(payload.getName())
                .append(": ")
                .append(Util.escape(s));

            first = false;
        }

        return buff.toString();
    }

    /**
     * Given a fully qualified class name, return just the class name
     * without the package name.
     *
     * @param source A (possibly) fully qualified class name
     * @return the class name without the package.
     */
    private String classFromSource(String source) {
        int last = source.lastIndexOf('.');
        if (-1 == last) {
            return source;
        }

        return source.substring(last + 1);
    }

}
