package org.cloudname.log.format;

import org.cloudname.log.pb.Timber;

/**
 * Format Timber.LogEvent messages on a single line using NL to
 * separate log records and TAB to separate fields. The fields are, in
 * order:
 *
 *  <ul>
 *   <li> timestamp
 *   <li> hostname
 *   <li> thread id / process id
 *   <li> service name
 *   <li> source
 *   <li> type of log message
 *   <li> log level
 *   <li> consistency level
 *   <li> log message payload
 *  </ul>
 *
 * @author borud
 */
public class SingleLineFormatter implements LogEventFormatter {
    @Override
    public String format(Timber.LogEvent logEvent) {
        StringBuilder buff = new StringBuilder(200);

        // Format the timestamp and append it to the buffer
        Util.formatTimeSecondsSinceEpoch(logEvent.getTimestamp(), buff);

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
            .append(logEvent.getSource())
            .append('\t')

            // Type of log message
            .append(logEvent.getType())
            .append('\t')

            // Level of log message
            .append(Util.logLevelNameForValue(logEvent.getLevel()))
            .append('\t')

            // Add consistency level
            .append(logEvent.getConsistencyLevel())
            .append('\t');

        // Add the payloads
        boolean first = true;
        for (Timber.Payload payload : logEvent.getPayloadList()) {
            buff.append((first?"":" | "))
                .append(payload.getName())
                .append(": ")
                .append(Util.escape(payload.getPayload().toStringUtf8()));

            first = false;
        }


        return buff.toString();
    }
}
