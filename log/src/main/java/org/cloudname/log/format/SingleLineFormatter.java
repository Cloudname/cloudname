package org.cloudname.log.format;

import org.cloudname.log.pb.Timber;

import java.util.TreeMap;

import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    // Patterns used by the escape() method.
    private static Pattern special   = Pattern.compile("[\\\n\\\t\\\\]+");
    private static Pattern newLine   = Pattern.compile("\n");
    private static Pattern tab       = Pattern.compile("\t");
    private static Pattern backSlash = Pattern.compile("\\\\");

    private static final TreeMap<Integer,String> logLevelByValue = new TreeMap<Integer,String>();

    static {
        logLevelByValue.put(Level.CONFIG.intValue(), Level.CONFIG.getName());
        logLevelByValue.put(Level.FINE.intValue(), Level.FINE.getName());
        logLevelByValue.put(Level.FINER.intValue(), Level.FINER.getName());
        logLevelByValue.put(Level.FINEST.intValue(), Level.FINEST.getName());
        logLevelByValue.put(Level.INFO.intValue(), Level.INFO.getName());
        logLevelByValue.put(Level.SEVERE.intValue(), Level.SEVERE.getName());
        logLevelByValue.put(Level.WARNING.intValue(), Level.WARNING.getName());
        logLevelByValue.put(Integer.MAX_VALUE, "NUCLEAR");
    }



    @Override
    public String format(Timber.LogEvent logEvent) {
        StringBuilder buff = new StringBuilder(200);

        // Format the timestamp and append it to the buffer
        formatTimeSecondsSinceEpoch(logEvent.getTimestamp(), buff);

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
            .append(logLevelNameForValue(logEvent.getLevel()))
            .append('\t')

            // Add consistency level
            .append(logEvent.getConsistencyLevel())
            .append('\t');

        // Add the payloads
        boolean first = true;
        for (Timber.Payload payload : logEvent.getPayloadList()) {
            if (! first) {
                buff.append(" | ");
            }
            first = false;

            buff.append(payload.getName())
                .append(": ")
                .append(escape(payload.getPayload().toStringUtf8()));
        }

        return buff.toString();
    }


    /**
     * Convert timestamp to seconds since epoch with 3 decimal places.
     *
     * @param time the time as milliseconds since epoch
     * @param sbuffer a StringBuilder used to put the formatted number into
     */
    private static void formatTimeSecondsSinceEpoch (long time, StringBuilder sbuffer) {
        String timeString = Long.toString(time);
        int len = timeString.length();

        // something wrong.  handle it by just returning the input
        // long as a string.  we prefer this to just crashing in
        // the substring handling.
        if (len < 3) {
            sbuffer.append(timeString);
            return;
        }

        sbuffer.append(timeString.substring(0, len - 3));
        sbuffer.append('.');
        sbuffer.append(timeString.substring(len - 3));
    }

    /**
     * This static method is used to detect if a message needs
     * to be escaped, and if so, performs the escaping.  Since the
     * common case is most likely that escaping is <em>not</em>
     * needed, the code is optimized for this case.  The forbidden
     * characters are:
     *
     * <UL>
     *  <LI> newline
     *  <LI> tab
     *  <LI> backslash
     * </UL>
     *
     * <P>
     * Also handles the case where the message is <code>null</code>
     * and replaces the null message with a tag saying that the
     * value was "(empty)".
     *
     * @param s String that might need escaping
     * @return Returns escaped string
     *
     */
    public static String escape (String s) {
        if (s == null) {
            return "(empty)";
        }

        Matcher m = special.matcher(s);
        if (! m.find()) {
            return s;
        }

        // invariant: we had special characters

        m = backSlash.matcher(s);
        if (m.find()) {
            s = m.replaceAll("\\\\\\\\");
        }

        m = newLine.matcher(s);
        if (m.find()) {
            s = m.replaceAll("\\\\n");
        }

        m = tab.matcher(s);
        if (m.find()) {
            s = m.replaceAll("\\\\t");
        }

        return s;
    }





    /**
     *
     */
    private static String logLevelNameForValue(int value) {
        return logLevelByValue.ceilingEntry(value).getValue();
    }

}