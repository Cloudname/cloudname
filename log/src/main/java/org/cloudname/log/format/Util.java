package org.cloudname.log.format;

import java.util.TreeMap;
import java.util.NavigableMap;
import java.util.logging.Level;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Utilities used by formatting implementations.
 *
 * @author borud
 */
public class Util {
    // Format: yyyy-MM-dd'T'HH:mm:ss.SSS in UTC
    private static final DateTimeFormatter isoTimeFormatter = ISODateTimeFormat
        .dateHourMinuteSecondMillis()
        .withZone(DateTimeZone.UTC);

    // There is no convenient way to map numeric values of log levels
    // to names, so we use the log levels defined in java.util.logging
    // and we add them to the map.  Since we use the TreeMap to look
    // up the ceiling of each value we have added a bogus log level
    // called "NUCLEAR".
    private static final NavigableMap<Integer,String> logLevelByValue = new TreeMap<Integer,String>();
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

    /**
     * This is a non-instantiable class.
     */
    private Util() {}

    /**
     * Will find the first named log level higher than or equal to the
     * numberic value.
     *
     * @param value the numeric value of a log level
     * @return the name of the log level that is equal to or higher
     *   than the value we provided.
     */
    public static String logLevelNameForValue(int value) {
        return logLevelByValue.ceilingEntry(value).getValue();
    }

    /**
     * Convert timestamp to seconds since epoch with 3 decimal places.
     *
     * @param time the time as milliseconds since epoch
     * @param sbuffer a StringBuilder used to put the formatted number into
     */
    public static void formatTimeSecondsSinceEpoch(final long time, StringBuilder sbuffer) {
        String timeString = Long.toString(time);
        int len = timeString.length();

        if (len < 3) {
            // Something wrong.  Handle it by just returning the input
            // long as a string.  We prefer this to just crashing in the
            // substring handling.
            sbuffer.append(timeString);
            return;
        }

        sbuffer.append(timeString.substring(0, len - 3));
        sbuffer.append('.');
        sbuffer.append(timeString.substring(len - 3));
    }

    /**
     * Format timestamp as ISO8601 formatted date in UTC timezone.
     *
     * @param time the time as milliseconds since epoch
     * @param sbuffer a StringBuilder used to put the formatted timestamp into
     */
    public static void formatTimeISO(long time, StringBuilder sbuffer) {
        sbuffer.append(isoTimeFormatter.print(time));
    }

    /**
     * Replace NL, TAB and Backslash with their escaped versions.  (We
     * could probably speed up this method a bit more by replacing the
     * disallowed characters with SPACE rather than escaping them.)
     *
     * @param s the String we wish to escape
     * @return a String guaranteed to not contain NL, TAB or single backslashes
     */
    public static String escape(String s) {
        // Guesstimate on length.  Doesn't need to be accurate, only
        // probable
        StringBuilder buff = new StringBuilder(s.length() + 20);

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch(c) {
                case '\r':
                    // If not followed by '\n', convert to '\n', else strip.
                    if (i == s.length() - 1 || s.charAt(i + 1) != '\n') {
                        buff.append("\\n");
                    }
                    break;
                case '\n':
                    buff.append("\\n");
                    break;

                case '\t':
                    buff.append("\\t");
                    break;

                case '\\':
                    buff.append("\\\\");
                    break;

                default:
                    buff.append(c);
            }
        }

        return buff.toString();
    }
}
