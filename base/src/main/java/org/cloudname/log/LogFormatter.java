package org.cloudname.log;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Log formatter that outputs a sensible and parseable format.
 *
 * <p>
 * TODO(borud): implement a parser as well and move this class to a
 *   separate log utility artifact.
 *
 * @author borud
 */
public class LogFormatter extends SimpleFormatter {
    private static Pattern special   = Pattern.compile("[\\\n\\\t\\\\]+");
    private static Pattern newLine   = Pattern.compile("\n");
    private static Pattern tab       = Pattern.compile("\t");
    private static Pattern backSlash = Pattern.compile("\\\\");

    // Unknown values are set to this string
    private static final String unknown = "-";

    // TODO(borud): look up hostname
    private String hostname = unknown;
    private String serviceName = unknown;

    /**
     * Create an instance of the LogFormatter with a given
     * serviceName set.
     *
     * @param serviceName the value we wish to log for service name.
     */
    public LogFormatter (String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("serviceName cannot be null");
        }
        this.serviceName = serviceName;

        String h = getHostName();
        if (null != h) {
            hostname = h;
        }
    }

    /**
     * Get the hostname.
     *
     * @return the current hostname or {@code null} if unable to
     *   resolve it.
     */
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName().split("\\.")[0];
        } catch (UnknownHostException e) {
            return null;
        }
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
     * It is easier to slice and dice strings in Java than formatting
     * numbers...
     */
    public void formatTime (long time, StringBuffer sbuffer) {
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

    public String format (LogRecord r) {
        // Guess that most log messages will be smaller than 200 chars.
        StringBuffer sbuf = new StringBuffer(200);

        String levelName = r.getLevel().getName();

        String component = r.getLoggerName();
        if (component == null) {
            component = "-";
        }

        // format the time
        formatTime(r.getMillis(), sbuf);
        sbuf.append("\t");

        sbuf.append(hostname).append("\t")
            .append("-/").append(r.getThreadID()).append("\t")
            .append(serviceName).append("\t")
            .append(component).append("\t")
            .append(levelName).append("\t")
            .append(escape(r.getMessage()));

        // do we have a throwable?  if so, let the fun and games ensue.
        Throwable t = r.getThrown();
        if (t != null) {
            formatException(t, sbuf);
        }

        sbuf.append("\n");
        return sbuf.toString();
    }

    /**
     * Format throwable into given StringBuffer.
     *
     * @param t The Throwable we want to format
     * @param sbuf The stringbuffer into which we wish to
     *             format the Throwable
     */
    public static void formatException (Throwable t, StringBuffer sbuf) {
        Throwable last = t;
        int depth = 0;
        while (last != null) {
            sbuf.append(" msg=\"");
            sbuf.append(escape(last.getMessage()));
            sbuf.append("\" name=\"");
            sbuf.append(escape(last.getClass().getName()));
            sbuf.append("\" stack=\"");

            // loop through stack frames and format them
            StackTraceElement[] st = last.getStackTrace();
            int stopAt = Math.min(st.length, 15);
            boolean first = true;
            for (int i = 0; i < stopAt; i++) {
                if (first) {
                    first = false;
                } else {
                    sbuf.append(", ");
                }
                sbuf.append(escape(st[i].toString()));
            }

            // tell the reader if we chopped off part of the stacktrace
            if (stopAt < st.length) {
                sbuf.append(", [...]");
            }
            sbuf.append("\"");

            last = last.getCause();
            depth++;
        }
        sbuf.append(" nesting=").append(depth);
    }

    /**
     * Get the service name for this formatter.
     *
     * @return Returns the service name.
     */
    public String getServiceName () {
        return serviceName;
    }
}
