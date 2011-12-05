package org.cloudname.log;

import org.cloudname.log.pb.Timber;
import static org.cloudname.log.pb.Timber.ConsistencyLevel;

import java.net.InetAddress;
import java.net.UnknownHostException;
import com.google.protobuf.ByteString;

/**
 * Miscellaneous utilities.
 *
 * @author borud
 */
public class LogUtil {
    private static String hostName;

    // I am not a big fan of static sections, but there seems to be no
    // intelligent way to do this.
    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostName = null;
        }
    }

    /**
     * Simple factory for creating UTF8 text log messages.
     *
     * @param level an integer representing the log level
     * @param service the name of the service which is logging
     * @param source the source from whence the log message originated
     * @param message the log message itself
     *
     * @return a Timber.LogEvent of type "T"
     */
    public static Timber.LogEvent textEvent(int level,
                                            String service,
                                            String source,
                                            String message)
    {
        return Timber.LogEvent.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setConsistencyLevel(ConsistencyLevel.BESTEFFORT)
            .setLevel(level)
            .setHost(hostName)
            .setServiceName(service)
            .setSource(source)
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8(message)))
            .build();
    }

    /**
     * @return the hostname of the machine we are running on
     *   or {@code null} if the hostname is not possible to
     *   figure out.
     */
    public static String getHostName()
    {
        return hostName;
    }
}
