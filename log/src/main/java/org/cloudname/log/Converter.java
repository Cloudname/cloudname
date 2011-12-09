package org.cloudname.log;

import org.cloudname.log.pb.Timber;
import static org.cloudname.log.pb.Timber.ConsistencyLevel;

import com.google.protobuf.ByteString;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.LogRecord;
import java.util.logging.Level;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * This utility class converts log messages to Timber.LogEvent
 * instances.
 *
 * @author borud
 */
public class Converter {
    private static String hostName;
    private String serviceName;

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
     * Create a Converter for a given service.
     *
     * @param serviceName the name of the service we are logging on
     *  behalf of.
     */
    public Converter(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Create a Timber.LogEvent from a LogRecord.
     *
     * <p>
     * TODO(borud): we need a better way of defining the type (setType()).
     * TODO(borud): add encoding of nested exception.
     *
     * @param rec the LogRecord we wish to convert
     * @return a Timber.LogEvent instance.
     */
    public Timber.LogEvent convertFrom(LogRecord rec) {
        Timber.LogEvent.Builder eventBuilder =  Timber.LogEvent.newBuilder()
            .setTimestamp(rec.getMillis())
            .setConsistencyLevel(ConsistencyLevel.BESTEFFORT)
            .setLevel(rec.getLevel().intValue())
            .setHost(hostName)
            .setServiceName(serviceName)
            .setSource(rec.getSourceClassName() + "#" + rec.getSourceMethodName())
            .setPid(0)
            .setTid(rec.getThreadID())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8(rec.getMessage())));

        // Check if we have an exception
        Throwable cause = rec.getThrown();
        if (cause != null) {
            // Awkward
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            cause.printStackTrace(new PrintStream(os));

            eventBuilder.addPayload(
                Timber.Payload.newBuilder()
                .setName("exception")
                .setContentType("application/java-exception")
                .setPayload(ByteString.copyFrom(os.toByteArray()))
            );
        }

        return eventBuilder.build();
    }
}
