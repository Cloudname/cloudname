package org.cloudname.log;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.Converter;
import org.cloudname.log.archiver.Archiver;

import java.io.File;
import java.io.IOException;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import static java.util.concurrent.TimeUnit.*;

/**
 * A combined log handler.
 *
 * This re-uses the archiver from the Timber project.
 *
 * This class is semi-thread safe.  We perform locking for the
 * archiver output, but we don't give a toss about the System.out log.
 *
 * TODO(borud): the SimpleArchiver was written to be used in a server
 *   that initiates flushing periodically so it buffers quite a bit
 *   before writing.  This can easily become a problem when used from
 *   a j.u.l.Handler so we should probably add a flusher thread that
 *   can ensure that there is some balance to how often it flushes.
 *
 *   Of course, the trivial (and wrong) thing to do would be to flush
 *   for every log message, but since the SimpleArchiver was built to
 *   be used in a somewhat different scenario, that is not
 *   feasible. (If you really want to know why, have look at the slot
 *   logging implementation in the archiver package).
 *
 *   The SimpleArchiver probably needs to be refactored and
 *   genericized a bit so it can be configured for more use-cases.
 *   Among other things we would want to use it as a replayable log
 *   store.  We also need to make it able to cap log storage.
 *
 * @author borud
 */
public class ComboHandler extends Handler {
    // Each slot file should be at most 50Mb
    private static final int MAX_FILE_SIZE = 50 * 1024 * 1024;

    // The flushing period in seconds.
    private static final int FLUSH_PERIOD = 2;

    private String logdir;
    private String coordinate;
    private Archiver archiver;
    private Converter converter;
    private LogFormatter formatter;
    private StreamHandler streamHandler;

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> flusherHandle;

    // The archiver does no locking.  Eventually replace this with
    // something a tad more elegant.
    private Object archiverLock = new Object();

    public ComboHandler(String coordinate, String logdir) {
        this.logdir = logdir;
        this.coordinate = coordinate;

        archiver = new Archiver(logdir, MAX_FILE_SIZE);
        archiver.init();

        // TODO(borud): We do not know the "serviceName" and since
        //  that code was written it was renamed to "coordinate" in the
        //  rest of the Cloudname universe.
        converter = new Converter(coordinate);

        // The following is a bit of cheating.
        streamHandler = new StreamHandler(System.out, new LogFormatter(coordinate));
        streamHandler.setLevel(Level.FINEST);

        // Start the flusher.
        startFlusher();
    }

    /**
     * Start the flusher.
     */
    private void startFlusher() {
        final Runnable flusher = new Runnable() {
                public void run() {
                    flush();
                }
            };

        // Execute the flusher with FLUSH_PERIOD initial delay and
        // FLUSH_PERIOD delay between executions after that.
        flusherHandle = scheduledExecutor.scheduleWithFixedDelay(
            flusher,
            FLUSH_PERIOD,
            FLUSH_PERIOD,
            SECONDS);
    }

    /**
     * Stop the flusher.
     */
    private void stopFlusher() {
        if (null == flusherHandle) {
            throw new IllegalStateException("Flusher was never started");
        }

        flusherHandle.cancel(true);
    }


    @Override
    public void publish(LogRecord record) {
        streamHandler.publish(record);

        Timber.LogEvent event = converter.convertFrom(record);
        synchronized(archiverLock) {
            archiver.handle(event);
        }
    }

    @Override
    public void flush() {
        streamHandler.flush();
        synchronized(archiverLock) {
            archiver.flush();
        }
    }

    @Override
    public void close() {
        stopFlusher();
        streamHandler.close();
        synchronized(archiverLock) {
            archiver.close();
        }
    }
}