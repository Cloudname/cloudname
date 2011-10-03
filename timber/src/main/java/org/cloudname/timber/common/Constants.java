package org.cloudname.timber.common;

/**
 * Common constants and default values for Timber.
 *
 * @author borud
 */
public class Constants {
    /**
     * The default port for the Timber server.
     */
    public static final int DEFAULT_TIMBER_PORT = 9202;

    /**
     * The default receive buffer size for the server.
     */
    public static final int DEFAULT_TIMBER_SERVER_RECEIVE_BUFFER_SIZE = (100 * 1024);

    /**
     * The default number of backlogged connections
     */
    public static final int DEFAULT_TIMBER_SERVER_BACKLOG = 500;

    /**
     * The default length of the Dispatcher input queue.
     */
    public static final int DEFAULT_DISPATCHER_QUEUE_LENGTH = 2048;

    public static final int DEFAULT_MAX_ARCHIVER_FILESIZE = (100 * 1024 * 1024);
}
