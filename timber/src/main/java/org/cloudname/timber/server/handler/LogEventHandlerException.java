package org.cloudname.timber.server.handler;

/**
 * General exception for LogEventHandler problems.
 *
 * @author borud
 */
public class LogEventHandlerException extends RuntimeException {
    public LogEventHandlerException(String msg)
    {
        super(msg);
    }

    public LogEventHandlerException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
