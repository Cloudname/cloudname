package org.cloudname.log.archiver;

/**
 * General exception for Archiver problems.
 *
 * @author borud
 */
public class ArchiverException extends RuntimeException {
    public ArchiverException(String msg)
    {
        super(msg);
    }

    public ArchiverException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
