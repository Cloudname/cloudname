package org.cloudname;

/**
 * Exceptions for Cloudname caused by problems talking to storage.
 *
 * @author borud
 */
public class CloudnameException extends Exception {

    public CloudnameException(Throwable t) {
        super(t);
    }

    public CloudnameException(String message) {
        super(message);
    }
    
    public CloudnameException(String message, Throwable t) {
        super(message, t);
    }
}
