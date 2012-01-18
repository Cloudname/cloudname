package org.cloudname;

/**
 * Exceptions for Cloudname cased by problems talking to storage.
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

}
