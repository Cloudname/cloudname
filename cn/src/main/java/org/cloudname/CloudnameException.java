package org.cloudname;

/**
 * Exceptions for Cloudname.
 *
 * @author borud
 */
public class CloudnameException extends RuntimeException {
    /**
     * The coordinate we attempted to claim has already been claimed
     * by a different instance.
     */
    public static class AlreadyClaimed extends CloudnameException {
        public AlreadyClaimed() {
            super();
        }

        public AlreadyClaimed(Throwable t) {
            super(t);
        }
    }

    /**
     * The coordinate does not exist.
     */
    public static class CoordinateNotFound extends CloudnameException {
        public CoordinateNotFound() {
            super();
        }

        public CoordinateNotFound(Throwable t) {
            super(t);
        }
    }

    /**
     * The endpoint the user tried to publish already existed.
     */
    public static class EndpointExists extends CloudnameException {
        public EndpointExists() {
            super();
        }

        public EndpointExists(Throwable t) {
            super(t);
        }
    }

    /**
     * The endpoint the user tried to publish already existed.
     */
    public static class EndpointDoesNotExist extends CloudnameException {
        public EndpointDoesNotExist() {
            super();
        }

        public EndpointDoesNotExist(Throwable t) {
            super(t);
        }
    }

    public CloudnameException() {
        super();
    }

    public CloudnameException(Throwable t) {
        super(t);
    }

}
