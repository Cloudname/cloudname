package org.cloudname;

/**
 * Exception related to a problem with a specific endpoint.
 * @auther dybdahl
 */
public class EndpointException extends Exception {
    public EndpointException(String message) {
        super(message);
    }
}
