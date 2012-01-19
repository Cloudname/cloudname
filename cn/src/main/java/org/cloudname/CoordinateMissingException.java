package org.cloudname;

/**
 * Exception related to a coordinate that is missing.
 * @auther dybdahl
 */
public class CoordinateMissingException extends CoordinateException {
    public CoordinateMissingException(String reason) {
        super(reason);
    }
}
