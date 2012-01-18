package org.cloudname;

/**
 * It was assumed that the coordinate did not exist, but it did.
 * @auther dybdahl
 */
public class CoordinateExistsException extends CoordinateException {
    public CoordinateExistsException(String reason) {
        super(reason);
    }
}
