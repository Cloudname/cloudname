package org.cloudname;

/**
 * Base class for exception related to a specific coordinate.
 * @auther dybdahl
 */
public abstract class CoordinateException extends Exception {

    public CoordinateException(String reason) {
        super(reason);
    }
}
