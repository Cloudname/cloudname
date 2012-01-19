package org.cloudname;

/**
 * Thrown when there are problems deleting a coordinate.
 * @auther dybdahl
 */
public class CoordinateDeletionException extends CoordinateException {
    public CoordinateDeletionException(String reason) {
        super(reason);
    }
}
