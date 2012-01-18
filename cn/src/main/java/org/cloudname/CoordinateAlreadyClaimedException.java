package org.cloudname;

/**
 * Thrown when coordinate is claimed and it is assumed it is not.
 * @auther dybdahl
 */
public class CoordinateAlreadyClaimedException extends CoordinateException {
    public CoordinateAlreadyClaimedException(String reason) {
        super(reason);
    }
}
