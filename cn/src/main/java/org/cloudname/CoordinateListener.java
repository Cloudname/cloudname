package org.cloudname;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 11.01.12
 * Time: 08:47
 * To change this template use File | Settings | File Templates.
 */
public interface CoordinateListener {
    public enum Event {
        LOST_OWNERSHIP,
        LOST_CONNECTION_TO_STORAGE,
        COORDINATE_CONFIRMED,
        COORDINATE_VANISHED,
        COORDINATE_CORRUPTED
    }
    public void onConfigEvent(Event event);
}
