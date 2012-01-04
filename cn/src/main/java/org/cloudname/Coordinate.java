package org.cloudname;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * This class represents a service coordinate.  A coordinate is given
 * by four pieces of data.
 *
 * <dl>
 *  <dt> Cell
 *  <dd> A cell is roughly equivalent to "data center".  The strict definition
 *       is that a cell represents a ZooKeeper installation.  You can have
 *       multiple cells in a physical datacenter, but it is not advisable to
 *       have ZooKeeper installations span physical data centers.
 *
 *  <dt> User
 *  <dd> The user owning the service.  May or may not have any relation to
 *       the operating system user.
 *
 *  <dt> Service
 *  <dd> The name of the service.
 *
 *  <dt> Instance
 * </dd> An integer [0, Integer.MAX_VALUE) indicating the instance number
 *       of the service.
 *
 * The canonical form of a coordinate is {@code 0.service.user.dc}.
 *
 * This class is immutable.
 *
 * @author borud
 */
public class Coordinate {
    private final String cell;
    private final String user;
    private final String service;
    private final int instance;

    // TODO(borud): allow for numbers in service, user and cell.  Just
    //   not the first character.
    public static final Pattern coordinatePattern
        = Pattern.compile("^(\\d+)\\." // instance
                          + "([a-z][a-z0-9-_]*)\\." // service
                          + "([a-z][a-z0-9-_]*)\\." // user
                          + "([a-z][a-z0-9-_]*)\\z"); // cell

    /**
     * Create a new coordinate instance.
     *
     * @param instance the instance number
     * @param service the service name
     * @param user the user name
     * @param cell the cell name
     * @throws IllegalArgumentException if the coordinate is invalid.
     */
    @JsonCreator
    public Coordinate (@JsonProperty("instance") int instance,
                       @JsonProperty("service") String service,
                       @JsonProperty("user") String user,
                       @JsonProperty("cell") String cell) {
        // Enables validation of coordinate.
        this(instance, service, user, cell, true);
    }

    /**
     * Internal version of constructor. Makes validation optional.
     */
    public Coordinate (int instance, String service, String user, String cell, boolean validate) {
        this.instance = instance;
        this.service = service;
        this.user = user;
        this.cell = cell;

        if (instance < 0) {
            throw new IllegalArgumentException("Invalid instance number: " + instance);
        }

        // If the coordinate was created by the parse() method the
        // coordinate has already been parsed using the
        // coordinatePattern so no validation is required.  If the
        // coordinate was defined using the regular constructor we
        // need to validate the parts.  And we do this by re-using the
        // coordinatePattern.
        if (validate) {
            if (! coordinatePattern.matcher(asString()).matches()) {
                throw new IllegalArgumentException("Invalid coordinate: '" + asString() + "'");
            }
        }
    }

    /**
     * Parse coordinate and create a {@code Coordinate} instance from
     * a {@code String}.
     *
     * @param s Coordinate we wish to parse as a string.
     * @return a Coordinate instance equivalent to {@code s}
     * @throws IllegalArgumentException if the coordinate string {@s}
     *   is not a valid coordinate.
     */
    public static Coordinate parse(String s) {
        Matcher m = coordinatePattern.matcher(s);
        if (! m.matches()) {
            throw new IllegalArgumentException("Malformed coordinate: " + s);
        }

        int instance = Integer.parseInt(m.group(1));
        String service = m.group(2);
        String user = m.group(3);
        String cell = m.group(4);

        return new Coordinate(instance, service, user, cell, false);
    }

    public String getCell() {
        return cell;
    }

    public String getUser() {
        return user;
    }

    public String getService() {
        return service;
    }

    public int getInstance() {
        return instance;
    }

    public String asString() {
        return instance + "." + service + "." + user + "." + cell;
    }

    @Override
    public String toString() {
        return asString();
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (this == o) {
            return true;
        }

        if (getClass() != o.getClass()) {
            return false;
        }

        Coordinate c = (Coordinate) o;
        return ((instance == instance)
                && service.equals(c.service)
                && user.equals(c.user)
                && cell.equals(c.cell));
    }

    @Override
    public int hashCode() {
        return asString().hashCode();
    }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static Coordinate fromJson(String json) throws IOException {
        return new ObjectMapper().readValue(json, Coordinate.class);
    }

}
