package org.cloudname.service;

import org.cloudname.core.CloudnamePath;
import org.json.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A coordinate representing a running service. The coordinate consists of four parts; instance id,
 * service name, tag and region.
 *
 * <p>Note that the order of elements in the string representation is opposite of the CloudnamePath
 * class; you can't create a canonical representation of the instance coordinate by calling join()
 * on the CloudnamePath instance.
 *
 * @author stalehd@gmail.com
 */
public class InstanceCoordinate {
    private static final Pattern COORDINATE_PATTERN = Pattern.compile("(.*)\\.(.*)\\.(.*)\\.(.*)");
    private static final String REGION_NAME = "region";
    private static final String TAG_NAME = "tag";
    private static final String SERVICE_NAME = "service";
    private static final String INSTANCE_NAME = "instance";


    private final String region;
    private final String tag;
    private final String service;
    private final String instance;

    /**
     * Create new instance based on CloudnamePath.
     *
     * @throws IllegalArgumentException if parameters are invalid
     */
    /* package-private */ InstanceCoordinate(final CloudnamePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path can not be null");
        }
        if (path.length() != 4) {
            throw new IllegalArgumentException("Path must contain 4 elements");
        }
        this.region = path.get(0);
        this.tag = path.get(1);
        this.service = path.get(2);
        this.instance = path.get(3);
    }

    /**
     * The region of the coordinate.
     */
    public String getRegion() {
        return region;
    }

    /**
     * The tag of the coordinate.
     */
    public String getTag() {
        return tag;
    }

    /**
     * The service name.
     */
    public String getService() {
        return service;
    }

    /**
     * The instance identifier.
     */
    public String getInstance() {
        return instance;
    }

    /**
     * A CloudnamePath instance representing this coordinate.
     */
    /* package-private */ CloudnamePath toCloudnamePath() {
        return new CloudnamePath(
                new String[] {this.region, this.tag, this.service, this.instance});
    }

    /**
     * Canonical string representation of coordinate.
     */
    public String toCanonicalString() {
        return new StringBuffer()
            .append(instance).append(".")
            .append(service).append(".")
            .append(tag).append(".")
            .append(region)
            .toString();
    }

    /**
     * Coordinate represented as a JSON-formatted string.
     */
    /* package-private */ String toJsonString() {
        return new JSONObject()
            .put(REGION_NAME, this.region)
            .put(TAG_NAME, this.tag)
            .put(SERVICE_NAME, this.service)
            .put(INSTANCE_NAME, this.instance)
            .toString();
    }

    /**
     * Create InstanceCoordinate instance from JSON string.
     */
    /* package-private */ static InstanceCoordinate fromJson(final String jsonString) {
        final JSONObject object = new JSONObject(jsonString);
        final String[] pathElements = new String[4];
        pathElements[0] = object.getString(REGION_NAME);
        pathElements[1] = object.getString(TAG_NAME);
        pathElements[2] = object.getString(SERVICE_NAME);
        pathElements[3] = object.getString(INSTANCE_NAME);

        return new InstanceCoordinate(new CloudnamePath(pathElements));
    }

    /**
     * Parse a canonical string representation of a coordinate.
     */
    public static InstanceCoordinate parse(final String string) {
        if (string == null) {
            return null;
        }
        final Matcher matcher = COORDINATE_PATTERN.matcher(string);
        if (!matcher.matches()) {
            return null;
        }
        final String[] path = new String[] {
                matcher.group(4), matcher.group(3), matcher.group(2), matcher.group(1)
        };
        return new InstanceCoordinate(new CloudnamePath(path));
    }

    @Override
    public String toString() {
        return "[ Coordinate " + toCanonicalString() + "]";
    }
}
