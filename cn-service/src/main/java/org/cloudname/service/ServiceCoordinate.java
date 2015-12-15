package org.cloudname.service;

import org.cloudname.core.CloudnamePath;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A coordinate pointing to a set of services or a single permanent service.
 *
 * @author stalehd@gmail.com
 */
public class ServiceCoordinate {
    // TODO: Tests, javadoc
    public static class Builder {
        private String region;
        private String tag;
        private String service;

        public Builder() {

        }

        public Builder setRegion(final String region) {
            this.region = region;
            return this;
        }

        public Builder setTag(final String tag) {
            this.tag = tag;
            return this;
        }

        public Builder setService(final String service) {
            this.service = service;
            return this;
        }

        public Builder fromCoordinate(final ServiceCoordinate coordinate) {
            service = coordinate.getService();
            tag = coordinate.getTag();
            region = coordinate.getRegion();
            return this;
        }

        public ServiceCoordinate build() {
            if (region == null) {
                throw new IllegalStateException("Region can't be null");
            }
            if (tag == null) {
                throw new IllegalStateException("Tag can't be null");
            }
            if (service == null) {
                throw new IllegalStateException("Service can't be null");
            }
            return new ServiceCoordinate(new CloudnamePath(new String[] {region, tag, service}));
        }
    }

    private final String region;
    private final String tag;
    private final String service;

    // Pattern for string parsing
    private static final Pattern COORDINATE_PATTERN = Pattern.compile("(.*)\\.(.*)\\.(.*)");

    /**
     * @param path The CloudnamePath instance to use when building the coordinate. The coordinate
     *     must consist of three elements and can not be null.
     * @throws IllegalArgumentException if parameter is invalid
     */
    /* package-private */ ServiceCoordinate(final CloudnamePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path can not be null");
        }
        if (path.length() != 3) {
            throw new IllegalArgumentException("Path must have three elements");
        }
        region = path.get(0);
        tag = path.get(1);
        service = path.get(2);
    }

    /**
     * @return The coordinate's region
     */
    public String getRegion() {
        return region;
    }

    /**
     * @return The coordinate's tag
     */
    public String getTag() {
        return tag;
    }

    /**
     * @return The coordinate's service name
     */
    public String getService() {
        return service;
    }

    /**
     * @param serviceCoordinateString String representation of coordinate
     * @return ServiceCoordinate instance built from the string. Null if the coordinate
     *     can't be parsed correctly.
     */
    public static ServiceCoordinate parse(final String serviceCoordinateString) {
        final Matcher matcher = COORDINATE_PATTERN.matcher(serviceCoordinateString);
        if (!matcher.matches()) {
            return null;
        }
        final String[] path = new String[] { matcher.group(3), matcher.group(2), matcher.group(1) };
        return new ServiceCoordinate(new CloudnamePath(path));
    }

    /**
     * @return CloudnamePath representing this coordinate
     */
    /* package-private */ CloudnamePath toCloudnamePath() {
        return new CloudnamePath(new String[] { this.region, this.tag, this.service });
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ServiceCoordinate other = (ServiceCoordinate) o;

        if (!this.region.equals(other.region)
                || !this.tag.equals(other.tag)
                || !this.service.equals(other.service)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = region.hashCode();
        result = 31 * result + tag.hashCode();
        result = 31 * result + service.hashCode();
        return result;
    }

}
