package org.cloudname.core;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A generic representation of a path. A "path" might be a bit of a misnomer in the actual
 * backend implementation but it can be represented as an uniquely identifying string for the
 * leases handed out. A path can be split into elements which can be accessed individually.
 *
 * <p>Paths are an ordered set of strings consisting of the characters according to RFC 952 and
 * RFC 1123, ie [a-z,0-9,-]. The names cannot start or end with an hyphen and can be between
 * 1 and 63 characters long.
 *
 * @author stalehd@gmail.com
 */
public class CloudnamePath {
    private final String[] pathElements;
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9-]*");

    /**
     * Check if path element is a valid name according to RFC 953/RCC 1123.
     *
     * @param name The element to check
     * @return true if element is a valid string
     */
    public static boolean isValidPathElementName(final String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }

        final Matcher matcher = NAME_PATTERN.matcher(name);
        if (!matcher.matches()) {
            return false;
        }
        if (name.length() > 64) {
            return false;
        }
        if (name.charAt(0) == '-' || name.charAt(name.length() - 1) == '-') {
            return false;
        }
        return true;
    }

    /**
     * @param pathElements the string array to create the path from. Order is preserved so
     *     pathElements[0] corresponds to the first element in the path. Note that the element will
     *     be converted to lower case strings.
     * @throws AssertionError if the pathElements parameter is null
     */
    public CloudnamePath(final String[] pathElements) {
        if (pathElements == null) {
            throw new IllegalArgumentException("Path elements can not be null");
        }
        this.pathElements = new String[pathElements.length];
        for (int i = 0; i < pathElements.length; i++) {
            if (pathElements[i] == null) {
                throw new IllegalArgumentException("Path element at index " + i + " is null");
            }
            final String element = pathElements[i].toLowerCase();
            if (!isValidPathElementName(element)) {
                throw new IllegalArgumentException("Name element "
                        + element + " isn't a valid name");
            }
            this.pathElements[i] = element;
        }
    }

    /**
     * Create a new path based on an existing one by appending a new element.
     *
     * @param path The original CloudnamePath instance
     * @param additionalElement Element to append to the end of the original path
     * @throws AssertionError if one or more of the parameters are null
     */
    public CloudnamePath(final CloudnamePath path, final String additionalElement) {
        if (path == null) {
            throw new IllegalArgumentException("Path can not be null");
        }
        if (additionalElement == null) {
            throw new IllegalArgumentException("additionalElement can not be null");
        }

        if (!isValidPathElementName(additionalElement)) {
            throw new IllegalArgumentException(additionalElement + " isn't a valid path name");
        }
        this.pathElements = Arrays.copyOf(path.pathElements, path.pathElements.length + 1);
        this.pathElements[this.pathElements.length - 1] = additionalElement;

    }

    /**
     * The number of elements in the path.
     */
    public int length() {
        return pathElements.length;
    }

    /**
     * Join the path elements into a string, f.e. join "foo", "bar" into "foo:bar".
     *
     * @param separator  separator character between elements
     * @return  joined elements
     */
    public String join(final char separator) {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final String element : pathElements) {
            if (!first) {
                sb.append(separator);
            }
            sb.append(element);
            first = false;
        }
        return sb.toString();
    }

    /**
     * Get element by index.
     *
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public String get(final int index) {
        return pathElements[index];
    }

    /**
     * Check if this path is a subpath. A path is a subpath whenever it starts with the
     * same elements as the other path ("foo/bar/baz" would be a subpath of "foo/bar/baz/baz"
     * but not of "bar/foo")
     *
     * @param other Path to check
     * @return true if this path is a subpath of the specified path
     */
    public boolean isSubpathOf(final CloudnamePath other) {
        if (other == null) {
            return false;
        }
        if (this.pathElements.length > other.pathElements.length) {
            return false;
        }

        if (this.pathElements.length == 0) {
            // This is an empty path. It is the subpath of any other path.
            return true;
        }

        for (int i = 0; i < this.pathElements.length; i++) {
            if (!other.pathElements[i].equals(this.pathElements[i])) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return parent path of current. If this is the root path (ie it is empty), return the
     *     current path
     */
    public CloudnamePath getParent() {
        if (this.pathElements.length == 0) {
            return this;
        }
        return new CloudnamePath(Arrays.copyOf(pathElements, this.pathElements.length - 1));
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || !(other instanceof CloudnamePath)) {
            return false;
        }
        final CloudnamePath otherPath = (CloudnamePath) other;
        return Arrays.deepEquals(otherPath.pathElements, pathElements);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(pathElements);
    }

    @Override
    public String toString() {
        return "[ CloudnamePath (" + Arrays.toString(pathElements) + ") ]";
    }


}
