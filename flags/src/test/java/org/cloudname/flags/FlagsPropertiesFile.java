package org.cloudname.flags;

public class FlagsPropertiesFile {
    @Flag(name="name", description = "should be set")
    static String name;

    @Flag(name="integer", description = "should be set")
    static int integer = 0;

    @Flag(name="comments", description = "should not be set")
    static String comments;

    @Flag(name="booleanvalue")
    static boolean booleanValue = false;
}
