package org.cloudname.flags;

/**
 * Class containing all supported Field types.
 *
 * @author acidmoose
 *
 */
public class FlagsAllLegalFields {

    @Flag(name="string", defaultValue="NA", description="String test")
    public static String string;

    @Flag(name="int", defaultValue="1", description="Some int")
    public static int integer;

    @Flag(name="boolean", defaultValue="false")
    public static boolean bool;

    @Flag(name="Integer", defaultValue="1")
    public static Integer integer2;

    @Flag(name="Boolean", defaultValue="false")
    public static Boolean bool2;

    @Flag(name="long", defaultValue="1")
    public static long longNum;

    @Flag(name="Long", defaultValue="1")
    public static long longNum2;
}
