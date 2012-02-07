package org.cloudname.flags;

/**
 * Class containing booleans.
 * 
 * @author acidmoose
 * 
 */
public class FlagsBooleanFlag {

    @Flag(name="boolean")
    public static boolean bool = false;

    @Flag(name="Boolean")
    public static Boolean bool2 = false;
}
