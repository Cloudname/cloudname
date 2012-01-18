package org.cloudname.flags;

/**
 * Class containing a field without a default value.
 * Not giving this as an argument should not result in an NPE,
 * meaning that Flags will still attempt to set this value.
 *
 * @author acidmoose
 *
 */
public class FlagsArgumentNPE {

    @Flag(name="string", description="Not giving this argument should not yield NPE")
    public static String argWithoutDefault;

}
