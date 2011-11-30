package org.cloudname.flags;

/**
 * Class containing all supported Field types.
 *
 * @author acidmoose
 *
 */
public class FlagsAllLegalFields {

    @Flag(name="string", description="String test")
    public static String string = "NA";

    @Flag(name="int", description="Some int")
    public static int integer = 1;

    @Flag(name="boolean")
    public static boolean bool = false;

    @Flag(name="Integer")
    public static Integer integer2 = new Integer(1);

    @Flag(name="Boolean")
    public static Boolean bool2 = false;

    @Flag(name="long")
    public static long longNum = 1L;

    @Flag(name="Long")
    public static long longNum2 = 1L;

    public enum SimpleEnum {OPTION1, OPTION2};

    @Flag(name="option", options=SimpleEnum.class)
    public static SimpleEnum option = SimpleEnum.OPTION1;
}
