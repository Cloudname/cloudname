package org.cloudname.flags;

/**
 * Class containing integers for testing non-option-args.
 * 
 * @author acidmoose
 * 
 */
public class FlagsOptionTest {

    @Flag(name="int", description="Some int")
    private static int integer = 1;

    @Flag(name="Integer")
    public static Integer integer2 = new Integer(1);
    /**
     * Get the value of the private variable "integer"
     */
    public static int getValueForPrivateInteger() {
        return integer;
    }
}
