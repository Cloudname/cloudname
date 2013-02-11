package org.cloudname.flags;

/**
 * Class containing all supported Field types.
 *
 * @author acidmoose
 *
 */
public class FlagsInstanced {
    @Flag(name="string", description="String test")
    public String string = "NA";

    @Flag(name="int", description="Some int")
    private int integer = 1;

    @Flag(name="boolean")
    public boolean bool = false;

    @Flag(name="Integer")
    public Integer integer2 = new Integer(1);

    @Flag(name="Boolean")
    public Boolean bool2 = false;

    @Flag(name="long")
    public long longNum = 1L;

    @Flag(name="Long")
    public long longNum2 = 1L;

    public enum SimpleEnum {OPTION1, OPTION2};

    @Flag(name="option", options=SimpleEnum.class)
    public SimpleEnum option = SimpleEnum.OPTION1;

    /**
     * Get the value of the private variable "integer"
     */
    public int getValueForPrivateInteger() {
        return integer;
    }
}
