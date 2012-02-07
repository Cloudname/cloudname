package org.cloudname.flags;

/**
 * Class containing enum for testing.
 * 
 * @author acidmoose
 * 
 */
public class FlagsEnum {

    public enum SimpleEnum {OPTION1, OPTION2};

    @Flag(name="option", options=SimpleEnum.class)
    public static SimpleEnum option = SimpleEnum.OPTION1;
}
