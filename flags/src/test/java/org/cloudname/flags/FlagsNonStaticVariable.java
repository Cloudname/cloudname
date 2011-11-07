package org.cloudname.flags;

/**
 * Class containing a non static variable.
 * 
 * @author acidmoose
 *
 */
public class FlagsNonStaticVariable {
    
    @Flag(name="string", description="String test")
    public String string = "NA";
    
}
