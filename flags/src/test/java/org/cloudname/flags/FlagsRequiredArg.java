package org.cloudname.flags;

/**
 * Class containing a required variable.
 * 
 * @author acidmoose
 *
 */
public class FlagsRequiredArg {
    
    @Flag(name="int", required=true)
    public static int integer;
    
}
