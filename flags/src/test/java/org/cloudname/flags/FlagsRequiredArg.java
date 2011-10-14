package org.cloudname.flags;

/**
 * Class containing a required variable.
 * 
 * @author acidmoose
 *
 */
public class FlagsRequiredArg {
    
    @Flag(name="int", defaultValue="1", required=true)
    public static int integer;
    
}
