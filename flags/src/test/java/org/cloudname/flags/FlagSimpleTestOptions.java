package org.cloudname.flags;

/**
 * 
 * @author acidmoose
 *
 */
public class FlagSimpleTestOptions {

    @Flag(name="text", type=Flag.TYPE_STRING, defaultValue="N/A", description="Output text")
    public static String text;
    
    @Flag(name="times", type=Flag.TYPE_INTEGER, defaultValue="1", description="Number of times to print output text")
    public static int times;
    
    @Flag(name="active", type=Flag.TYPE_BOOLEAN, defaultValue="false", description="Should I run the task?")
    public static boolean active;
    
}
