#flags

This is a library to help developers build neatly formatted and easy to understand command line parsing.

#example

This example can be run with the command **java FlagsStarter --help** which will print the command line help, or **java FlagsStarter --text HelloWorld --times 2 --active true** which will print "HelloWorld" 2 times.

	import org.cloudname.flags.Flag;
	import org.cloudname.flags.Flags;

	public class FlagsStarter {
    
    	@Flag(name="text", defaultValue="Default boring text", description="Output text")
    	public static String text;
    
    	@Flag(name="times", defaultValue="1", required=true, description="Number of times to print output text")
    	public static int times;
    
    	@Flag(name="active", defaultValue="false", required=true, description="Should I run the task?")
    	public static boolean active;
    
    	public FlagsStarter() {
        	if (active) {
            	for (int i = 0; i < times; i++) {
                	System.out.println(text);
            	}
        	}
    	}
    
    	/**
     	* @param args
     	*/
    	public static void main(String[] args) {
        	Flags flags = new Flags()
            	.loadOpts(FlagsStarter.class)
            	.parse(args);
        
        	//quit if help has been called
        	if (flags.helpCalled())
            	return;
        
        	FlagsStarter starter = new FlagsStarter();
    	}

	}
