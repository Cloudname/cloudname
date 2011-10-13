package org.cloudname.flags;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * This class can load command line arguments based of Flag annotations through JOpt Simple.
 * 
 * Fields must be static, and defined as a String, Long, long, Integer, int, Boolean or boolean
 * and the Flag must have the corresponding type.
 * 
 * Typical use:
 * 
 * @Flag(name="text", type=Flag.TYPE_STRING, defaultValue="N/A", description="Output text")
 * public static String text;
 * 
 * Flags flags = new Flags()
 *              .loadOpts(MyClass.class)
 *              .parse(args);
 *              
 * System.out.print(text);
 * 
 * The class supports the use of --help. If --help is given, parse will just print the help
 * and not attempt to set any values.
 * 
 * @author acidmoose
 *
 */
public class Flags {

    /**
     * The option set builder.
     */
    private final static OptionParser optionParser = new OptionParser();

    /**
     * The help option.
     */
    private final static OptionSpec<Void> HELP = optionParser
            .accepts("help", "Show this help");

    /**
     * Helper list for loaded options.
     */
    private List<OptionHolder> options = new ArrayList<OptionHolder>();

    /**
     * Has help been called. If it has then parse() has not set any values.
     */
    private boolean helpCalled = false;

    /**
     * Load a class that contains Flag annotations.
     * 
     * @param c - the class to load options from.
     * @return this
     */
    public Flags loadOpts(Class<?> c) {
        for (Field field : c.getFields()) {
            Flag flag = field.getAnnotation(Flag.class);
            if (flag != null) {
                String name = flag.name();
                //check to see that we can set the field«s value
                if (!Modifier.isStatic(field.getModifiers())) {
                    throw new IllegalStateException("Field "+field.toGenericString()+" is not static and cannot be modified.");
                } else {
                    String description = flag.description();
                    if (flag.type() == Flag.TYPE_INTEGER) {
                        if (!field.getType().isAssignableFrom(Integer.TYPE) && !field.getType().isAssignableFrom(Integer.class)) {
                            throw new IllegalArgumentException("Field "+field.toGenericString()+" is not the same type (Integer) as defined in the Flag.");
                        }
                        OptionSpec<Integer> option;
                        if (flag.required()) {
                            option = optionParser
                                    .accepts(name, description)
                                    .withRequiredArg().ofType(Integer.class);
                        } else {
                            option = optionParser
                                    .accepts(name, description)
                                    .withOptionalArg().ofType(Integer.class);
                        }
                        options.add(new OptionHolder(flag, field, option));
                    } else if (flag.type() == Flag.TYPE_STRING) {
                        if (!field.getType().isAssignableFrom(String.class)) {
                            throw new IllegalArgumentException("Field "+field.toGenericString()+" is not the same type (String) as defined in the Flag.");
                        }
                        OptionSpec<String> option;
                        if (flag.required()) {
                            option = optionParser
                                    .accepts(name, description)
                                    .withRequiredArg().ofType(String.class);
                        } else {
                            option = optionParser
                                    .accepts(name, description)
                                    .withOptionalArg().ofType(String.class);
                        }
                        options.add(new OptionHolder(flag, field, option));
                    } else if (flag.type() == Flag.TYPE_LONG) {
                        if (!field.getType().isAssignableFrom(Long.TYPE) && field.getType().isAssignableFrom(Long.class)) {
                            throw new IllegalArgumentException("Field "+field.toGenericString()+" is not the same type (Long) as defined in the Flag.");
                        }
                        OptionSpec<Long> option;
                        if (flag.required()) {
                            option = optionParser
                                    .accepts(name, description)
                                    .withRequiredArg().ofType(Long.class);
                        } else {
                            option = optionParser
                                    .accepts(name, description)
                                    .withOptionalArg().ofType(Long.class);
                        }
                        options.add(new OptionHolder(flag, field, option));
                    } else if (flag.type() == Flag.TYPE_BOOLEAN) {
                        if (!field.getType().isAssignableFrom(Boolean.TYPE) && field.getType().isAssignableFrom(Boolean.class)) {
                            throw new IllegalArgumentException("Field "+field.toGenericString()+" is not the same type (Boolean) as defined in the Flag.");
                        }
                        OptionSpec<Boolean> option;
                        if (flag.required()) {
                            option = optionParser
                                    .accepts(name, description)
                                    .withRequiredArg().ofType(Boolean.class);
                        } else {
                            option = optionParser
                                    .accepts(name, description)
                                    .withOptionalArg().ofType(Boolean.class);
                        }
                        options.add(new OptionHolder(flag, field, option));
                    }
                }
            }
        }
        return this;
    }

    /**
     * Try to set the arguments from main method on the fields loaded by loadOpts(Class<?> c).
     * 
     * @param args - Arguments passed from main method.
     * @return this
     */
    public Flags parse(String[] args) {
        final OptionSet optionSet = optionParser.parse(args);

        if (optionSet.has(HELP)) {
            printHelp();
            helpCalled = true;
            return this;
        }

        for (OptionHolder holder : options) {
            OptionSpec<?> optionSpec = holder.getOptionSpec();
            if (optionSet.has(optionSpec)) {
                //set the option provided
                try {
                    if (holder.getFlag().type() == Flag.TYPE_INTEGER) {
                        holder.getField().setInt(holder.getField().getClass(), (Integer) optionSet.valueOf(optionSpec));
                    } else if (holder.getFlag().type() == Flag.TYPE_LONG) {
                        holder.getField().setLong(holder.getField().getClass(), (Long) optionSet.valueOf(optionSpec));
                    } else if (holder.getFlag().type() == Flag.TYPE_STRING) {
                        holder.getField().set(holder.getField().getClass(), (String) optionSet.valueOf(optionSpec));
                    } else if (holder.getFlag().type() == Flag.TYPE_BOOLEAN) {
                        holder.getField().setBoolean(holder.getField().getClass(), (Boolean) optionSet.valueOf(optionSpec));
                    }
                } catch (IllegalArgumentException e) {
                    throw e;
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("Unable to set the value for field: "+holder.getField().toGenericString(), e);
                }
            } else if (holder.getFlag().required()) {
                //missing required parameter
                printHelp();
                throw new IllegalStateException("Required field missing: "+holder.getFlag().name());
            } else {
                //not provided, but optional... set the default value
                String defaultValue = holder.getFlag().defaultValue();
                try {
                    if (holder.getFlag().type() == Flag.TYPE_INTEGER) {
                        holder.getField().setInt(holder.getField().getClass(), Integer.parseInt(defaultValue));
                    } else if (holder.getFlag().type() == Flag.TYPE_LONG) {
                        holder.getField().setLong(holder.getField().getClass(), Long.parseLong(defaultValue));
                    } else if (holder.getFlag().type() == Flag.TYPE_STRING) {
                        holder.getField().set(holder.getField().getClass(), defaultValue);
                    } else if (holder.getFlag().type() == Flag.TYPE_BOOLEAN) {
                        holder.getField().setBoolean(holder.getField().getClass(), Boolean.getBoolean(defaultValue));
                    }
                } catch (IllegalArgumentException e) {
                    throw e;
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("Unable to set the value for field: "+holder.getField().toGenericString(), e);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException("Default value \""+defaultValue+"\"can not be casted to correct type.");
                }
            }
        }

        return this;
    }
    
    public void printHelp() {
        try {
            optionParser.printHelpOn(System.out);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to print help.", e);
        }
    }

    /**
     * Returns true if "--help" was one of the arguments.
     * This means that no values has been set when calling parse().
     * 
     * @return true if "help" was used, false if not.
     */
    public boolean helpCalled() {
        return helpCalled ;
    }

    /**
     * Holds an option«s corresponding Field, Flag and OptionSpec
     * 
     * @author acidmoose
     *
     */
    private class OptionHolder {
        private Flag flag;
        private Field field;
        private OptionSpec<?> optionSpec;

        public OptionHolder(Flag flag, Field field, OptionSpec<?> optionSpec) {
            this.flag = flag;
            this.field = field;
            this.optionSpec = optionSpec;
        }

        public Flag getFlag() {
            return flag;
        }

        public Field getField() {
            return field;
        }

        public OptionSpec<?> getOptionSpec() {
            return optionSpec;
        }
    }

    /**
     * Debugging method. Prints the Flags found and the corresponding Fields.
     */
    public void printFlags() {
        for (OptionHolder holder : options) {
            System.out.println("Field: "+holder.getField().toGenericString()+"\nFlag: name:"+holder.getFlag().name()
                    +", description:"+holder.getFlag().description()+", type:"+holder.getFlag().type()
                    +", default:"+holder.getFlag().defaultValue());
        }
    }

}
