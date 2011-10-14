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
 * This class can load command line arguments based of Flag annotations.
 * 
 * Fields must be static, and defined as a String, Long, long, Integer,
 * int, Boolean or boolean.
 * 
 * Typical use:
 * 
 * @Flag(name="text", defaultValue="N/A", description="Output text")
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
     * The supported field types. Determined in determinType(Field field).
     * 
     * @author acidmoose
     *
     */
    public enum FieldType {STRING, INTEGER, LONG, BOOLEAN, UNKNOWN}
    
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

            //check for flag
            if (flag == null)
                continue;

            String name = flag.name();
            //check to see that we can set the field's value
            if (!Modifier.isStatic(field.getModifiers()))
                throw new IllegalStateException("Field "+field.toGenericString()+" is not static and cannot be modified.");
            
            String description = flag.description();

            //determine the type of field
            FieldType type = determinType(field);
            
            switch (type) {
            
            case UNKNOWN:
                throw new IllegalArgumentException("Field "+field.toGenericString()+" is not of a supported type.");

            case INTEGER:
                OptionSpec<Integer> intOption;
                if (flag.required()) {
                    intOption = optionParser
                            .accepts(name, description)
                            .withRequiredArg()
                            .ofType(Integer.class);
                } else {
                    intOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(Integer.class);
                }
                options.add(new OptionHolder(type, flag, field, intOption));
                break;

            case STRING:
                OptionSpec<String> stringOption;
                if (flag.required()) {
                    stringOption = optionParser
                            .accepts(name, description)
                            .withRequiredArg()
                            .ofType(String.class);
                } else {
                    stringOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(String.class);
                }
                options.add(new OptionHolder(type, flag, field, stringOption));
                break;

            case BOOLEAN:
                OptionSpec<Boolean> booleanOption;
                if (flag.required()) {
                    booleanOption = optionParser
                            .accepts(name, description)
                            .withRequiredArg()
                            .ofType(Boolean.class);
                } else {
                    booleanOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(Boolean.class);
                }
                options.add(new OptionHolder(type, flag, field, booleanOption));
                break;

            case LONG:
                OptionSpec<Long> longOption;
                if (flag.required()) {
                    longOption = optionParser
                            .accepts(name, description)
                            .withRequiredArg()
                            .ofType(Long.class);
                } else {
                    longOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(Long.class);
                }
                options.add(new OptionHolder(type, flag, field, longOption));
                break;
                
            default:
                break;
            }
        }
        return this;
    }

    /**
     * Determine the type of the field.
     * 
     * @param field
     * @return
     */
    private FieldType determinType(Field field) {
        if (field.getType().isAssignableFrom(Long.TYPE) ||
                field.getType().isAssignableFrom(Long.class))
            return FieldType.LONG;
        
        if (field.getType().isAssignableFrom(Boolean.TYPE) ||
                field.getType().isAssignableFrom(Boolean.class))
            return FieldType.BOOLEAN;
        
        if (field.getType().isAssignableFrom(String.class))
            return FieldType.STRING;
        
        if (field.getType().isAssignableFrom(Integer.TYPE) ||
                field.getType().isAssignableFrom(Integer.class))
            return FieldType.INTEGER;
        
        return FieldType.UNKNOWN;
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
                    if (holder.getType() == FieldType.INTEGER) {
                        holder.getField().set(holder.getField().getClass(), (Integer) optionSet.valueOf(optionSpec));
                    } else if (holder.getType() == FieldType.LONG) {
                        holder.getField().set(holder.getField().getClass(), (Long) optionSet.valueOf(optionSpec));
                    } else if (holder.getType() == FieldType.STRING) {
                        holder.getField().set(holder.getField().getClass(), (String) optionSet.valueOf(optionSpec));
                    } else if (holder.getType() == FieldType.BOOLEAN) {
                        holder.getField().set(holder.getField().getClass(), (Boolean) optionSet.valueOf(optionSpec));
                    }
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Unable to set the value for field: "+holder.getField().toGenericString(), e);
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
                    if (holder.getType() == FieldType.INTEGER) {
                        holder.getField().set(holder.getField().getClass(), Integer.parseInt(defaultValue));
                    } else if (holder.getType() == FieldType.LONG) {
                        holder.getField().set(holder.getField().getClass(), Long.parseLong(defaultValue));
                    } else if (holder.getType() == FieldType.STRING) {
                        holder.getField().set(holder.getField().getClass(), defaultValue);
                    } else if (holder.getType() == FieldType.BOOLEAN) {
                        holder.getField().set(holder.getField().getClass(), Boolean.getBoolean(defaultValue));
                    }
                } catch (IllegalArgumentException e) {
                    throw e;
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("Unable to set the value for field: "+holder.getField().toGenericString(), e);
                }
            }
        }

        return this;
    }

    /**
     * Prints the help.
     */
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
     * Debugging method. Prints the Flags found and the corresponding Fields.
     */
    public void printFlags() {
        for (OptionHolder holder : options) {
            System.out.println("Field: "+holder.getField().toGenericString()+"\nFlag: name:"+holder.getFlag().name()
                    +", description:"+holder.getFlag().description()+", type:"+holder.getType()
                    +", default:"+holder.getFlag().defaultValue());
        }
    }
    
    /**
     * Internal class that holds an option's corresponding FieldType, Field, Flag and OptionSpec.
     * 
     * @author acidmoose
     *
     */
    private static class OptionHolder {
        private Flag flag;
        private Field field;
        private OptionSpec<?> optionSpec;
        private final FieldType type;

        public OptionHolder(FieldType type, Flag flag, Field field, OptionSpec<?> optionSpec) {
            this.type = type;
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

        public FieldType getType() {
            return type;
        }
    }
}
