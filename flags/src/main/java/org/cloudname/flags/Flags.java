package org.cloudname.flags;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * This class can load command line arguments based of Flag annotations.
 *
 * Fields must be static, and public, and defined as a String, Long, long, Integer,
 * int, Boolean or boolean.
 *
 * Typical use:
 *
 * @Flag(name="text", description="Output text")
 * public static String text = "DefaultString";
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
     * The supported field types. Determined in fieldTypeOf(Field field).
     *
     * @author acidmoose
     *
     */
    private enum FieldType {ENUM, STRING, INTEGER, LONG, BOOLEAN, UNKNOWN}

    //The option set builder.
    private final OptionParser optionParser = new OptionParser();

    // Help option
    private final OptionSpec<Void> HELP = optionParser
            .accepts("help", "Show this help");

    // Version option
    private final OptionSpec<Void> VERSION = optionParser
        .accepts("version", "Show version");

    private final OptionSpec<String> PROPERTIES_FILE = optionParser.accepts("properties-file",
        "Load properties from a given file").withRequiredArg().ofType(String.class).withValuesSeparatedBy(';');

    // Version text
    private String versionString = "NA";

    // Helper list for loaded options.
    private final Map<String, OptionHolder> options = new HashMap<String, OptionHolder>();

    // OptionSet used by option parser implementation
    private OptionSet optionSet;

    private List<String> nonOptionArguments;

    // Helper map to store enum options.
    private final Map<Class<? extends Enum<?>>, List<String>> enumOptions = new HashMap<Class<? extends Enum<?>>, List<String>>();


    /**
     * Load a class that contains Flag annotations.
     *
     * @param c - the class to load options from.
     * @return this
     */
    public Flags loadOpts(Class<?> c) {
        return loadOpts(c, false);
    }

    /**
     * Load an instanced class that contains Flag annotations.
     *
     * @param o - the class to load options from.
     * @return this
     */
    public Flags loadOpts(final Object o) {
        return loadOpts(o, true);
    }

    private Flags loadOpts(Object o, boolean instanced) {
        final Field[] declaredFields;
        Class<?> c = null;
        if (instanced) {
            declaredFields = o.getClass().getDeclaredFields();
        } else {
            c = ((Class<?>)o);
            declaredFields = c.getDeclaredFields();
        }

        for (Field field : declaredFields) {
            Flag flag = field.getAnnotation(Flag.class);
            // Check if we found a flag annotation for this field.
            if (null == flag) {
                continue;
            }

            // Flag fields must be static if you are initializing the flags through a Class instance.
            if ( ! instanced && ! Modifier.isStatic(field.getModifiers())) {
                throw new IllegalArgumentException("Field "+field.toGenericString()+" is not static. Flag fields " +
                    "must be static when initializing through a Class instance.");
            }

            String name = flag.name();
            String description = flag.description();

            // Determine the type of field
            FieldType type = fieldTypeOf(field, flag);

            switch (type) {

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
                if (instanced) {
                    addInstancedOption(type, flag, field, intOption, o);
                } else {
                    addOption(type, flag, field, intOption, c);
                }
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
                if (instanced) {
                    addInstancedOption(type, flag, field, stringOption, o);
                } else {
                    addOption(type, flag, field, stringOption, c);
                }
                break;

            case BOOLEAN:
                OptionSpec<Boolean> booleanOption;
                if (flag.required()) {
                    booleanOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(Boolean.class);
                } else {
                    booleanOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(Boolean.class);
                }
                if (instanced) {
                    addInstancedOption(type, flag, field, booleanOption, o);
                } else {
                    addOption(type, flag, field, booleanOption, c);
                }
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
                if (instanced) {
                    addInstancedOption(type, flag, field, longOption, o);
                } else {
                    addOption(type, flag, field, longOption, c);
                }
                break;

            case ENUM:
                Class<? extends Enum<?>> enumClass = flag.options();
                Object[] enumConstants = enumClass.getEnumConstants();
                if (enumConstants == null) {
                    throw new IllegalArgumentException("Field "+field.toGenericString()+" is not an enum type.");
                }
                for (Object object : enumConstants) {
                    addEnumOption(enumClass, object.toString());
                }
                OptionSpec<?> enumOption;
                if (flag.required()) {
                    enumOption = optionParser
                            .accepts(name, description)
                            .withRequiredArg()
                            .ofType(enumClass);
                } else {
                    enumOption = optionParser
                            .accepts(name, description)
                            .withOptionalArg()
                            .ofType(enumClass);
                }
                if (instanced) {
                    addInstancedOption(type, flag, field, enumOption, o);
                } else {
                    addOption(type, flag, field, enumOption, c);
                }
                break;

            case UNKNOWN:
            default:
                throw new IllegalArgumentException("Field "+field.toGenericString()+" is not of a supported type.");
            }
        }
        return this;
    }

    /**
     * Set the string to show when "--version" is used.
     * @param versionString
     */
    public void setVersionString(final String versionString) {
        this.versionString = versionString;
    }

    /**
     * Returns all arguments given to parse() that are not Flagged arguments.
     * @return List<String> - list of all arguments given to parse() that are not Flagged arguments.
     */
    public List<String> getNonOptionArguments() {
        return nonOptionArguments;
    }

    /**
     * If a field is found to be of type ENUM, this method is used to store
     * valid input for that specific flagged option.
     * @param enumClass
     * @param validOption
     */
    private void addEnumOption(Class<? extends Enum<?>> enumClass, String validOption) {
        List<String> optionsForClass = enumOptions.get(enumClass);
        if (optionsForClass == null) {
            optionsForClass = new ArrayList<String>();
        }
        optionsForClass.add(validOption);
        enumOptions.put(enumClass, optionsForClass);
    }

    /**
     * Private helper method to add an option. Will check that an option
     * with the same name has not previously been added.
     *
     * @param type
     * @param flag
     * @param field
     * @param option
     * @param c
     * @throws IllegalArgumentException
     */
    private void addOption(FieldType type, Flag flag, Field field, OptionSpec<?> option, Class<?> c)
        throws IllegalArgumentException {
        if (options.containsKey(flag.name())) {
            throw new IllegalArgumentException("Flag named "+flag.name()+" is defined more than once.");
        }
        options.put(flag.name(), new OptionHolder(type, flag, field, option, c));
    }

    /**
     * Private helper method to add an instanced option. Will check that an option
     * with the same name has not previously been added.
     *
     * @param type
     * @param flag
     * @param field
     * @param option
     * @param c
     * @throws IllegalArgumentException
     */
    private void addInstancedOption(FieldType type, Flag flag, Field field, OptionSpec<?> option, Object c)
        throws IllegalArgumentException {
        if (options.containsKey(flag.name())) {
            throw new IllegalArgumentException("Flag named "+flag.name()+" is defined more than once.");
        }
        options.put(flag.name(), new OptionHolder(type, flag, field, option, c));
    }

    /**
     * Try to set the arguments from main method on the fields loaded by loadOpts(Class<?> c).
     *
     * @param args - Arguments passed from main method.
     * @return this
     */
    public Flags parse(String[] args) {
        optionSet = optionParser.parse(args);

        //Store non option arguments
        nonOptionArguments = optionSet.nonOptionArguments();
        if (nonOptionArguments == null) {
            nonOptionArguments = new ArrayList<String>();
        }

        //do not parse options if "help" is a part of the arguments given
        if (helpFlagged()) {
            return this;
        }
        if (versionFlagged()) {
            return this;
        }
        if (propertiesFlagged()) {
            List<String> files = optionSet.valuesOf(PROPERTIES_FILE);
            ArrayList<String> newArgs = new ArrayList<String>();
            for (String filename : files) {
                final Properties props = new Properties();
                try {
                    final FileInputStream stream = new FileInputStream(filename);
                    props.load(stream);
                    for (Enumeration<?> keys = props.propertyNames(); keys.hasMoreElements();) {
                        String flagName = (String) keys.nextElement();
                        if (!options.containsKey(flagName) || optionSet.hasArgument(flagName)) {
                            //Properties contains something not in options or is already set by commandline argument
                            //Command line argument takes precedence over properties file
                            continue;
                        }
                        newArgs.add("--" + flagName);
                        newArgs.add(props.getProperty(flagName));
                    }

                    stream.close();
                } catch (IOException e) {
                    throw new RuntimeException("Could not parse property-file", e);
                }
            }
            Collections.addAll(newArgs, args);
            optionSet = optionParser.parse(newArgs.toArray(new String[newArgs.size()]));
        }

        for (OptionHolder holder : options.values()) {
            try {
                OptionSpec<?> optionSpec = holder.getOptionSpec();

                // Deal with the flags that were given on the command line.
                if (optionSet.has(optionSpec)) {
                    switch(holder.getType()) {
                    case INTEGER:
                        if (holder.isInstanced()) {
                            holder.getField().set(holder.getObjectSource(), optionSet.valueOf(optionSpec));
                        } else {
                            holder.getField().set(holder.getField().getClass(), optionSet.valueOf(optionSpec));
                        }
                        break;

                    case LONG:
                        if (holder.isInstanced()) {
                            holder.getField().set(holder.getObjectSource(), optionSet.valueOf(optionSpec));
                        } else {
                            holder.getField().set(holder.getField().getClass(), optionSet.valueOf(optionSpec));
                        }
                        break;

                    case STRING:
                        if (holder.isInstanced()) {
                            holder.getField().set(holder.getObjectSource(), optionSet.valueOf(optionSpec));
                        } else {
                            holder.getField().set(holder.getField().getClass(), optionSet.valueOf(optionSpec));
                        }
                        break;

                    case BOOLEAN:
                        Object value = optionSet.valueOf(optionSpec);
                        if (holder.isInstanced()) {
                            holder.getField().set(holder.getObjectSource(),
                                (value == null) ? true : value);
                        } else {
                            holder.getField().set(holder.getField().getClass(),
                                (value == null) ? true : value);
                        }
                        break;

                    case ENUM:
                        if (holder.isInstanced()) {
                            try {
                                holder.getField().set(holder.getObjectSource(), optionSet.valueOf(optionSpec));
                            } catch (Exception e) {
                                throw new IllegalArgumentException("Option given is not a valid option. Valid options are: "+enumOptions.get(holder.flag.options()).toString()+".");
                            }
                        } else {
                            try {
                                holder.getField().set(holder.getField().getClass(), optionSet.valueOf(optionSpec));
                            } catch (Exception e) {
                                throw new IllegalArgumentException("Option given is not a valid option. Valid options are: "+enumOptions.get(holder.flag.options()).toString()+".");
                            }
                        }
                        break;
                    }

                    // No further action needed for this field.
                    continue;
                }

                // Check if flag that does not occur in command line was required.
                if (holder.getFlag().required()) {
                    throw new IllegalArgumentException("Required argument missing: " + holder.getFlag().name());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Programming error, illegal access for " + holder.getField().toGenericString());
            }
        }
        return this;
    }

    /**
     * Prints the help to the specified output stream.
     *
     * @param out the OutputStream we wish to print the help output to.
     */
    public void printHelp(OutputStream out) {
        PrintWriter w = new PrintWriter(out);

        Map<String, List<OptionHolder>> holdersByClass = new TreeMap<String, List<OptionHolder>>();

        // Iterate over all the options we have gathered and stash them by class.
        for (OptionHolder holder : options.values()) {
            // Fetch list corresponding to source class name
            final String className;
            if (holder.isInstanced()) {
                className = holder.getObjectSource().getClass().getName();
            } else {
                className = holder.getClassSource().getName();
            }
            List<OptionHolder> holderList = holdersByClass.get(className);
            if (null == holderList) {
                // The list did not exist.  Create it.
                holderList = new LinkedList<OptionHolder>();
                holdersByClass.put(className, holderList);
            }

            holderList.add(holder);
        }

        // Output options by class
        for (Map.Entry<String, List<OptionHolder>> ent : holdersByClass.entrySet()) {
            String className = ent.getKey();
            List<OptionHolder> holderList = ent.getValue();

            // Sort the options. In Java, sorting collections is worse
            // than watching Pandas fuck.
            Collections.sort(holderList, new Comparator<OptionHolder>() {
                @Override
                public int compare(OptionHolder a, OptionHolder b) {
                    return a.getFlag().name().toLowerCase().compareTo(b.getFlag().name().toLowerCase());
                }
            });

            StringBuffer buff = new StringBuffer();

            buff.append("\n\n")
            .append(className)
            .append("\n")
            .append("------------------------------------------------------------------------")
            .append("\n");

            for (OptionHolder holder : holderList) {
                // Mark required flags with a "*"
                buff.append(holder.getFlag().required() ? "* " : "  ");

                String s;
                try {
                    s = "  --" + holder.getFlag().name() + " <" + holder.getType() + "> default: "
                            + (holder.isInstanced()
                                ? holder.getField().get(holder.getObjectSource())
                                : holder.getField().get(holder.getClassSource()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                //TODO: handle enum options
                if (holder.getFlag().options() != NoOption.class) {
                    s = s + " options: "+enumOptions.get(holder.getFlag().options()).toString();
                }

                // Avert your eyes.
                int spaces = 50 - s.length();
                spaces = spaces < 0 ? 0 : spaces;
                buff.append(s)
                .append("  . . . . . . . . . . . . . . . . . . . . . . . . ".substring(0, spaces))
                .append("| " + holder.getFlag().description())
                .append("\n");
            }
            w.println(buff.toString());
        }
        w.flush();
    }

    /**
     * Prints the version to the specified output stream.
     *
     * @param out the OutputStream we wish to print the version output to.
     */
    public void printVersion(final OutputStream out) {
        final PrintWriter w = new PrintWriter(out);
        w.println(versionString);
        w.flush();
    }

    /**
     * @return {@code true} if a "--help" flag was passed on the command line.
     */
    public boolean helpFlagged() {
        return optionSet.has(HELP);
    }

    /**
     * @return {@code true} if a "--version" flag was passed on the command line.
     */
    public boolean versionFlagged() {
        return optionSet.has(VERSION);
    }

    /**
     *
     * @return {@code true} if a "--properties-file" flag was passed on the command line.
     */
    public boolean propertiesFlagged() {
        return optionSet.hasArgument(PROPERTIES_FILE);
    }

    /**
     * Debugging method. Prints the Flags found and the corresponding Fields.
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void printFlags() {
        try {
            for (OptionHolder holder : options.values()) {
                System.out.println("Field: "+holder.getField().toGenericString()+"\nFlag: name:"+holder.getFlag().name()
                        +", description:"+holder.getFlag().description()+", type:"+holder.getType()
                        +", default:"+(holder.isInstanced()
                            ? holder.getField().get(holder.getObjectSource())
                            : holder.getField().get(holder.getClassSource())));
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the field type of a Field instance.
     *
     * @param field the field instance we want the type for.
     * @return the type of the {@code field} in question.
     */
    private static FieldType fieldTypeOf(Field field, Flag flag) {
        if (field.getType().isAssignableFrom(Long.TYPE)
                || field.getType().isAssignableFrom(Long.class)) {
            return FieldType.LONG;
        }

        if (field.getType().isAssignableFrom(Boolean.TYPE)
                || field.getType().isAssignableFrom(Boolean.class)) {
            return FieldType.BOOLEAN;
        }

        if (field.getType().isAssignableFrom(String.class)) {
            return FieldType.STRING;
        }

        if (field.getType().isAssignableFrom(Integer.TYPE)
                || field.getType().isAssignableFrom(Integer.class)) {
            return FieldType.INTEGER;
        }

        if (flag.options() != NoOption.class
                && field.getType().isAssignableFrom(flag.options())) {
            return FieldType.ENUM;
        }

        return FieldType.UNKNOWN;
    }

    /**
     * Get all Flag instances parsed from class (via the loadOpts(Class<?> c) method) as a List.
     * @return List containing Flag instances.
     */
    public List<Flag> getFlagsAsList() {
        final List<Flag> list = new ArrayList<Flag>();
        for(OptionHolder holder : options.values()) {
            list.add(holder.getFlag());
        }
        return list;
    }

    /**
     * Internal class that holds an option's corresponding FieldType, Field, Flag and OptionSpec.
     *
     * @author acidmoose
     *
     */
    private static class OptionHolder {
        private final Flag flag;
        private final Field field;
        private final OptionSpec<?> optionSpec;
        private final FieldType type;
        private final Class<?> classSource;
        private final Object objectSource;

        public OptionHolder(FieldType type, Flag flag, Field field, OptionSpec<?> optionSpec, Class<?> classSource) {
            this.type = type;
            this.flag = flag;
            this.field = field;
            this.optionSpec = optionSpec;
            this.classSource = classSource;
            objectSource = null;
        }

        public OptionHolder(FieldType type, Flag flag, Field field, OptionSpec<?> optionSpec, Object objectSource) {
            this.type = type;
            this.flag = flag;
            this.field = field;
            this.optionSpec = optionSpec;
            this.objectSource = objectSource;
            classSource = null;
        }

        public boolean isInstanced() {
            return objectSource != null;
        }

        public Flag getFlag() {
            return flag;
        }

        public Field getField() {
            // To support private variables we simply make the field accessible.
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            return field;
        }

        public OptionSpec<?> getOptionSpec() {
            return optionSpec;
        }

        public FieldType getType() {
            return type;
        }

        public Class<?> getClassSource() {
            return classSource;
        }

        public Object getObjectSource() {
            return objectSource;
        }
    }

}
