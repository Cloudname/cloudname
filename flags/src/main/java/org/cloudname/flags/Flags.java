package org.cloudname.flags;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    private enum FieldType {STRING, INTEGER, LONG, BOOLEAN, UNKNOWN}

    //The option set builder.
    private final static OptionParser optionParser = new OptionParser();

    // Help option
    private final static OptionSpec<Void> HELP = optionParser
            .accepts("help", "Show this help");

    // Helper list for loaded options.
    private List<OptionHolder> options = new ArrayList<OptionHolder>();

    // OptionSet used by option parser implementation
    private OptionSet optionSet;

    /**
     * Load a class that contains Flag annotations.
     *
     * @param c - the class to load options from.
     * @return this
     */
    public Flags loadOpts(Class<?> c) {
        for (Field field : c.getFields()) {
            Flag flag = field.getAnnotation(Flag.class);

            // Check if we found a flag annotation for this field.
            if (null == flag) {
                continue;
            }

            // Make sure the field is static.  If the field is
            // nonstatic it makes no sense to use it for flags.
            if (! Modifier.isStatic(field.getModifiers())) {
                throw new IllegalArgumentException("Field "+field.toGenericString()+" is not static. Flag fields must be static");
            }

            String name = flag.name();
            String description = flag.description();

            // Determine the type of field
            FieldType type = fieldTypeOf(field);

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
                options.add(new OptionHolder(type, flag, field, intOption, c));
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
                options.add(new OptionHolder(type, flag, field, stringOption, c));
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
                options.add(new OptionHolder(type, flag, field, booleanOption, c));
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
                options.add(new OptionHolder(type, flag, field, longOption, c));
                break;

            case UNKNOWN:
            default:
                throw new IllegalArgumentException("Field "+field.toGenericString()+" is not of a supported type.");
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
        optionSet = optionParser.parse(args);

        for (OptionHolder holder : options) {
            try {
                OptionSpec<?> optionSpec = holder.getOptionSpec();

                // Deal with the flags that were given on the command line.
                if (optionSet.has(optionSpec)) {
                    switch(holder.getType()) {
                    case INTEGER:
                        holder.getField().set(holder.getField().getClass(), (Integer) optionSet.valueOf(optionSpec));
                        break;

                    case LONG:
                        holder.getField().set(holder.getField().getClass(), (Long) optionSet.valueOf(optionSpec));
                        break;

                    case STRING:
                        holder.getField().set(holder.getField().getClass(), (String) optionSet.valueOf(optionSpec));
                        break;

                    case BOOLEAN:
                        holder.getField().set(holder.getField().getClass(), (Boolean) optionSet.valueOf(optionSpec));
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
        for (OptionHolder holder : options) {
            // Fetch list corresponding to source class name
            String className = holder.getSource().getName();
            List holderList = holdersByClass.get(className);
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

                String s = "  --" + holder.getFlag().name() + "=<" + holder.getType() + ">";

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
     * @return {@code true} if a "--help" flag was passed on the command line.
     */
    public boolean helpFlagged() {
        return optionSet.has(HELP);
    }

    /**
     * Debugging method. Prints the Flags found and the corresponding Fields.
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
    public void printFlags() {
        try {
            for (OptionHolder holder : options) {
                System.out.println("Field: "+holder.getField().toGenericString()+"\nFlag: name:"+holder.getFlag().name()
                        +", description:"+holder.getFlag().description()+", type:"+holder.getType()
                        +", default:"+holder.getField().get(holder.getSource()));
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
    private static FieldType fieldTypeOf(Field field) {
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

        return FieldType.UNKNOWN;
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
        private Class source;

        public OptionHolder(FieldType type, Flag flag, Field field, OptionSpec<?> optionSpec, Class source) {
            this.type = type;
            this.flag = flag;
            this.field = field;
            this.optionSpec = optionSpec;
            this.source = source;
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

        public Class getSource() {
            return source;
        }
    }
}
