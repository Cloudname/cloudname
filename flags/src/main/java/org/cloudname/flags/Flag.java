package org.cloudname.flags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation used to define a field that should be configurable
 * via command line arguments.
 * 
 * @author acidmoose
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Flag {
    
    /**
     * The name used in command line argument. e.g. "bar" in "java Foo --bar 1".
     * In this example, "bar" can not be defined twice as this could make
     * "--bar 1" set the wrong value.
     * @return
     */
    String name();
    
    /**
     * A description of the field. Visible when running "--help".
     * @return
     */
    String description() default "";
    
    /**
     * Defines if a field is required to be present in the String[] or not.
     * Overrides the default value.
     * @return
     */
    boolean required() default false;

    /**
     * Optional field that supports the use of enum.
     * Example using enum:
     *
     * public enum SimpleEnum {OPTION1, OPTION2};
     *
     * @Flag(name="option", options=SimpleEnum.class)
     * public static SimpleEnum option = SimpleEnum.OPTION1;
     *
     * @return
     */
    Class<? extends Enum<?>> options() default NoOption.class;
}
