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
     * Use if type of Field is String.
     */
    static int TYPE_STRING = 0;
    
    /**
     * Use if type of Field is Integer or int.
     */
    static int TYPE_INTEGER = 1;
    
    /**
     * Use if type of Field is Long or long.
     */
    static int TYPE_LONG = 2;
    
    /**
     * Use if type of Field is Boolean or boolean.
     */
    static int TYPE_BOOLEAN = 3;
    
    /**
     * The type of a field. Use TYPE_STRING, TYPE_INTEGER, TYPE_LONG or TYPE_BOOLEAN.
     * @return
     */
    int type();
    
    /**
     * The name used in command line argument. e.g. "bar" in "java Foo --bar 1".
     * @return
     */
    String name();
    
    /**
     * The default value of the field, if it is not given as a command line argument.
     * @return
     */
    String defaultValue();
    
    /**
     * A description of the field. Visible when running "--help".
     * @return
     */
    String description() default "";
    
    /**
     * Defines if a field is required or not.
     * @return
     */
    boolean required() default false;
}
