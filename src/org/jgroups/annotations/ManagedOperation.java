package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a method in an MBean class defines an MBean
 * operation. @ManagedOperation annotation can be applied to a
 * public method of a public class that is itself optionally
 * annotated with an @MBean annotation, or inherits such an
 * annotation from a superclass.
 *
 * @author Chris Mills
 */

// TODO: 17/7/4 by zmyer
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface ManagedOperation {
    String description() default "";
}
