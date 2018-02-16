package com.dakuupa.pulsar.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
/**
 * Annotation to require unique value in table
 * @author EWilliams
 *
 */
public @interface DbUnique {

	
}
