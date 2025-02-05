package org.apache.spark.annotation;

import java.lang.annotation.*;

/**
 * Method annotation to indicate that this method is only available in the Classic implementation
 * of the Scala/Java SQL API.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ClassicOnly { }
