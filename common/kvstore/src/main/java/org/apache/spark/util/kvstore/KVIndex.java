/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.kvstore;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.spark.annotation.Private;

/**
 * Tags a field to be indexed when storing an object.
 *
 * <p>
 * Types are required to have a natural index that uniquely identifies instances in the store.
 * The default value of the annotation identifies the natural index for the type.
 * </p>
 *
 * <p>
 * Indexes allow for more efficient sorting of data read from the store. By annotating a field or
 * "getter" method with this annotation, an index will be created that will provide sorting based on
 * the string value of that field.
 * </p>
 *
 * <p>
 * Note that creating indices means more space will be needed, and maintenance operations like
 * updating or deleting a value will become more expensive.
 * </p>
 *
 * <p>
 * Indices are restricted to String, integral types (byte, short, int, long, boolean), and arrays
 * of those values.
 * </p>
 */
@Private
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface KVIndex {

  String NATURAL_INDEX_NAME = "__main__";

  /**
   * The name of the index to be created for the annotated entity. Must be unique within
   * the class. Index names are not allowed to start with an underscore (that's reserved for
   * internal use). The default value is the natural index name (which is always a copy index
   * regardless of the annotation's values).
   */
  String value() default NATURAL_INDEX_NAME;

  /**
   * The name of the parent index of this index. By default there is no parent index, so the
   * generated data can be retrieved without having to provide a parent value.
   *
   * <p>
   * If a parent index is defined, iterating over the data using the index will require providing
   * a single value for the parent index. This serves as a rudimentary way to provide relationships
   * between entities in the store.
   * </p>
   */
  String parent() default "";

  /**
   * Whether to copy the instance's data to the index, instead of just storing a pointer to the
   * data. The default behavior is to just store a reference; that saves disk space but is slower
   * to read, since there's a level of indirection.
   */
  boolean copy() default false;

}
