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

package org.apache.spark.sql.catalog.v2.expressions;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.types.DataType;
import scala.collection.JavaConverters;

import java.util.Arrays;

/**
 * Helper methods to create logical transforms to pass into Spark.
 */
@Experimental
public class Expressions {
  private Expressions() {
  }

  /**
   * Create a logical transform for applying a named transform.
   * <p>
   * This transform can represent applying any named transform.
   *
   * @param name the transform name
   * @param args expression arguments to the transform
   * @return a logical transform
   */
  public static Transform apply(String name, Expression... args) {
    return LogicalExpressions.apply(name,
        JavaConverters.asScalaBuffer(Arrays.asList(args)).toSeq());
  }

  /**
   * Create a named reference expression for a column.
   *
   * @param name a column name
   * @return a named reference for the column
   */
  public static NamedReference column(String name) {
    return LogicalExpressions.reference(name);
  }

  /**
   * Create a literal from a value.
   * <p>
   * The JVM type of the value held by a literal must be the type used by Spark's InternalRow API
   * for the literal's {@link DataType SQL data type}.
   *
   * @param value a value
   * @param <T> the JVM type of the value
   * @return a literal expression for the value
   */
  public static <T> Literal<T> literal(T value) {
    return LogicalExpressions.literal(value);
  }

  /**
   * Create a bucket transform for one or more columns.
   * <p>
   * This transform represents a logical mapping from a value to a bucket id in [0, numBuckets)
   * based on a hash of the value.
   * <p>
   * The name reported by transforms created with this method is "bucket".
   *
   * @param numBuckets the number of output buckets
   * @param columns input columns for the bucket transform
   * @return a logical bucket transform with name "bucket"
   */
  public static Transform bucket(int numBuckets, String... columns) {
    return LogicalExpressions.bucket(numBuckets,
        JavaConverters.asScalaBuffer(Arrays.asList(columns)).toSeq());
  }

  /**
   * Create an identity transform for a column.
   * <p>
   * This transform represents a logical mapping from a value to itself.
   * <p>
   * The name reported by transforms created with this method is "identity".
   *
   * @param column an input column
   * @return a logical identity transform with name "identity"
   */
  public static Transform identity(String column) {
    return LogicalExpressions.identity(column);
  }

  /**
   * Create a yearly transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a year, such as 2018.
   * <p>
   * The name reported by transforms created with this method is "years".
   *
   * @param column an input timestamp or date column
   * @return a logical yearly transform with name "years"
   */
  public static Transform years(String column) {
    return LogicalExpressions.years(column);
  }

  /**
   * Create a monthly transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a month, such as
   * 2018-05.
   * <p>
   * The name reported by transforms created with this method is "months".
   *
   * @param column an input timestamp or date column
   * @return a logical monthly transform with name "months"
   */
  public static Transform months(String column) {
    return LogicalExpressions.months(column);
  }

  /**
   * Create a daily transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a date, such as
   * 2018-05-13.
   * <p>
   * The name reported by transforms created with this method is "days".
   *
   * @param column an input timestamp or date column
   * @return a logical daily transform with name "days"
   */
  public static Transform days(String column) {
    return LogicalExpressions.days(column);
  }

  /**
   * Create an hourly transform for a timestamp column.
   * <p>
   * This transform represents a logical mapping from a timestamp to a date and hour, such as
   * 2018-05-13, hour 19.
   * <p>
   * The name reported by transforms created with this method is "hours".
   *
   * @param column an input timestamp column
   * @return a logical hourly transform with name "hours"
   */
  public static Transform hours(String column) {
    return LogicalExpressions.hours(column);
  }

}
