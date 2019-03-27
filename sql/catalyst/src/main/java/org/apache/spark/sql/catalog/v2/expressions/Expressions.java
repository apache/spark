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
  public Transform apply(String name, Expression[] args) {
    return LogicalExpressions.apply(name, args);
  }

  /**
   * Create a named reference expression for a column.
   *
   * @param name a column name
   * @return a named reference for the column
   */
  public NamedReference column(String name) {
    return LogicalExpressions.reference(name);
  }

  /**
   * Create a literal from a value.
   *
   * @param value a value
   * @param <T> the JVM type of the value
   * @return a literal expression for the value
   */
  public <T> Literal<T> literal(T value) {
    return LogicalExpressions.literal(value);
  }

  /**
   * Create a bucket transform for one or more columns.
   * <p>
   * This transform represents a logical mapping from a value to a bucket id in [0, numBuckets)
   * based on a hash of the value.
   *
   * @param numBuckets the number of output buckets
   * @param columns input columns for the bucket transform
   * @return a logical bucket transform
   */
  public Transform bucket(int numBuckets, String[] columns) {
    return LogicalExpressions.bucket(numBuckets, columns);
  }

  /**
   * Create an identity transform for a column.
   * <p>
   * This transform represents a logical mapping from a value to itself.
   *
   * @param column an input column
   * @return a logical identity transform
   */
  public Transform identity(String column) {
    return LogicalExpressions.identity(column);
  }

  /**
   * Create a year transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a year, such as 2018.
   *
   * @param column an input timestamp or date column
   * @return a logical year transform
   */
  public Transform year(String column) {
    return LogicalExpressions.year(column);
  }

  /**
   * Create a month transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a month, such as
   * 2018-05.
   *
   * @param column an input timestamp or date column
   * @return a logical month transform
   */
  public Transform month(String column) {
    return LogicalExpressions.month(column);
  }

  /**
   * Create a date transform for a timestamp or date column.
   * <p>
   * This transform represents a logical mapping from a timestamp or date to a date, such as
   * 2018-05-13.
   *
   * @param column an input timestamp or date column
   * @return a logical date transform
   */
  public Transform date(String column) {
    return LogicalExpressions.date(column);
  }

  /**
   * Create a date and hour transform for a timestamp column.
   * <p>
   * This transform represents a logical mapping from a timestamp to an hour, such as 2018-05-13,
   * hour 11.
   *
   * @param column an input timestamp column
   * @return a logical date and hour transform
   */
  public Transform dateHour(String column) {
    return LogicalExpressions.dateHour(column);
  }

}
