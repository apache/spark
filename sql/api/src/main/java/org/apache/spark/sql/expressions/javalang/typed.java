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

package org.apache.spark.sql.expressions.javalang;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.internal.TypedAverage;
import org.apache.spark.sql.internal.TypedCount;
import org.apache.spark.sql.internal.TypedSumDouble;
import org.apache.spark.sql.internal.TypedSumLong;

/**
 * Type-safe functions available for {@link org.apache.spark.sql.Dataset} operations in Java.
 *
 * Scala users should use {@link org.apache.spark.sql.expressions.scalalang.typed}.
 *
 * @since 2.0.0
 * @deprecated As of release 3.0.0, please use the untyped builtin aggregate functions.
 */
@Deprecated(since = "3.0.0")
public class typed {
  // Note: make sure to keep in sync with typed.scala

  /**
   * Average aggregate function.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Double> avg(MapFunction<T, Double> f) {
    return new TypedAverage<T>(f).toColumnJava();
  }

  /**
   * Count aggregate function.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Long> count(MapFunction<T, Object> f) {
    return new TypedCount<T>(f).toColumnJava();
  }

  /**
   * Sum aggregate function for floating point (double) type.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Double> sum(MapFunction<T, Double> f) {
    return new TypedSumDouble<T>(f).toColumnJava();
  }

  /**
   * Sum aggregate function for integral (long, i.e. 64 bit integer) type.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Long> sumLong(MapFunction<T, Long> f) {
    return new TypedSumLong<T>(f).toColumnJava();
  }
}
