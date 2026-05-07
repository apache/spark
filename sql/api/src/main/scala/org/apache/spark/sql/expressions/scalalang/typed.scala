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

package org.apache.spark.sql.expressions.scalalang

import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.internal.{TypedAverage, TypedCount, TypedSumDouble, TypedSumLong}

/**
 * Type-safe functions available for `Dataset` operations in Scala.
 *
 * Java users should use [[org.apache.spark.sql.expressions.javalang.typed]].
 *
 * @since 2.0.0
 */
@deprecated("please use untyped builtin aggregate functions.", "3.0.0")
// scalastyle:off
object typed {
  // scalastyle:on

  // Note: whenever we update this file, we should update the corresponding Java version too.
  // The reason we have separate files for Java and Scala is because in the Scala version, we can
  // use tighter types (primitive types) for return types, whereas in the Java version we can only
  // use boxed primitive types.
  // For example, avg in the Scala version returns Scala primitive Double, whose bytecode
  // signature is just a java.lang.Object; avg in the Java version returns java.lang.Double.

  /**
   * Average aggregate function.
   *
   * @since 2.0.0
   */
  def avg[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedAverage(f).toColumn

  /**
   * Count aggregate function.
   *
   * @since 2.0.0
   */
  def count[IN](f: IN => Any): TypedColumn[IN, Long] = new TypedCount(f).toColumn

  /**
   * Sum aggregate function for floating point (double) type.
   *
   * @since 2.0.0
   */
  def sum[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedSumDouble[IN](f).toColumn

  /**
   * Sum aggregate function for integral (long, i.e. 64 bit integer) type.
   *
   * @since 2.0.0
   */
  def sumLong[IN](f: IN => Long): TypedColumn[IN, Long] = new TypedSumLong[IN](f).toColumn
}
