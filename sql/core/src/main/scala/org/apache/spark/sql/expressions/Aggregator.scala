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

package org.apache.spark.sql.expressions

import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, TypedColumn}

/**
 * A base class for user-defined aggregations, which can be used in [[DataFrame]] and [[Dataset]]
 * operations to take all of the elements of a group and reduce them to a single value.
 *
 * For example, the following aggregator extracts an `int` from a specific class and adds them up:
 * {{{
 *   case class Data(i: Int)
 *
 *   val customSummer =  new Aggregator[Data, Int, Int] {
 *     def zero: Int = 0
 *     def reduce(b: Int, a: Data): Int = b + a.i
 *     def merge(b1: Int, b2: Int): Int = b1 + b2
 *     def finish(r: Int): Int = r
 *   }.toColumn()
 *
 *   val ds: Dataset[Data] = ...
 *   val aggregated = ds.select(customSummer)
 * }}}
 *
 * Based loosely on Aggregator from Algebird: https://github.com/twitter/algebird
 *
 * @tparam I The input type for the aggregation.
 * @tparam B The type of the intermediate value of the reduction.
 * @tparam O The type of the final output result.
 *
 * @since 1.6.0
 */
abstract class Aggregator[-I, B, O] extends Serializable {

  /**
   * A zero value for this aggregation. Should satisfy the property that any b + zero = b.
   * @since 1.6.0
   */
  def zero: B

  /**
   * Combine two values to produce a new value.  For performance, the function may modify `b` and
   * return it instead of constructing new object for b.
   * @since 1.6.0
   */
  def reduce(b: B, a: I): B

  /**
   * Merge two intermediate values.
   * @since 1.6.0
   */
  def merge(b1: B, b2: B): B

  /**
   * Transform the output of the reduction.
   * @since 1.6.0
   */
  def finish(reduction: B): O

  /**
   * Returns this `Aggregator` as a [[TypedColumn]] that can be used in [[Dataset]] or [[DataFrame]]
   * operations.
   * @since 1.6.0
   */
  def toColumn(
      implicit bEncoder: Encoder[B],
      cEncoder: Encoder[O]): TypedColumn[I, O] = {
    val expr =
      new AggregateExpression(
        TypedAggregateExpression(this),
        Complete,
        false)

    new TypedColumn[I, O](expr, encoderFor[O])
  }
}
