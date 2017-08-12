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

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types._

/**
 * The base class for implementing user-defined aggregate functions (UDAF).
 *
 * @since 1.5.0
 */
@InterfaceStability.Stable
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
   * A `StructType` represents data types of input arguments of this aggregate function.
   * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
   * with type of `DoubleType` and `LongType`, the returned `StructType` will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this `StructType` is only used to identify the corresponding
   * input argument. Users can choose names to identify the input arguments.
   *
   * @since 1.5.0
   */
  def inputSchema: StructType

  /**
   * A `StructType` represents data types of values in the aggregation buffer.
   * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
   * (i.e. two intermediate values) with type of `DoubleType` and `LongType`,
   * the returned `StructType` will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this `StructType` is only used to identify the corresponding
   * buffer value. Users can choose names to identify the input arguments.
   *
   * @since 1.5.0
   */
  def bufferSchema: StructType

  /**
   * The `DataType` of the returned value of this [[UserDefinedAggregateFunction]].
   *
   * @since 1.5.0
   */
  def dataType: DataType

  /**
   * Returns true iff this function is deterministic, i.e. given the same input,
   * always return the same output.
   *
   * @since 1.5.0
   */
  def deterministic: Boolean

  /**
   * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
   *
   * The contract should be that applying the merge function on two initial buffers should just
   * return the initial buffer itself, i.e.
   * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
   *
   * @since 1.5.0
   */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /**
   * Updates the given aggregation buffer `buffer` with new input data from `input`.
   *
   * This is called once per input row.
   *
   * @since 1.5.0
   */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /**
   * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
   *
   * This is called when we merge two partially aggregated data together.
   *
   * @since 1.5.0
   */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
   * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
   * aggregation buffer.
   *
   * @since 1.5.0
   */
  def evaluate(buffer: Row): Any

  /**
   * Creates a `Column` for this UDAF using given `Column`s as input arguments.
   *
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = false)
    Column(aggregateExpression)
  }

  /**
   * Creates a `Column` for this UDAF using the distinct values of the given
   * `Column`s as input arguments.
   *
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def distinct(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = true)
    Column(aggregateExpression)
  }
}

/**
 * A `Row` representing a mutable aggregation buffer.
 *
 * This is not meant to be extended outside of Spark.
 *
 * @since 1.5.0
 */
@InterfaceStability.Stable
abstract class MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer. */
  def update(i: Int, value: Any): Unit
}
