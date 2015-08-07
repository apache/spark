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

import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types._
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * The abstract class for implementing user-defined aggregate functions.
 */
@Experimental
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
   * A [[StructType]] represents data types of input arguments of this aggregate function.
   * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
   * with type of [[DoubleType]] and [[LongType]], the returned [[StructType]] will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * input argument. Users can choose names to identify the input arguments.
   */
  def inputSchema: StructType

  /**
   * A [[StructType]] represents data types of values in the aggregation buffer.
   * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
   * (i.e. two intermediate values) with type of [[DoubleType]] and [[LongType]],
   * the returned [[StructType]] will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * buffer value. Users can choose names to identify the input arguments.
   */
  def bufferSchema: StructType

  /**
   * The [[DataType]] of the returned value of this [[UserDefinedAggregateFunction]].
   */
  def returnDataType: DataType

  /** Indicates if this function is deterministic. */
  def deterministic: Boolean

  /**
   *  Initializes the given aggregation buffer. Initial values set by this method should satisfy
   *  the condition that when merging two buffers with initial values, the new buffer
   *  still store initial values.
   */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /** Updates the given aggregation buffer `buffer` with new input data from `input`. */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /** Merges two aggregation buffers and stores the updated buffer values back to `buffer1`. */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
   * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
   * aggregation buffer.
   */
  def evaluate(buffer: Row): Any

  /**
   * Creates a [[Column]] for this UDAF with given [[Column]]s as arguments.
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = false)
    Column(aggregateExpression)
  }

  /**
   * Creates a [[Column]] for this UDAF with given [[Column]]s as arguments.
   * If `isDistinct` is true, this UDAF is working on distinct input values.
   */
  @scala.annotation.varargs
  def apply(isDistinct: Boolean, exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = isDistinct)
    Column(aggregateExpression)
  }
}

/**
 * :: Experimental ::
 * A [[Row]] representing an mutable aggregation buffer.
 */
@Experimental
trait MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer. */
  def update(i: Int, value: Any): Unit
}
