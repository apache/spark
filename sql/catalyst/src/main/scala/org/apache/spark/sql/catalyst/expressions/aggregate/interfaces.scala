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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** The mode of an [[AggregateFunction2]]. */
private[sql] sealed trait AggregateMode

/**
 * An [[AggregateFunction2]] with [[Partial]] mode is used for partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the aggregation buffer is returned.
 */
private[sql] case object Partial extends AggregateMode

/**
 * An [[AggregateFunction2]] with [[PartialMerge]] mode is used to merge aggregation buffers
 * containing intermediate results for this function.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the aggregation buffer is returned.
 */
private[sql] case object PartialMerge extends AggregateMode

/**
 * An [[AggregateFunction2]] with [[Final]] mode is used to merge aggregation buffers
 * containing intermediate results for this function and then generate final result.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the final result of this function is returned.
 */
private[sql] case object Final extends AggregateMode

/**
 * An [[AggregateFunction2]] with [[Complete]] mode is used to evaluate this function directly
 * from original input rows without any partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the final result of this function is returned.
 */
private[sql] case object Complete extends AggregateMode

/**
 * A place holder expressions used in code-gen, it does not change the corresponding value
 * in the row.
 */
private[sql] case object NoOp extends Expression with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
}

/**
 * A container for an [[AggregateFunction2]] with its [[AggregateMode]] and a field
 * (`isDistinct`) indicating if DISTINCT keyword is specified for this function.
 * @param aggregateFunction
 * @param mode
 * @param isDistinct
 */
private[sql] case class AggregateExpression2(
    aggregateFunction: AggregateFunction2,
    mode: AggregateMode,
    isDistinct: Boolean) extends AggregateExpression {

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

  override def references: AttributeSet = {
    val childReferences = mode match {
      case Partial | Complete => aggregateFunction.references.toSeq
      case PartialMerge | Final => aggregateFunction.bufferAttributes
    }

    AttributeSet(childReferences)
  }

  override def toString: String = s"(${aggregateFunction},mode=$mode,isDistinct=$isDistinct)"
}

abstract class AggregateFunction2
  extends Expression with ImplicitCastInputTypes {

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /**
   * The offset of this function's start buffer value in the
   * underlying shared mutable aggregation buffer.
   * For example, we have two aggregate functions `avg(x)` and `avg(y)`, which share
   * the same aggregation buffer. In this shared buffer, the position of the first
   * buffer value of `avg(x)` will be 0 and the position of the first buffer value of `avg(y)`
   * will be 2.
   */
  protected var mutableBufferOffset: Int = 0

  def withNewMutableBufferOffset(newMutableBufferOffset: Int): Unit = {
    mutableBufferOffset = newMutableBufferOffset
  }

  /**
   * The offset of this function's start buffer value in the
   * underlying shared input aggregation buffer. An input aggregation buffer is used
   * when we merge two aggregation buffers and it is basically the immutable one
   * (we merge an input aggregation buffer and a mutable aggregation buffer and
   * then store the new buffer values to the mutable aggregation buffer).
   * Usually, an input aggregation buffer also contain extra elements like grouping
   * keys at the beginning. So, mutableBufferOffset and inputBufferOffset are often
   * different.
   * For example, we have a grouping expression `key``, and two aggregate functions
   * `avg(x)` and `avg(y)`. In this shared input aggregation buffer, the position of the first
   * buffer value of `avg(x)` will be 1 and the position of the first buffer value of `avg(y)`
   * will be 3 (position 0 is used for the value of key`).
   */
  protected var inputBufferOffset: Int = 0

  def withNewInputBufferOffset(newInputBufferOffset: Int): Unit = {
    inputBufferOffset = newInputBufferOffset
  }

  /** The schema of the aggregation buffer. */
  def bufferSchema: StructType

  /** Attributes of fields in bufferSchema. */
  def bufferAttributes: Seq[AttributeReference]

  /** Clones bufferAttributes. */
  def cloneBufferAttributes: Seq[Attribute]

  /**
   * Initializes its aggregation buffer located in `buffer`.
   * It will use bufferOffset to find the starting point of
   * its buffer in the given `buffer` shared with other functions.
   */
  def initialize(buffer: MutableRow): Unit

  /**
   * Updates its aggregation buffer located in `buffer` based on the given `input`.
   * It will use bufferOffset to find the starting point of its buffer in the given `buffer`
   * shared with other functions.
   */
  def update(buffer: MutableRow, input: InternalRow): Unit

  /**
   * Updates its aggregation buffer located in `buffer1` by combining intermediate results
   * in the current buffer and intermediate results from another buffer `buffer2`.
   * It will use bufferOffset to find the starting point of its buffer in the given `buffer1`
   * and `buffer2`.
   */
  def merge(buffer1: MutableRow, buffer2: InternalRow): Unit

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}

/**
 * A helper class for aggregate functions that can be implemented in terms of catalyst expressions.
 */
abstract class AlgebraicAggregate extends AggregateFunction2 with Serializable with Unevaluable {

  val initialValues: Seq[Expression]
  val updateExpressions: Seq[Expression]
  val mergeExpressions: Seq[Expression]
  val evaluateExpression: Expression

  override lazy val cloneBufferAttributes = bufferAttributes.map(_.newInstance())

  /**
   * A helper class for representing an attribute used in merging two
   * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
   * we merge buffer values and then update bufferLeft. A [[RichAttribute]]
   * of an [[AttributeReference]] `a` has two functions `left` and `right`,
   * which represent `a` in `bufferLeft` and `bufferRight`, respectively.
   * @param a
   */
  implicit class RichAttribute(a: AttributeReference) {
    /** Represents this attribute at the mutable buffer side. */
    def left: AttributeReference = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = cloneBufferAttributes(bufferAttributes.indexOf(a))
  }

  /** An AlgebraicAggregate's bufferSchema is derived from bufferAttributes. */
  override def bufferSchema: StructType = StructType.fromAttributes(bufferAttributes)

  override def initialize(buffer: MutableRow): Unit = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's initialize should not be called directly")
  }

  override final def update(buffer: MutableRow, input: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's update should not be called directly")
  }

  override final def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's merge should not be called directly")
  }

}

