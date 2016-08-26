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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** The mode of an [[AggregateFunction]]. */
sealed trait AggregateMode

/**
 * An [[AggregateFunction]] with [[Partial]] mode is used for partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the aggregation buffer is returned.
 */
case object Partial extends AggregateMode

/**
 * An [[AggregateFunction]] with [[PartialMerge]] mode is used to merge aggregation buffers
 * containing intermediate results for this function.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the aggregation buffer is returned.
 */
case object PartialMerge extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Final]] mode is used to merge aggregation buffers
 * containing intermediate results for this function and then generate final result.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the final result of this function is returned.
 */
case object Final extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Complete]] mode is used to evaluate this function directly
 * from original input rows without any partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the final result of this function is returned.
 */
case object Complete extends AggregateMode

/**
 * A place holder expressions used in code-gen, it does not change the corresponding value
 * in the row.
 */
case object NoOp extends Expression with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
}

object AggregateExpression {
  def apply(
      aggregateFunction: AggregateFunction,
      mode: AggregateMode,
      isDistinct: Boolean): AggregateExpression = {
    AggregateExpression(
      aggregateFunction,
      mode,
      isDistinct,
      NamedExpression.newExprId)
  }
}

/**
 * A container for an [[AggregateFunction]] with its [[AggregateMode]] and a field
 * (`isDistinct`) indicating if DISTINCT keyword is specified for this function.
 */
case class AggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    resultId: ExprId)
  extends Expression
  with Unevaluable {

  lazy val resultAttribute: Attribute = if (aggregateFunction.resolved) {
    AttributeReference(
      aggregateFunction.toString,
      aggregateFunction.dataType,
      aggregateFunction.nullable)(exprId = resultId)
  } else {
    // This is a bit of a hack.  Really we should not be constructing this container and reasoning
    // about datatypes / aggregation mode until after we have finished analysis and made it to
    // planning.
    UnresolvedAttribute(aggregateFunction.toString)
  }

  // We compute the same thing regardless of our final result.
  override lazy val canonicalized: Expression =
    AggregateExpression(
      aggregateFunction.canonicalized.asInstanceOf[AggregateFunction],
      mode,
      isDistinct,
      ExprId(0))

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

  override def references: AttributeSet = {
    val childReferences = mode match {
      case Partial | Complete => aggregateFunction.references.toSeq
      case PartialMerge | Final => aggregateFunction.aggBufferAttributes
    }

    AttributeSet(childReferences)
  }

  override def toString: String = {
    val prefix = mode match {
      case Partial => "partial_"
      case PartialMerge => "merge_"
      case Final | Complete => ""
    }
    prefix + aggregateFunction.toAggString(isDistinct)
  }

  override def sql: String = aggregateFunction.sql(isDistinct)
}

/**
 * AggregateFunction is the superclass of two aggregation function interfaces:
 *
 *  - [[ImperativeAggregate]] is for aggregation functions that are specified in terms of
 *    initialize(), update(), and merge() functions that operate on Row-based aggregation buffers.
 *  - [[DeclarativeAggregate]] is for aggregation functions that are specified using
 *    Catalyst expressions.
 *
 * In both interfaces, aggregates must define the schema ([[aggBufferSchema]]) and attributes
 * ([[aggBufferAttributes]]) of an aggregation buffer which is used to hold partial aggregate
 * results. At runtime, multiple aggregate functions are evaluated by the same operator using a
 * combined aggregation buffer which concatenates the aggregation buffers of the individual
 * aggregate functions.
 *
 * Code which accepts [[AggregateFunction]] instances should be prepared to handle both types of
 * aggregate functions.
 */
sealed abstract class AggregateFunction extends Expression with ImplicitCastInputTypes {

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /**
   * Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
   * merged with mutable aggregation buffers in the merge() function or merge expressions).
   * These attributes are created automatically by cloning the [[aggBufferAttributes]].
   */
  def inputAggBufferAttributes: Seq[AttributeReference]

  /**
   * Indicates if this function supports partial aggregation.
   * Currently Hive UDAF is the only one that doesn't support partial aggregation.
   */
  def supportsPartial: Boolean = true

  /**
   * Result of the aggregate function when the input is empty. This is currently only used for the
   * proper rewriting of distinct aggregate functions.
   */
  def defaultResult: Option[Literal] = None

  /**
   * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] because
   * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
   * and the flag indicating if this aggregation is distinct aggregation or not.
   * An [[AggregateFunction]] should not be used without being wrapped in
   * an [[AggregateExpression]].
   */
  def toAggregateExpression(): AggregateExpression = toAggregateExpression(isDistinct = false)

  /**
   * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] and set isDistinct
   * field of the [[AggregateExpression]] to the given value because
   * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
   * and the flag indicating if this aggregation is distinct aggregation or not.
   * An [[AggregateFunction]] should not be used without being wrapped in
   * an [[AggregateExpression]].
   */
  def toAggregateExpression(isDistinct: Boolean): AggregateExpression = {
    AggregateExpression(aggregateFunction = this, mode = Complete, isDistinct = isDistinct)
  }

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }
}

/**
 * API for aggregation functions that are expressed in terms of imperative initialize(), update(),
 * and merge() functions which operate on Row-based aggregation buffers.
 *
 * Within these functions, code should access fields of the mutable aggregation buffer by adding the
 * bufferSchema-relative field number to `mutableAggBufferOffset` then using this new field number
 * to access the buffer Row. This is necessary because this aggregation function's buffer is
 * embedded inside of a larger shared aggregation buffer when an aggregation operator evaluates
 * multiple aggregate functions at the same time.
 *
 * We need to perform similar field number arithmetic when merging multiple intermediate
 * aggregate buffers together in `merge()` (in this case, use `inputAggBufferOffset` when accessing
 * the input buffer).
 *
 * Correct ImperativeAggregate evaluation depends on the correctness of `mutableAggBufferOffset` and
 * `inputAggBufferOffset`, but not on the correctness of the attribute ids in `aggBufferAttributes`
 * and `inputAggBufferAttributes`.
 */
abstract class ImperativeAggregate extends AggregateFunction with CodegenFallback {

  /**
   * The offset of this function's first buffer value in the underlying shared mutable aggregation
   * buffer.
   *
   * For example, we have two aggregate functions `avg(x)` and `avg(y)`, which share the same
   * aggregation buffer. In this shared buffer, the position of the first buffer value of `avg(x)`
   * will be 0 and the position of the first buffer value of `avg(y)` will be 2:
   * {{{
   *          avg(x) mutableAggBufferOffset = 0
   *                  |
   *                  v
   *                  +--------+--------+--------+--------+
   *                  |  sum1  | count1 |  sum2  | count2 |
   *                  +--------+--------+--------+--------+
   *                                    ^
   *                                    |
   *                     avg(y) mutableAggBufferOffset = 2
   * }}}
   */
  protected val mutableAggBufferOffset: Int

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate

  /**
   * The offset of this function's start buffer value in the underlying shared input aggregation
   * buffer. An input aggregation buffer is used when we merge two aggregation buffers together in
   * the `update()` function and is immutable (we merge an input aggregation buffer and a mutable
   * aggregation buffer and then store the new buffer values to the mutable aggregation buffer).
   *
   * An input aggregation buffer may contain extra fields, such as grouping keys, at its start, so
   * mutableAggBufferOffset and inputAggBufferOffset are often different.
   *
   * For example, say we have a grouping expression, `key`, and two aggregate functions,
   * `avg(x)` and `avg(y)`. In the shared input aggregation buffer, the position of the first
   * buffer value of `avg(x)` will be 1 and the position of the first buffer value of `avg(y)`
   * will be 3 (position 0 is used for the value of `key`):
   * {{{
   *          avg(x) inputAggBufferOffset = 1
   *                   |
   *                   v
   *          +--------+--------+--------+--------+--------+
   *          |  key   |  sum1  | count1 |  sum2  | count2 |
   *          +--------+--------+--------+--------+--------+
   *                                     ^
   *                                     |
   *                       avg(y) inputAggBufferOffset = 3
   * }}}
   */
  protected val inputAggBufferOffset: Int

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate

  // Note: although all subclasses implement inputAggBufferAttributes by simply cloning
  // aggBufferAttributes, that common clone code cannot be placed here in the abstract
  // ImperativeAggregate class, since that will lead to initialization ordering issues.

  /**
   * Initializes the mutable aggregation buffer located in `mutableAggBuffer`.
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   */
  def initialize(mutableAggBuffer: MutableRow): Unit

  /**
   * Updates its aggregation buffer, located in `mutableAggBuffer`, based on the given `inputRow`.
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   */
  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit

  /**
   * Combines new intermediate results from the `inputAggBuffer` with the existing intermediate
   * results in the `mutableAggBuffer.`
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   * Use `fieldNumber + inputAggBufferOffset` to access fields of `inputAggBuffer`.
   */
  def merge(mutableAggBuffer: MutableRow, inputAggBuffer: InternalRow): Unit
}

/**
 * API for aggregation functions that are expressed in terms of Catalyst expressions.
 *
 * When implementing a new expression-based aggregate function, start by implementing
 * `bufferAttributes`, defining attributes for the fields of the mutable aggregation buffer. You
 * can then use these attributes when defining `updateExpressions`, `mergeExpressions`, and
 * `evaluateExpressions`.
 *
 * Please note that children of an aggregate function can be unresolved (it will happen when
 * we create this function in DataFrame API). So, if there is any fields in
 * the implemented class that need to access fields of its children, please make
 * those fields `lazy val`s.
 */
abstract class DeclarativeAggregate
  extends AggregateFunction
  with Serializable
  with Unevaluable {

  /**
   * Expressions for initializing empty aggregation buffers.
   */
  val initialValues: Seq[Expression]

  /**
   * Expressions for updating the mutable aggregation buffer based on an input row.
   */
  val updateExpressions: Seq[Expression]

  /**
   * A sequence of expressions for merging two aggregation buffers together. When defining these
   * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
   * to the attributes corresponding to each of the buffers being merged (this magic is enabled
   * by the [[RichAttribute]] implicit class).
   */
  val mergeExpressions: Seq[Expression]

  /**
   * An expression which returns the final value for this aggregate function. Its data type should
   * match this expression's [[dataType]].
   */
  val evaluateExpression: Expression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  /**
   * A helper class for representing an attribute used in merging two
   * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
   * we merge buffer values and then update bufferLeft. A [[RichAttribute]]
   * of an [[AttributeReference]] `a` has two functions `left` and `right`,
   * which represent `a` in `bufferLeft` and `bufferRight`, respectively.
   */
  implicit class RichAttribute(a: AttributeReference) {
    /** Represents this attribute at the mutable buffer side. */
    def left: AttributeReference = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = inputAggBufferAttributes(aggBufferAttributes.indexOf(a))
  }
}

/**
 * Aggregation function which allows **arbitrary** user-defined java object to be used as internal
 * aggregation buffer object.
 *
 * {{{
 *                aggregation buffer for normal aggregation function `avg`
 *                    |
 *                    v
 *                  +--------------+---------------+-----------------------------------+
 *                  |  sum1 (Long) | count1 (Long) | generic user-defined java objects |
 *                  +--------------+---------------+-----------------------------------+
 *                                                     ^
 *                                                     |
 *                    Aggregation buffer object for `TypedImperativeAggregate` aggregation function
 * }}}
 *
 * Work flow (Partial mode aggregate at Mapper side, and Final mode aggregate at Reducer side):
 *
 * Stage 1: Partial aggregate at Mapper side:
 *
 *  1. The framework calls `createAggregationBuffer(): T` to create an empty internal aggregation
 *     buffer object.
 *  2. Upon each input row, the framework calls
 *     `update(buffer: T, input: InternalRow): Unit` to update the aggregation buffer object T.
 *  3. After processing all rows of current group (group by key), the framework will serialize
 *     aggregation buffer object T to storage format (Array[Byte]) and persist the Array[Byte]
 *     to disk if needed.
 *  4. The framework moves on to next group, until all groups have been processed.
 *
 * Shuffling exchange data to Reducer tasks...
 *
 * Stage 2: Final mode aggregate at Reducer side:
 *
 *  1. The framework calls `createAggregationBuffer(): T` to create an empty internal aggregation
 *     buffer object (type T) for merging.
 *  2. For each aggregation output of Stage 1, The framework de-serializes the storage
 *     format (Array[Byte]) and produces one input aggregation object (type T).
 *  3. For each input aggregation object, the framework calls `merge(buffer: T, input: T): Unit`
 *     to merge the input aggregation object into aggregation buffer object.
 *  4. After processing all input aggregation objects of current group (group by key), the framework
 *     calls method `eval(buffer: T)` to generate the final output for this group.
 *  5. The framework moves on to next group, until all groups have been processed.
 *
 * NOTE: SQL with TypedImperativeAggregate functions is planned in sort based aggregation,
 * instead of hash based aggregation, as TypedImperativeAggregate use BinaryType as aggregation
 * buffer's storage format, which is not supported by hash based aggregation. Hash based
 * aggregation only support aggregation buffer of mutable types (like LongType, IntType that have
 * fixed length and can be mutated in place in UnsafeRow)
 */
abstract class TypedImperativeAggregate[T] extends ImperativeAggregate {

  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  def createAggregationBuffer(): T

  /**
   * In-place updates the aggregation buffer object with an input row. buffer = buffer + input.
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input an input row
   */
  def update(buffer: T, input: InternalRow): Unit

  /**
   * Merges an input aggregation object into aggregation buffer object. buffer = buffer + input.
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input an input aggregation object. Input aggregation object can be produced by
   *              de-serializing the partial aggregate's output from Mapper side.
   */
  def merge(buffer: T, input: T): Unit

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  def eval(buffer: T): Any

  /** Serializes the aggregation buffer object T to Array[Byte] */
  def serialize(buffer: T): Array[Byte]

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  def deserialize(storageFormat: Array[Byte]): T

  final override def initialize(buffer: MutableRow): Unit = {
    val bufferObject = createAggregationBuffer()
    buffer.update(mutableAggBufferOffset, bufferObject)
  }

  final override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val bufferObject = getField[T](buffer, mutableAggBufferOffset)
    update(bufferObject, input)
  }

  final override def merge(buffer: MutableRow, inputBuffer: InternalRow): Unit = {
    val bufferObject = getField[T](buffer, mutableAggBufferOffset)
    // The inputBuffer stores serialized aggregation buffer object produced by partial aggregate
    val inputObject = deserialize(inputBuffer.getBinary(inputAggBufferOffset))
    merge(bufferObject, inputObject)
  }

  final override def eval(buffer: InternalRow): Any = {
    val bufferObject = getField[T](buffer, mutableAggBufferOffset)
    eval(bufferObject)
  }

  private[this] val anyObjectType = ObjectType(classOf[AnyRef])
  private def getField[U](input: InternalRow, fieldIndex: Int): U = {
    input.get(fieldIndex, anyObjectType).asInstanceOf[U]
  }

  final override lazy val aggBufferAttributes: Seq[AttributeReference] = {
    // Underlying storage type for the aggregation buffer object
    Seq(AttributeReference("buf", BinaryType)())
  }

  final override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  final override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  /**
   * In-place replaces the aggregation buffer object stored at buffer's index
   * `mutableAggBufferOffset`, with SparkSQL internally supported underlying storage format
   * (BinaryType).
   */
  final def serializeAggregateBufferInPlace(buffer: MutableRow): Unit = {
    val bufferObject = getField[T](buffer, mutableAggBufferOffset)
    buffer(mutableAggBufferOffset) = serialize(bufferObject)
  }
}
