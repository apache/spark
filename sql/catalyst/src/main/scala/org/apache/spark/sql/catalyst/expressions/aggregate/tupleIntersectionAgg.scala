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

import org.apache.datasketches.tuple.{Intersection, Sketch, Summary, SummarySetOperations}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummarySetOperations}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.{SummaryAggregateMode, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

/**
 * The TupleIntersectionAggDouble function computes the intersection of multiple TupleSketch
 * binary representations with a double type summary to produce a single TupleSketch containing
 * only the elements common to all input sketches. This is useful for finding overlapping unique
 * values across different datasets.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with a double type summary) to be
 *   intersected
 * @param mode
 *   the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, mode) - Returns the intersected TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with a double type summary.
      `mode` is the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_double(key, summary) as sketch FROM VALUES (1, 5.0D), (2, 10.0D), (3, 15.0D) tab(key, summary) UNION ALL SELECT tuple_sketch_agg_double(key, summary) as sketch FROM VALUES (2, 3.0D), (3, 7.0D), (4, 12.0D) tab(key, summary));
       2.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionAggDouble(
    child: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleIntersectionAggBase[DoubleSummary]
    with BinaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(child, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(child: Expression, mode: Expression) = {
    this(child, mode, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleIntersectionAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): TupleIntersectionAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleIntersectionAggDouble =
    copy(child = newLeft, mode = newRight)

  override def left: Expression = child
  override def right: Expression = mode

  // Override for TypedImperativeAggregate
  override def prettyName: String = "tuple_intersection_agg_double"

  /**
   * Creates DoubleSummarySetOperations for intersection operations.
   */
  override protected def createSummarySetOperations(): SummarySetOperations[DoubleSummary] = {
    new DoubleSummarySetOperations(modeEnum.toDoubleSummaryMode)
  }

  /**
   * Heapify a sketch from a byte array.
   *
   * @param buffer
   *   the serialized sketch byte array
   * @return
   *   a Sketch[DoubleSummary] instance
   */
  override protected def heapifySketch(buffer: Array[Byte]): Sketch[DoubleSummary] = {
    TupleSketchUtils.heapifyDoubleSketch(buffer, prettyName)
  }
}

/**
 * The TupleIntersectionAggInteger function computes the intersection of multiple TupleSketch
 * binary representations with an integer type summary to produce a single TupleSketch containing
 * only the elements common to all input sketches. This is useful for finding overlapping unique
 * values across different datasets.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with an integer type summary) to
 *   be intersected
 * @param mode
 *   the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, mode) - Returns the intersected TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with an integer type summary.
      `mode` is the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_integer(key, summary) as sketch FROM VALUES (1, 1), (2, 2), (3, 3) tab(key, summary) UNION ALL SELECT tuple_sketch_agg_integer(key, summary) as sketch FROM VALUES (2, 2), (3, 3), (4, 4) tab(key, summary));
       2.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionAggInteger(
    child: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleIntersectionAggBase[IntegerSummary]
    with BinaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(child, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(child: Expression, mode: Expression) = {
    this(child, mode, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleIntersectionAggInteger =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): TupleIntersectionAggInteger =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleIntersectionAggInteger =
    copy(child = newLeft, mode = newRight)

  override def left: Expression = child
  override def right: Expression = mode

  // Override for TypedImperativeAggregate
  override def prettyName: String = "tuple_intersection_agg_integer"

  /**
   * Creates IntegerSummarySetOperations for intersection operations.
   */
  override protected def createSummarySetOperations(): SummarySetOperations[IntegerSummary] = {
    val mode = modeEnum.toIntegerSummaryMode
    new IntegerSummarySetOperations(mode, mode)
  }

  /**
   * Heapify a sketch from a byte array.
   *
   * @param buffer
   *   the serialized sketch byte array
   * @return
   *   a Sketch[IntegerSummary] instance
   */
  override protected def heapifySketch(buffer: Array[Byte]): Sketch[IntegerSummary] = {
    TupleSketchUtils.heapifyIntegerSketch(buffer, prettyName)
  }
}

abstract class TupleIntersectionAggBase[S <: Summary]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with SummaryAggregateMode
    with ImplicitCastInputTypes {

  // Abstract methods that subclasses must implement
  protected def createSummarySetOperations(): SummarySetOperations[S]
  protected def heapifySketch(buffer: Array[Byte]): Sketch[S]

  // Abstract members that subclasses must implement
  protected def child: Expression

  // Type and input validation overrides
  override def dataType: DataType = BinaryType
  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkModeParameter()
    }
  }

  /**
   * Instantiate an Intersection instance using the summary set operations.
   *
   * @return
   *   an Intersection instance wrapped with IntersectionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState[S] = {
    val summarySetOps = createSummarySetOperations()
    val intersection = new Intersection[S](summarySetOps)
    IntersectionTupleAggregationBuffer(intersection)
  }

  /**
   * Deserialize the input TupleSketch binary representation and intersect it with the aggregation
   * buffer. The input must be a valid TupleSketch binary representation. Notes:
   *   - Null values are ignored.
   *   - Invalid binary sketches will throw an exception.
   *
   * @param intersectionBuffer
   *   An Intersection instance used as the aggregation buffer
   * @param input
   *   An input row containing a TupleSketch binary representation
   */
  override def update(
      intersectionBuffer: TupleSketchState[S],
      input: InternalRow): TupleSketchState[S] = {
    // Get the binary sketch from the input
    val sketchBytes = child.eval(input)

    // Return early for null values
    if (sketchBytes == null) {
      intersectionBuffer
    } else {
      val bytes = sketchBytes.asInstanceOf[Array[Byte]]
      val inputSketch = heapifySketch(bytes)

      val intersection = intersectionBuffer match {
        case IntersectionTupleAggregationBuffer(existingIntersection) => existingIntersection
        case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
      }

      // Merge it with the buffer
      intersection.intersect(inputSketch)
      intersectionBuffer
    }
  }

  /**
   * Merges an input TupleSketch state into the Intersection aggregation buffer.
   *
   * @param intersectionBuffer
   *   The Intersection instance used to store the aggregation result
   * @param input
   *   An input Intersection or CompactSketch instance
   */
  override def merge(
      intersectionBuffer: TupleSketchState[S],
      input: TupleSketchState[S]): TupleSketchState[S] = {

    (intersectionBuffer, input) match {
      // The input was serialized then deserialized.
      case (
            intersectionBuffer @ IntersectionTupleAggregationBuffer(intersection),
            FinalizedTupleSketch(sketch)) =>
        intersection.intersect(sketch)
        intersectionBuffer
      // If both arguments are intersection objects, merge them directly.
      case (
            intersectionBuffer @ IntersectionTupleAggregationBuffer(intersection1),
            IntersectionTupleAggregationBuffer(intersection2)) =>
        intersection1.intersect(intersection2.getResult)
        intersectionBuffer

      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a CompactSketch binary representation from the Intersection aggregation buffer.
   *
   * @param sketchState
   *   Intersection instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def eval(sketchState: TupleSketchState[S]): Any = sketchState.eval()

  /**
   * Returns a CompactSketch binary representation from the Intersection aggregation buffer.
   *
   * @param sketchState
   *   Intersection instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def serialize(sketchState: TupleSketchState[S]): Array[Byte] =
    sketchState.serialize()

  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[S] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      createAggregationBuffer()
    }
  }
}
