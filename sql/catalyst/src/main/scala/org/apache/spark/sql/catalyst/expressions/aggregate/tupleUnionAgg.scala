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

import org.apache.datasketches.tuple.{Sketch, Summary, SummarySetOperations, Union}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummarySetOperations}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.{SketchSize, SummaryAggregateMode, ThetaSketchUtils, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, IntegerType}

/**
 * The TupleUnionAggDouble function unions multiple TupleSketch binary representations with a
 * double type summary to produce a single merged TupleSketch. This is useful for combining
 * pre-aggregated TupleSketch results from different partitions or data sources.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with a double type summary) to be
 *   unioned
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the union operation
 * @param mode
 *   the aggregation mode for numeric summaries during union (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
case class TupleUnionAggDouble(
    child: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleUnionAggBase[DoubleSummary]
    with TernaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(
      child,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString),
      0,
      0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(child, lgNomEntries, mode, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleUnionAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleUnionAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleUnionAggDouble =
    copy(child = newFirst, lgNomEntries = newSecond, mode = newThird)

  override def first: Expression = child
  override def second: Expression = lgNomEntries
  override def third: Expression = mode

  // Override for TypedImperativeAggregate
  override def prettyName: String = "tuple_union_agg_double"

  /**
   * Creates DoubleSummarySetOperations for merge operations.
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
 * The TupleUnionAggInteger function unions multiple TupleSketch binary representations with an
 * integer type summary to produce a single merged TupleSketch. This is useful for combining
 * pre-aggregated TupleSketch results from different partitions or data sources.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with an integer type summary) to
 *   be unioned
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the union operation
 * @param mode
 *   the aggregation mode for numeric summaries during union (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
case class TupleUnionAggInteger(
    child: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleUnionAggBase[IntegerSummary]
    with TernaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(
      child,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString),
      0,
      0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(child, lgNomEntries, mode, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleUnionAggInteger =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleUnionAggInteger =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleUnionAggInteger =
    copy(child = newFirst, lgNomEntries = newSecond, mode = newThird)

  override def first: Expression = child
  override def second: Expression = lgNomEntries
  override def third: Expression = mode

  // Override for TypedImperativeAggregate
  override def prettyName: String = "tuple_union_agg_integer"

  /**
   * Creates IntegerSummarySetOperations for merge operations.
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

abstract class TupleUnionAggBase[S <: Summary]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with SketchSize
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
    Seq(BinaryType, IntegerType, StringTypeWithCollation(supportsTrimCollation = true))

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    val lgCheck = checkLgNomEntriesParameter()

    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (lgCheck.isFailure) {
      lgCheck
    } else {
      checkModeParameter()
    }
  }

  /**
   * Instantiate a Union instance using the lgNomEntries param and summary set operations.
   *
   * @return
   *   a Union instance wrapped with UnionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState[S] = {
    val summarySetOps = createSummarySetOperations()
    val union = new Union[S](1 << lgNomEntriesInput, summarySetOps)
    UnionTupleAggregationBuffer(union)
  }

  /**
   * Deserialize the input TupleSketch binary representation and union it with the aggregation
   * buffer. The input must be a valid TupleSketch binary representation. Notes:
   *   - Null values are ignored.
   *   - Invalid binary sketches will throw an exception.
   *
   * @param unionBuffer
   *   A Union instance used as the aggregation buffer
   * @param input
   *   An input row containing a TupleSketch binary representation
   */
  override def update(
      unionBuffer: TupleSketchState[S],
      input: InternalRow): TupleSketchState[S] = {
    // Get the binary sketch from the input
    val sketchBytes = child.eval(input)

    // Return early for null values
    if (sketchBytes == null) {
      unionBuffer
    } else {
      val bytes = sketchBytes.asInstanceOf[Array[Byte]]
      val inputSketch = heapifySketch(bytes)

      val union = unionBuffer match {
        case UnionTupleAggregationBuffer(existingUnion) => existingUnion
        case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
      }

      // Merge it with the buffer
      union.union(inputSketch)
      unionBuffer
    }
  }

  /**
   * Merges an input TupleSketch state into the Union aggregation buffer.
   *
   * @param unionBuffer
   *   The Union instance used to store the aggregation result
   * @param input
   *   An input Union or CompactSketch instance
   */
  override def merge(
      unionBuffer: TupleSketchState[S],
      input: TupleSketchState[S]): TupleSketchState[S] = {
    (unionBuffer, input) match {
      // The input was serialized then deserialized.
      case (unionBuffer @ UnionTupleAggregationBuffer(union), FinalizedTupleSketch(sketch)) =>
        union.union(sketch)
        unionBuffer
      // If both arguments are union objects, merge them directly.
      case (
            unionBuffer @ UnionTupleAggregationBuffer(union1),
            UnionTupleAggregationBuffer(union2)) =>
        union1.union(union2.getResult)
        unionBuffer
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a CompactSketch binary representation from the Union aggregation buffer.
   *
   * @param sketchState
   *   Union instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def eval(sketchState: TupleSketchState[S]): Any = sketchState.eval()

  /**
   * Returns a CompactSketch binary representation from the Union aggregation buffer.
   *
   * @param sketchState
   *   Union instance used as an aggregation buffer
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

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, lgNomEntries, mode) - Returns the unioned TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with a double type summary.
      `lgNomEntries` is the log-base-2 of nominal entries for the union operation. Default is 12.
      `mode` is the aggregation mode for numeric summaries during union (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_double(key, summary) as sketch FROM VALUES (1, 5.0D), (2, 10.0D) tab(key, summary) UNION ALL SELECT tuple_sketch_agg_double(key, summary) as sketch FROM VALUES (2, 3.0D), (3, 7.0D) tab(key, summary));
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionAggDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("child"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 3 with defaults filled in
    assert(expressions.size == 3)
    new TupleUnionAggDouble(expressions(0), expressions(1), expressions(2))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, lgNomEntries, mode) - Returns the unioned TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with an integer type summary.
      `lgNomEntries` is the log-base-2 of nominal entries for the union operation. Default is 12.
      `mode` is the aggregation mode for numeric summaries during union (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_integer(key, summary) as sketch FROM VALUES (1, 5), (2, 10) tab(key, summary) UNION ALL SELECT tuple_sketch_agg_integer(key, summary) as sketch FROM VALUES (2, 3), (3, 7) tab(key, summary));
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionAggIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("child"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 3 with defaults filled in
    assert(expressions.size == 3)
    new TupleUnionAggInteger(expressions(0), expressions(1), expressions(2))
  }
}
