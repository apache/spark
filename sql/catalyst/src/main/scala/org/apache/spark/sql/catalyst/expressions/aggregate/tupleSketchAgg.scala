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

import org.apache.datasketches.tuple.{Sketch, SummaryFactory, SummarySetOperations, Union, UpdatableSketchBuilder, UpdatableSummary}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory, IntegerSummarySetOperations}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, SketchSize, SummaryAggregateMode, ThetaSketchUtils, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The TupleSketchAggDouble function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * double type summary values that can be aggregated using different modes (sum, min, max,
 * alwaysone), and outputs the binary representation of the TupleSketch.
 *
 * Keys are hashed internally based on their type and value - the same logical value in different
 * types (e.g., String("123") and Int(123)) will be treated as distinct keys. However, summary
 * value types must be consistent across all calls; mixing types can produce incorrect results or
 * precision loss. The value type suffix in the function name (e.g., _double) ensures type safety.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param key
 *   key expression against which unique counting will occur
 * @param summary
 *   summary expression (double type) against which different mode aggregations will occur
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param mode
 *   the aggregation mode for numeric summaries (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
case class TupleSketchAggDouble(
    key: Expression,
    summary: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleSketchAggBase[java.lang.Double, DoubleSummary]
    with QuaternaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(key: Expression, summary: Expression) = {
    this(
      key,
      summary,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString),
      0,
      0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression) = {
    this(key, summary, lgNomEntries, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(key, summary, lgNomEntries, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify key, summary (double/float), lgNomEntries (int), and mode
   * (string) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      keyInputTypes,
      summaryInputType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate base inputs (key, summary, lgNomEntries) and mode
   * parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = checkBaseInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkModeParameter()
    }
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleSketchAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleSketchAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleSketchAggDouble =
    copy(key = newFirst, summary = newSecond, lgNomEntries = newThird, mode = newFourth)

  override def first: Expression = key
  override def second: Expression = summary
  override def third: Expression = lgNomEntries
  override def fourth: Expression = mode

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_sketch_agg_double"

  /** Specifies accepted summary input types (double). */
  override protected def summaryInputType: AbstractDataType = DoubleType

  /**
   * Creates a DoubleSummaryFactory with the configured aggregation mode.
   */
  override protected def createSummaryFactory(): SummaryFactory[DoubleSummary] = {
    new DoubleSummaryFactory(modeEnum.toDoubleSummaryMode)
  }

  /**
   * Creates DoubleSummarySetOperations for merge operations with the configured mode.
   */
  override protected def createSummarySetOperations(): SummarySetOperations[DoubleSummary] = {
    new DoubleSummarySetOperations(modeEnum.toDoubleSummaryMode)
  }


  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[DoubleSummary] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        TupleSketchUtils.heapifyDoubleSketch(buffer, prettyName))
    } else {
      createAggregationBuffer()
    }
  }
}

/**
 * The TupleSketchAggInteger function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * integer type summary values that can be aggregated using different modes (sum, min, max,
 * alwaysone), and outputs the binary representation of the TupleSketch.
 *
 * Keys are hashed internally based on their type and value - the same logical value in different
 * types (e.g., String("123") and Int(123)) will be treated as distinct keys. However, summary
 * value types must be consistent across all calls; mixing types can produce incorrect results or
 * precision loss. The value type suffix in the function name (e.g., _integer) ensures type safety.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param key
 *   key expression against which unique counting will occur
 * @param summary
 *   summary expression (integer type) against which different mode aggregations will occur
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param mode
 *   the aggregation mode for numeric summaries (sum, min, max, alwaysone)
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
case class TupleSketchAggInteger(
    key: Expression,
    summary: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleSketchAggBase[Integer, IntegerSummary]
    with QuaternaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(key: Expression, summary: Expression) = {
    this(
      key,
      summary,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString),
      0,
      0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression) = {
    this(key, summary, lgNomEntries, Literal(TupleSummaryMode.Sum.toString), 0, 0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(key, summary, lgNomEntries, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify key, summary (integer), lgNomEntries (int), and mode (string)
   * parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      keyInputTypes,
      summaryInputType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate base inputs (key, summary, lgNomEntries) and mode
   * parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = checkBaseInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkModeParameter()
    }
  }

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleSketchAggInteger =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleSketchAggInteger =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleSketchAggInteger =
    copy(key = newFirst, summary = newSecond, lgNomEntries = newThird, mode = newFourth)

  override def first: Expression = key
  override def second: Expression = summary
  override def third: Expression = lgNomEntries
  override def fourth: Expression = mode

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_sketch_agg_integer"

  /** Specifies accepted summary input types (integer). */
  override protected def summaryInputType: AbstractDataType =
    IntegerType

  /**
   * Creates an IntegerSummaryFactory with the configured aggregation mode.
   */
  override protected def createSummaryFactory(): SummaryFactory[IntegerSummary] = {
    new IntegerSummaryFactory(modeEnum.toIntegerSummaryMode)
  }

  /**
   * Creates IntegerSummarySetOperations for merge operations with the configured mode.
   */
  override protected def createSummarySetOperations(): SummarySetOperations[IntegerSummary] = {
    val mode = modeEnum.toIntegerSummaryMode
    new IntegerSummarySetOperations(mode, mode)
  }


  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[IntegerSummary] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        TupleSketchUtils.heapifyIntegerSketch(buffer, prettyName))
    } else {
      createAggregationBuffer()
    }
  }
}

abstract class TupleSketchAggBase[U, S <: UpdatableSummary[U]]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with SketchSize
    with ImplicitCastInputTypes {

  // Abstract methods that subclasses must implement
  protected def summaryInputType: AbstractDataType
  protected def createSummaryFactory(): SummaryFactory[S]
  protected def createSummarySetOperations(): SummarySetOperations[S]

  // Abstract members that subclasses must implement
  protected def key: Expression
  protected def summary: Expression

  protected final val keyInputTypes: AbstractDataType =
    TypeCollection(
      ArrayType(IntegerType),
      ArrayType(LongType),
      BinaryType,
      DoubleType,
      FloatType,
      IntegerType,
      LongType,
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType
  override def nullable: Boolean = false

  protected def checkBaseInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkLgNomEntriesParameter()
    }
  }

  /**
   * Instantiate an UpdatableSketch instance using the lgNomEntries param and summary factory.
   *
   * @return
   *   an UpdatableSketch instance wrapped with UpdatableTupleSketchBuffer
   */
  override def createAggregationBuffer(): TupleSketchState[S] = {
    val factory = createSummaryFactory()
    val builder = new UpdatableSketchBuilder[U, S](factory)
    builder.setNominalEntries(1 << lgNomEntriesInput)
    val sketch = builder.build()
    UpdatableTupleSketchBuffer(sketch)
  }

  /**
   * Evaluate the input row and update the UpdatableSketch instance with the row's key and summary
   * value. The update function only supports a subset of Spark SQL types, and an exception will
   * be thrown for unsupported types. Notes:
   *   - Null values are ignored.
   *   - Empty byte arrays are ignored
   *   - Empty arrays of supported element types are ignored
   *   - Strings that are collation-equal to the empty string are ignored.
   *
   * @param updateBuffer
   *   A previously initialized UpdatableSketch instance
   * @param input
   *   An input row
   */
  override def update(
      updateBuffer: TupleSketchState[S],
      input: InternalRow): TupleSketchState[S] = {
    val keyValue = key.eval(input)
    val summaryValue = summary.eval(input)

    // Return early for null values.
    if (keyValue == null || summaryValue == null) {
      updateBuffer
    } else {

    // Type checking is already done by ImplicitCastInputTypes.
    val normalizedSummary = summaryValue.asInstanceOf[U]

    // Initialized buffer should be UpdatableTupleSketchBuffer, else error out.
    val sketch = updateBuffer match {
      case UpdatableTupleSketchBuffer(s) => s
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    key.dataType match {
      case ArrayType(IntegerType, _) =>
        val arr = keyValue.asInstanceOf[ArrayData].toIntArray()
        sketch.update(arr, normalizedSummary)
      case ArrayType(LongType, _) =>
        val arr = keyValue.asInstanceOf[ArrayData].toLongArray()
        sketch.update(arr, normalizedSummary)
      case BinaryType =>
        val bytes = keyValue.asInstanceOf[Array[Byte]]
        sketch.update(bytes, normalizedSummary)
      case DoubleType =>
        sketch.update(keyValue.asInstanceOf[Double], normalizedSummary)
      case FloatType =>
        sketch.update(keyValue.asInstanceOf[Float].toDouble, normalizedSummary)
      case IntegerType =>
        sketch.update(keyValue.asInstanceOf[Int].toLong, normalizedSummary)
      case LongType =>
        sketch.update(keyValue.asInstanceOf[Long], normalizedSummary)
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val str = keyValue.asInstanceOf[UTF8String]
        if (!collation.equalsFunction(str, UTF8String.EMPTY_UTF8)) {
          sketch.update(collation.sortKeyFunction.apply(str), normalizedSummary)
        }
      case _ =>
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3121",
          messageParameters = Map("dataType" -> key.dataType.toString))
    }

      updateBuffer
    }
  }

  /**
   * Merges an input CompactSketch into the UpdatableSketch which is acting as the aggregation
   * buffer.
   *
   * @param updateBuffer
   *   The UpdatableSketch or Union instance used to store the aggregation result
   * @param input
   *   An input UpdatableSketch, Union, or CompactSketch instance
   */
  override def merge(
      updateBuffer: TupleSketchState[S],
      input: TupleSketchState[S]): TupleSketchState[S] = {

    def createUnionWith(
        sketch1: Sketch[S],
        sketch2: Sketch[S]): UnionTupleAggregationBuffer[S] = {
      val summarySetOps = createSummarySetOperations()
      val union = new Union[S](1 << lgNomEntriesInput, summarySetOps)
      union.union(sketch1)
      union.union(sketch2)
      UnionTupleAggregationBuffer(union)
    }

    (updateBuffer, input) match {
      case (UnionTupleAggregationBuffer(union), buffer: UpdatableTupleSketchBuffer[_, S]) =>
        union.union(buffer.compactSketch)
        UnionTupleAggregationBuffer(union)

      case (UnionTupleAggregationBuffer(union), FinalizedTupleSketch(sketch)) =>
        union.union(sketch)
        UnionTupleAggregationBuffer(union)

      case (UnionTupleAggregationBuffer(union1), UnionTupleAggregationBuffer(union2)) =>
        union1.union(union2.getResult)
        UnionTupleAggregationBuffer(union1)

      case (
            buffer1: UpdatableTupleSketchBuffer[_, S],
            buffer2: UpdatableTupleSketchBuffer[_, S]) =>
        createUnionWith(buffer1.compactSketch, buffer2.compactSketch)

      case (buffer: UpdatableTupleSketchBuffer[_, S], FinalizedTupleSketch(sketch)) =>
        createUnionWith(buffer.compactSketch, sketch)

      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a CompactSketch binary derived from the input column or expression
   *
   * @param sketchState
   *   Union or UpdatableSketch instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def eval(sketchState: TupleSketchState[S]): Any = sketchState.eval()

  /**
   * Returns a CompactSketch binary derived from the input column or expression
   *
   * @param sketchState
   *   Union or UpdatableSketch instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def serialize(sketchState: TupleSketchState[S]): Array[Byte] =
    sketchState.serialize()
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(key, summary, lgNomEntries, mode) - Returns the TupleSketch compact binary representation.
      `key` is the expression for unique value counting.
      `summary` is the double value to be aggregated.
      `lgNomEntries` is the log-base-2 of nominal entries, with nominal entries deciding
      the number buckets or slots for the TupleSketch. Default is 12.
      `mode` is the aggregation mode for numeric summaries (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(key, summary, 12, 'sum')) FROM VALUES (1, 5.0D), (1, 1.0D), (2, 2.0D), (2, 3.0D), (3, 2.2D) tab(key, summary);
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleSketchAggDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("key"),
    InputParameter("summary"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleSketchAggDouble(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(key, summary, lgNomEntries, mode) - Returns the TupleSketch compact binary representation.
      `key` is the expression for unique value counting.
      `summary` is the integer value to be aggregated.
      `lgNomEntries` is the log-base-2 of nominal entries, with nominal entries deciding
      the number buckets or slots for the TupleSketch. Default is 12.
      `mode` is the aggregation mode for numeric summaries (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(key, summary, 12, 'sum')) FROM VALUES (1, 5), (1, 1), (2, 2), (2, 3), (3, 2) tab(key, summary);
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleSketchAggIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("key"),
    InputParameter("summary"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleSketchAggInteger(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}
