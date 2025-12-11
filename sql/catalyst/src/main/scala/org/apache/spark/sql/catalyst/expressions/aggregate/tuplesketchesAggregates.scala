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

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.tuple.{Intersection, Sketch, Sketches, Summary, SummaryFactory, SummarySetOperations, Union, UpdatableSketch, UpdatableSketchBuilder, UpdatableSummary}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer, IntegerSummaryFactory, IntegerSummarySetOperations}
import org.apache.datasketches.tuple.strings.{ArrayOfStringsSummary, ArrayOfStringsSummaryDeserializer, ArrayOfStringsSummaryFactory, ArrayOfStringsSummarySetOperations}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, QuaternaryLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, ThetaSketchUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

sealed trait TupleSketchState[S <: Summary] {
  def serialize(): Array[Byte]
  def eval(): Array[Byte]
}
case class UpdatableTupleSketchBuffer[U, S <: UpdatableSummary[U]](sketch: UpdatableSketch[U, S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = sketch.compact.toByteArray
  override def eval(): Array[Byte] = sketch.compact.toByteArray

  /** Returns compact form of the sketch, needed for merge operations that require Sketch type. */
  def compactSketch: Sketch[S] = sketch.compact
}
case class UnionTupleAggregationBuffer[S <: Summary](union: Union[S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = union.getResult.toByteArray
  override def eval(): Array[Byte] = union.getResult.toByteArray
}
case class IntersectionTupleAggregationBuffer[S <: Summary](intersection: Intersection[S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = intersection.getResult.toByteArray
  override def eval(): Array[Byte] = intersection.getResult.toByteArray
}
case class FinalizedTupleSketch[S <: Summary](sketch: Sketch[S]) extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = sketch.toByteArray
  override def eval(): Array[Byte] = sketch.toByteArray
}

/**
 * The TupleSketchAggDouble function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * double type summary values, and outputs the binary representation of the TupleSketch.
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
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression) = {
    this(key, summary, lgNomEntries, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
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
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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

  /** Specifies accepted summary input types (double and float). */
  override protected def summaryInputType: AbstractDataType =
    TypeCollection(DoubleType, FloatType)

  /**
   * Creates a DoubleSummaryFactory with the configured aggregation mode.
   *
   * @return
   *   a DoubleSummaryFactory instance configured with the aggregation mode
   */
  override protected def createSummaryFactory(): SummaryFactory[DoubleSummary] = {
    val mode = ThetaSketchUtils.getDoubleSummaryMode(modeInput)
    new DoubleSummaryFactory(mode)
  }

  /**
   * Creates DoubleSummarySetOperations for merge operations with the configured mode.
   *
   * @return
   *   a DoubleSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[DoubleSummary] = {
    val mode = ThetaSketchUtils.getDoubleSummaryMode(modeInput)
    new DoubleSummarySetOperations(mode)
  }

  /**
   * Converts Float inputs to Double, ensuring compatibility with DoubleSummary.
   *
   * @param input
   *   the input value to normalize (Float or Double)
   * @return
   *   the normalized Double value
   */
  override protected def normalizeSummaryValue(input: Any): java.lang.Double = {
    input match {
      case d: Double => d
      case f: Float => f.toDouble
      case _ =>
        val actualType = input.getClass.getSimpleName
        throw QueryExecutionErrors.tupleInvalidSummaryValueType(prettyName, actualType)
    }
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
        Sketches.heapifySketch(Memory.wrap(buffer), new DoubleSummaryDeserializer()))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleSketchAggInteger function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * integer type summary values, and outputs the binary representation of the TupleSketch.
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
case class TupleSketchAggInteger(
    key: Expression,
    summary: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleSketchAggBase[java.lang.Integer, IntegerSummary]
    with QuaternaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(key: Expression, summary: Expression) = {
    this(
      key,
      summary,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression) = {
    this(key, summary, lgNomEntries, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
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
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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
   *
   * @return
   *   an IntegerSummaryFactory instance configured with the aggregation mode
   */
  override protected def createSummaryFactory(): SummaryFactory[IntegerSummary] = {
    val mode = ThetaSketchUtils.getIntegerSummaryMode(modeInput)
    new IntegerSummaryFactory(mode)
  }

  /**
   * Creates IntegerSummarySetOperations for merge operations with the configured mode.
   *
   * @return
   *   an IntegerSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[IntegerSummary] = {
    val mode = ThetaSketchUtils.getIntegerSummaryMode(modeInput)
    new IntegerSummarySetOperations(mode, mode)
  }

  /**
   * Ensures compatibility with IntegerSummary.
   *
   * @param input
   *   the input value to normalize (Integer)
   * @return
   *   the normalized Integer value
   */
  override protected def normalizeSummaryValue(input: Any): Integer = {
    input match {
      case i: Int => i
      case _ =>
        val actualType = input.getClass.getSimpleName
        throw QueryExecutionErrors.tupleInvalidSummaryValueType(prettyName, actualType)
    }
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
        Sketches.heapifySketch(Memory.wrap(buffer), new IntegerSummaryDeserializer()))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleSketchAggString function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * string or string array type summary values, and outputs the binary representation of the
 * TupleSketch.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param key
 *   key expression against which unique counting will occur
 * @param summary
 *   summary expression (string or array of strings) to be collected
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(key, summary, lgNomEntries) - Returns the TupleSketch compact binary representation.
      `key` is the expression for unique value counting.
      `summary` is the string or array of strings to be collected.
      `lgNomEntries` is the log-base-2 of nominal entries, with nominal entries deciding
      the number buckets or slots for the TupleSketch. Default is 12. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_string(_FUNC_(key, summary, 12)) FROM VALUES (1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (3, 'e') tab(key, summary);
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchAggString(
    key: Expression,
    summary: Expression,
    lgNomEntries: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleSketchAggBase[Array[String], ArrayOfStringsSummary]
    with TernaryLike[Expression] {

  // Constructors
  def this(key: Expression, summary: Expression) = {
    this(key, summary, Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS), 0, 0)
  }

  def this(key: Expression, summary: Expression, lgNomEntries: Expression) = {
    this(key, summary, lgNomEntries, 0, 0)
  }

  /**
   * Override inputTypes to specify key, summary (string or array of strings), and lgNomEntries
   * (int) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(keyInputTypes, summaryInputType, IntegerType)

  /**
   * Override checkInputDataTypes to validate base inputs (key, summary, lgNomEntries) only.
   */
  override def checkInputDataTypes(): TypeCheckResult = checkBaseInputDataTypes()

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleSketchAggString =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleSketchAggString =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleSketchAggString =
    copy(key = newFirst, summary = newSecond, lgNomEntries = newThird)

  override def first: Expression = key
  override def second: Expression = summary
  override def third: Expression = lgNomEntries

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_sketch_agg_string"

  /** Specifies accepted summary input types (string or array of strings). */
  override protected def summaryInputType: AbstractDataType =
    TypeCollection(StringTypeWithCollation(supportsTrimCollation = true), ArrayType(StringType))

  /**
   * Creates an ArrayOfStringsSummaryFactory. Aggregation mode is not supported here.
   *
   * @return
   *   an ArrayOfStringsSummaryFactory instance
   */
  override protected def createSummaryFactory(): SummaryFactory[ArrayOfStringsSummary] = {
    new ArrayOfStringsSummaryFactory()
  }

  /**
   * Creates ArrayOfStringsSummarySetOperations for merge operations. Aggregation mode is not
   * supported here.
   *
   * @return
   *   an ArrayOfStringsSummarySetOperations instance
   */
  override protected def createSummarySetOperations()
      : SummarySetOperations[ArrayOfStringsSummary] = {
    new ArrayOfStringsSummarySetOperations()
  }

  /**
   * Converts String inputs to String Arrays, ensuring compatibility with ArrayOfStringsSummary.
   *
   * @param input
   *   the input value to normalize (UTF8String or ArrayData)
   * @return
   *   the normalized Array[String] value
   */
  override protected def normalizeSummaryValue(input: Any): Array[String] = {
    input match {
      case str: UTF8String =>
        Array(str.toString)
      case arr: ArrayData =>
        (0 until arr.numElements())
          .filter(i => !arr.isNullAt(i))
          .map(i => arr.getUTF8String(i).toString)
          .toArray
      case _ =>
        val actualType = input.getClass.getSimpleName
        throw QueryExecutionErrors.tupleInvalidSummaryValueType(prettyName, actualType)
    }
  }

  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[ArrayOfStringsSummary] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        Sketches.heapifySketch(Memory.wrap(buffer), new ArrayOfStringsSummaryDeserializer()))
    } else {
      this.createAggregationBuffer()
    }
  }
}

abstract class TupleSketchAggBase[U, S <: UpdatableSummary[U]]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with SketchSize
    with ExpectsInputTypes {

  // Abstract methods that subclasses must implement
  protected def summaryInputType: AbstractDataType
  protected def normalizeSummaryValue(input: Any): U
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
    if (defaultCheck.isFailure) return defaultCheck

    checkLgNomEntriesParameter()
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
    if (keyValue == null || summaryValue == null) return updateBuffer

    /**
     * Normalize summary to a datasketch supported type if possible. Type checking is already done
     * at this point.
     */
    val normalizedSummary = normalizeSummaryValue(summaryValue)

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
case class TupleUnionAggDouble(
    child: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleUnionAggBase[DoubleSummary]
    with TernaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(child: Expression) = {
    this(
      child,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(child, lgNomEntries, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify sketch binary (BinaryType), lgNomEntries (int), and mode
   * (string) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, IntegerType, StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate base inputs (sketch binary, lgNomEntries) and mode
   * parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = checkBaseInputDataTypes()
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_union_agg_double"

  /**
   * Creates DoubleSummarySetOperations for merge operations.
   *
   * @return
   *   a DoubleSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[DoubleSummary] = {
    val mode = ThetaSketchUtils.getDoubleSummaryMode(modeInput)
    new DoubleSummarySetOperations(mode)
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
    ThetaSketchUtils.heapifyDoubleTupleSketch(buffer, prettyName)
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
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
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
case class TupleUnionAggInteger(
    child: Expression,
    lgNomEntries: Expression,
    mode: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleUnionAggBase[IntegerSummary]
    with TernaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(child: Expression) = {
    this(
      child,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, mode: Expression) = {
    this(child, lgNomEntries, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify sketch binary (BinaryType), lgNomEntries (int), and mode
   * (string) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, IntegerType, StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate base inputs (sketch binary, lgNomEntries) and mode
   * parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = checkBaseInputDataTypes()
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_union_agg_integer"

  /**
   * Creates IntegerSummarySetOperations for merge operations.
   *
   * @return
   *   an IntegerSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[IntegerSummary] = {
    val mode = ThetaSketchUtils.getIntegerSummaryMode(modeInput)
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
    ThetaSketchUtils.heapifyIntegerTupleSketch(buffer, prettyName)
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
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleUnionAggString function unions multiple TupleSketch binary representations with a
 * string or string array type summary to produce a single merged TupleSketch. This is useful for
 * combining pre-aggregated TupleSketch results from different partitions or data sources.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with a string or array of strings
 *   summary) to be unioned
 * @param lgNomEntries
 *   the log-base-2 of nomEntries decides the number of buckets for the union operation
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, lgNomEntries) - Returns the unioned TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with a string or array of strings summary.
      `lgNomEntries` is the log-base-2 of nominal entries for the union operation. Default is 12. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_string(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_string(key, summary) as sketch FROM VALUES (1, 'a'), (2, 'b') tab(key, summary) UNION ALL SELECT tuple_sketch_agg_string(key, summary) as sketch FROM VALUES (2, 'c'), (3, 'd') tab(key, summary));
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleUnionAggString(
    child: Expression,
    lgNomEntries: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleUnionAggBase[ArrayOfStringsSummary]
    with BinaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, 0, 0)
  }

  /**
   * Override inputTypes to specify sketch binary (BinaryType) and lgNomEntries (int) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, IntegerType)

  /**
   * Override checkInputDataTypes to validate base inputs (sketch binary, lgNomEntries) only.
   */
  override def checkInputDataTypes(): TypeCheckResult = checkBaseInputDataTypes()

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleUnionAggString =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleUnionAggString =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleUnionAggString =
    copy(child = newLeft, lgNomEntries = newRight)

  override def left: Expression = child
  override def right: Expression = lgNomEntries

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_union_agg_string"

  /**
   * Creates ArrayOfStringsSummarySetOperations for merge operations.
   *
   * @return
   *   an ArrayOfStringsSummarySetOperations instance
   */
  override protected def createSummarySetOperations()
      : SummarySetOperations[ArrayOfStringsSummary] = {
    new ArrayOfStringsSummarySetOperations()
  }

  /**
   * Heapify a sketch from a byte array.
   *
   * @param buffer
   *   the serialized sketch byte array
   * @return
   *   a Sketch[ArrayOfStringsSummary] instance
   */
  override protected def heapifySketch(buffer: Array[Byte]): Sketch[ArrayOfStringsSummary] = {
    ThetaSketchUtils.heapifyStringTupleSketch(buffer, prettyName)
  }

  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[ArrayOfStringsSummary] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
  }
}

abstract class TupleUnionAggBase[S <: Summary]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with SketchSize
    with ExpectsInputTypes {

  // Abstract methods that subclasses must implement
  protected def createSummarySetOperations(): SummarySetOperations[S]
  protected def heapifySketch(buffer: Array[Byte]): Sketch[S]

  // Abstract members that subclasses must implement
  protected def child: Expression

  override def dataType: DataType = BinaryType
  override def nullable: Boolean = false

  protected def checkBaseInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) return defaultCheck

    checkLgNomEntriesParameter()
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
    if (sketchBytes == null) return unionBuffer

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
      case (UnionTupleAggregationBuffer(union), FinalizedTupleSketch(sketch)) =>
        union.union(sketch)
        UnionTupleAggregationBuffer(union)
      // If both arguments are union objects, merge them directly.
      case (UnionTupleAggregationBuffer(union1), UnionTupleAggregationBuffer(union2)) =>
        union1.union(union2.getResult)
        UnionTupleAggregationBuffer(union1)
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
}

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
    with BinaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(child: Expression, mode: Expression) = {
    this(child, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify sketch binary (BinaryType) and mode (string) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate sketch binary input and mode parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_intersection_agg_double"

  /**
   * Creates DoubleSummarySetOperations for intersection operations.
   *
   * @return
   *   a DoubleSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[DoubleSummary] = {
    val mode = ThetaSketchUtils.getDoubleSummaryMode(modeInput)
    new DoubleSummarySetOperations(mode)
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
    ThetaSketchUtils.heapifyDoubleTupleSketch(buffer, prettyName)
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
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
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
    with BinaryLike[Expression]
    with SummaryAggregateMode {

  // Constructors
  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(child: Expression, mode: Expression) = {
    this(child, mode, 0, 0)
  }

  /**
   * Override inputTypes to specify sketch binary (BinaryType) and mode (string) parameters.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  /**
   * Override checkInputDataTypes to validate sketch binary input and mode parameter.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) return defaultCheck

    checkModeParameter()
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

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_intersection_agg_integer"

  /**
   * Creates IntegerSummarySetOperations for intersection operations.
   *
   * @return
   *   an IntegerSummarySetOperations instance configured with the aggregation mode
   */
  override protected def createSummarySetOperations(): SummarySetOperations[IntegerSummary] = {
    val mode = ThetaSketchUtils.getIntegerSummaryMode(modeInput)
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
    ThetaSketchUtils.heapifyIntegerTupleSketch(buffer, prettyName)
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
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleIntersectionAggString function computes the intersection of multiple TupleSketch
 * binary representations with a string or string array type summary to produce a single
 * TupleSketch containing only the elements common to all input sketches. This is useful for
 * finding overlapping unique values across different datasets.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation created with a string or array of strings
 *   summary) to be intersected
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns the intersected TupleSketch compact binary representation.
      `child` should be a binary TupleSketch representation created with a string or array of strings summary. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_string(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg_string(key, summary) as sketch FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') tab(key, summary) UNION ALL SELECT tuple_sketch_agg_string(key, summary) as sketch FROM VALUES (2, 'b'), (3, 'c'), (4, 'd') tab(key, summary));
       2.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionAggString(
    child: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TupleIntersectionAggBase[ArrayOfStringsSummary]
    with UnaryLike[Expression] {

  // Constructors
  def this(child: Expression) = {
    this(child, 0, 0)
  }

  /**
   * Override inputTypes to specify only sketch binary (BinaryType) parameter since string
   * variants don't support mode, and intersection does not use lgNomEntries
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType)

  /**
   * Override checkInputDataTypes to validate only sketch binary input, skipping lgNomEntries and
   * mode validation.
   */
  override def checkInputDataTypes(): TypeCheckResult =
    super.checkInputDataTypes()

  // Copy constructors required by ImperativeAggregate
  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleIntersectionAggString =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): TupleIntersectionAggString =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): TupleIntersectionAggString =
    copy(child = newChild)

  // Overrides for TypedImperativeAggregate
  override def prettyName: String = "tuple_intersection_agg_string"

  /**
   * Creates ArrayOfStringsSummarySetOperations for intersection operations.
   *
   * @return
   *   an ArrayOfStringsSummarySetOperations instance
   */
  override protected def createSummarySetOperations()
      : SummarySetOperations[ArrayOfStringsSummary] = {
    new ArrayOfStringsSummarySetOperations()
  }

  /**
   * Heapify a sketch from a byte array.
   *
   * @param buffer
   *   the serialized sketch byte array
   * @return
   *   a Sketch[ArrayOfStringsSummary] instance
   */
  override protected def heapifySketch(buffer: Array[Byte]): Sketch[ArrayOfStringsSummary] = {
   ThetaSketchUtils.heapifyStringTupleSketch(buffer, prettyName)
  }

  /**
   * Heapify a CompactSketch from the sketch byte array.
   *
   * @param buffer
   *   A serialized sketch byte array
   * @return
   *   A CompactSketch instance wrapped with FinalizedTupleSketch
   */
  override def deserialize(buffer: Array[Byte]): TupleSketchState[ArrayOfStringsSummary] = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(heapifySketch(buffer))
    } else {
      this.createAggregationBuffer()
    }
  }
}

abstract class TupleIntersectionAggBase[S <: Summary]
    extends TypedImperativeAggregate[TupleSketchState[S]]
    with ExpectsInputTypes {

  // Abstract methods that subclasses must implement
  protected def createSummarySetOperations(): SummarySetOperations[S]
  protected def heapifySketch(buffer: Array[Byte]): Sketch[S]

  // Abstract members that subclasses must implement
  protected def child: Expression

  override def dataType: DataType = BinaryType
  override def nullable: Boolean = false

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
    if (sketchBytes == null) return intersectionBuffer

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
      case (IntersectionTupleAggregationBuffer(intersection), FinalizedTupleSketch(sketch)) =>
        intersection.intersect(sketch)
        IntersectionTupleAggregationBuffer(intersection)
      // If both arguments are intersection objects, merge them directly.
      case (
            IntersectionTupleAggregationBuffer(intersection1),
            IntersectionTupleAggregationBuffer(intersection2)) =>
        intersection1.intersect(intersection2.getResult)
        IntersectionTupleAggregationBuffer(intersection1)

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
}

/**
 * Trait for TupleSketch aggregation functions that use the lgNomEntries parameter. Provides
 * validation and extraction functionality for the log-base-2 of nominal entries.
 */
trait SketchSize extends AggregateFunction {

  /** log-base-2 of nominal entries (determines sketch size). */
  def lgNomEntries: Expression

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /**
   * Validates that lgNomEntries parameter is a constant and within valid range (4-26).
   */
  protected def checkLgNomEntriesParameter(): TypeCheckResult = {
    if (!lgNomEntries.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "lgNomEntries",
          "inputType" -> "int",
          "inputExpr" -> lgNomEntries.sql))
    } else if (lgNomEntries.eval() == null) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "lgNomEntries"))
    } else {
      val lgNomEntriesVal = lgNomEntries.eval().asInstanceOf[Int]
      try {
        ThetaSketchUtils.checkLgNomLongs(lgNomEntriesVal, prettyName)
      } catch {
        case e: Exception =>
          return TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  /**
   * Validates and extracts the lgNomEntries parameter value. Ensures the value is a constant and
   * within valid range (4-26)
   */
  protected lazy val lgNomEntriesInput: Int = {
    lgNomEntries.eval().asInstanceOf[Int]
  }
}

/**
 * Trait for TupleSketch aggregation functions that use the mode parameter. Provides validation
 * and extraction functionality for the aggregation mode.
 *
 * Tuple sketches extend Theta sketches by associating summary values with each key. When
 * performing set operations (union, intersection) or building sketches, duplicate keys may appear
 * with different summary values. The mode parameter determines how to combine these values: 'sum'
 * adds them together, 'min' keeps the smallest, 'max' keeps the largest, and 'alwaysone' sets all
 * summary values to 1 (effectively behaving like a Theta sketch).
 */
trait SummaryAggregateMode extends AggregateFunction {

  /** Aggregation mode for numeric summaries (sum, min, max, alwaysone). */
  def mode: Expression

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /**
   * Validates that mode parameter is a constant string (sum, min, max, alwaysone).
   */
  protected def checkModeParameter(): TypeCheckResult = {
    if (!mode.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters =
          Map("inputName" -> "mode", "inputType" -> "string", "inputExpr" -> mode.sql))
    } else if (mode.eval() == null) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "mode"))
    } else {
      val modeStr = mode.eval().asInstanceOf[UTF8String].toString
      try {
        ThetaSketchUtils.checkMode(modeStr, prettyName)
      } catch {
        case e: Exception =>
          return TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  /**
   * Validates and extracts the aggregation mode parameter value. Ensures the value is a constant
   * string and one of: sum, min, max, alwaysone.
   */
  protected lazy val modeInput: String = {
    mode.eval().asInstanceOf[UTF8String].toString
  }
}

/**
 * ExpressionBuilder for TupleSketchAggDouble to support SQL named parameters.
 * Supports 2-4 parameters: key and summary are required, lgNomEntries and mode are optional.
 * The rearrange method will always fill in defaults, so build() receives exactly 4 expressions.
 */
object TupleSketchAggDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("key"),
    InputParameter("summary"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleSketchAggDouble(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

/**
 * ExpressionBuilder for TupleSketchAggInteger to support SQL named parameters.
 * Supports 2-4 parameters: key and summary are required, lgNomEntries and mode are optional.
 * The rearrange method will always fill in defaults, so build() receives exactly 4 expressions.
 */
object TupleSketchAggIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("key"),
    InputParameter("summary"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleSketchAggInteger(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

/**
 * ExpressionBuilder for TupleUnionAggDouble to support SQL named parameters.
 * Supports 1-3 parameters: child is required, lgNomEntries and mode are optional.
 * The rearrange method will always fill in defaults, so build() receives exactly 3 expressions.
 */
object TupleUnionAggDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("child"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 3 with defaults filled in
    assert(expressions.size == 3)
    new TupleUnionAggDouble(expressions(0), expressions(1), expressions(2))
  }
}

/**
 * ExpressionBuilder for TupleUnionAggInteger to support SQL named parameters.
 * Supports 1-3 parameters: child is required, lgNomEntries and mode are optional.
 * The rearrange method will always fill in defaults, so build() receives exactly 3 expressions.
 */
object TupleUnionAggIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("child"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 3 with defaults filled in
    assert(expressions.size == 3)
    new TupleUnionAggInteger(expressions(0), expressions(1), expressions(2))
  }
}
