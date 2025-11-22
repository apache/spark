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

import org.apache.datasketches.tuple.{Intersection, Sketch, Summary, Union, UpdatableSketch, UpdatableSketchBuilder, UpdatableSummary}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.{QuaternaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, ThetaSketchUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

sealed trait TupleSketchState {
  def serialize(): Array[Byte]
  def eval(): Array[Byte]
}
case class UpdatableTupleSketchBuffer[U, S <: UpdatableSummary[U]](sketch: UpdatableSketch[U, S])
    extends TupleSketchState {
  override def serialize(): Array[Byte] = sketch.compact.toByteArray
  override def eval(): Array[Byte] = sketch.compact.toByteArray
}
case class UnionTupleAggregationBuffer[S <: Summary](union: Union[S]) extends TupleSketchState {
  override def serialize(): Array[Byte] = union.getResult.toByteArray
  override def eval(): Array[Byte] = union.getResult.toByteArray
}
case class IntersectionTupleAggregationBuffer[S <: Summary](intersection: Intersection[S])
    extends TupleSketchState {
  override def serialize(): Array[Byte] = intersection.getResult.toByteArray
  override def eval(): Array[Byte] = intersection.getResult.toByteArray
}
case class FinalizedTupleSketch[S <: Summary](sketch: Sketch[S]) extends TupleSketchState {
  override def serialize(): Array[Byte] = sketch.toByteArray
  override def eval(): Array[Byte] = sketch.toByteArray
}

/**
 * The TupleSketchAgg function utilizes a Datasketches TupleSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column with associated
 * summary values, and outputs the binary representation of the TupleSketch.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (struct with key and summary value) against which unique counting will occur
 * @param lgNomEntriesExpr
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param summaryType
 *   the type of summary (double, integer, string)
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
    _FUNC_(expr[, lgNomEntries[, summaryType[, mode]]]) - Returns the TupleSketch compact binary representation.
      `expr` should be a struct with key and summary value fields.
      `lgNomEntries` (optional) is the log-base-2 of nominal entries, with nominal entries deciding
      the number buckets or slots for the TupleSketch. Default is 12.
      `summaryType` (optional) is the type of summary (double, integer, string). Default is double.
      `mode` (optional) is the aggregation mode for numeric summaries (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(struct(col, 1.0D), 12, 'double', 'sum')) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchAgg(
    child: Expression,
    lgNomEntriesExpr: Option[Expression],
    summaryTypeExpr: Expression,
    modeExpr: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with TupleSketchAggregateBase
    with QuaternaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(
      child,
      Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS)),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntriesExpr: Expression) = {
    this(
      child,
      Some(lgNomEntriesExpr),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntriesExpr: Expression, summaryTypeExpr: Expression) = {
    this(child, Some(lgNomEntriesExpr), summaryTypeExpr, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(
      child: Expression,
      lgNomEntriesExpr: Expression,
      summaryTypeExpr: Expression,
      modeExpr: Expression) = {
    this(child, Some(lgNomEntriesExpr), summaryTypeExpr, modeExpr, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): TupleSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleSketchAgg =
    copy(
      child = newFirst,
      lgNomEntriesExpr = Some(newSecond),
      summaryTypeExpr = newThird,
      modeExpr = newFourth)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      StructType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }

    val structCheck = checkStructFieldCount()
    if (structCheck.isFailure) {
      return structCheck
    }

    val lgNomEntriesCheck = checkLgNomEntriesParameter()
    if (lgNomEntriesCheck.isFailure) {
      return lgNomEntriesCheck
    }

    val summaryTypeCheck = checkSummaryTypeParameter()
    if (summaryTypeCheck.isFailure) {
      return summaryTypeCheck
    }

    checkModeParameter()
  }

  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression =
    lgNomEntriesExpr.getOrElse(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))
  override def third: Expression = summaryTypeExpr
  override def fourth: Expression = modeExpr

  /**
   * Extract and cache the key and summary value types from the input struct. Field 0 is the key
   * type, Field 1 is the summary value type.
   *
   * Note: The asInstanceOf[StructType] cast is safe because inputTypes enforces that the first
   * parameter must be StructType. This is validated during query analysis before execution.
   */
  private lazy val structType = child.dataType.asInstanceOf[StructType]
  private lazy val keyType = structType.fields(0).dataType
  private lazy val valueType = structType.fields(1).dataType

  /**
   * Factory for creating summary objects based on the input summary type and aggregation mode.
   */
  private lazy val summaryFactoryInput =
    ThetaSketchUtils.getSummaryFactory(summaryTypeInput, modeInput)

  /**
   * Instantiate an UpdatableSketch instance using the lgNomEntries param and summary factory.
   *
   * @return
   *   an UpdatableSketch instance wrapped with UpdatableTupleSketchBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    val builder = new UpdatableSketchBuilder[Any, UpdatableSummary[Any]](summaryFactoryInput)
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
  override def update(updateBuffer: TupleSketchState, input: InternalRow): TupleSketchState = {
    // Return early for null values.
    val structValue = child.eval(input)
    if (structValue == null) return updateBuffer

    // Safe: child.eval() returns InternalRow when child.dataType is StructType
    val struct = structValue.asInstanceOf[InternalRow]
    val key = struct.get(0, this.keyType)
    val summaryValue = struct.get(1, this.valueType)

    if (key == null || summaryValue == null) return updateBuffer

    // Initialized buffer should be UpdatableTupleSketchBuffer, else error out.
    val sketch = updateBuffer match {
      case UpdatableTupleSketchBuffer(s) => s
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    // Convert summary value based on summaryTypeInput.
    val summary = ThetaSketchUtils.convertSummaryValue(summaryTypeInput, summaryValue, prettyName)

    // Handle the different data types for sketch updates.
    this.keyType match {
      case ArrayType(IntegerType, _) =>
        val arr = key.asInstanceOf[ArrayData].toIntArray()
        sketch.update(arr, summary)
      case ArrayType(LongType, _) =>
        val arr = key.asInstanceOf[ArrayData].toLongArray()
        sketch.update(arr, summary)
      case BinaryType =>
        val bytes = key.asInstanceOf[Array[Byte]]
        sketch.update(bytes, summary)
      case DoubleType =>
        sketch.update(key.asInstanceOf[Double], summary)
      case FloatType =>
        sketch.update(key.asInstanceOf[Float].toDouble, summary)
      case IntegerType =>
        sketch.update(key.asInstanceOf[Int].toLong, summary)
      case LongType =>
        sketch.update(key.asInstanceOf[Long], summary)
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val str = key.asInstanceOf[UTF8String]
        if (!collation.equalsFunction(str, UTF8String.EMPTY_UTF8)) {
          sketch.update(collation.sortKeyFunction.apply(str), summary)
        }
      case _ =>
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3121",
          messageParameters = Map("dataType" -> child.dataType.toString))
    }

    UpdatableTupleSketchBuffer(sketch)
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
      updateBuffer: TupleSketchState,
      input: TupleSketchState): TupleSketchState = {

    def createUnionWith(
        sketch1: Sketch[Summary],
        sketch2: Sketch[Summary]): UnionTupleAggregationBuffer[Summary] = {
      val summarySetOps = ThetaSketchUtils.getSummarySetOperations(summaryTypeInput, modeInput)
      val union = new Union(1 << lgNomEntriesInput, summarySetOps)
      union.union(sketch1)
      union.union(sketch2)
      UnionTupleAggregationBuffer(union)
    }

    // Note: The asInstanceOf[Sketch[Summary]] casts below are safe because:
    // 1. All sketches in a single aggregate are created with the same summaryTypeInput
    // 2. This ensures type consistency (all DoubleSummary, all IntegerSummary, or all
    //    ArrayOfStringsSummary)
    // 3. We use type erasure to handle all summary types in one method rather than
    //    duplicating code for each concrete summary type
    (updateBuffer, input) match {
      case (UnionTupleAggregationBuffer(union), UpdatableTupleSketchBuffer(sketch)) =>
        union.union(sketch.compact.asInstanceOf[Sketch[Summary]])
        UnionTupleAggregationBuffer(union)

      case (UnionTupleAggregationBuffer(union), FinalizedTupleSketch(sketch)) =>
        union.union(sketch)
        UnionTupleAggregationBuffer(union)

      case (UnionTupleAggregationBuffer(union1), UnionTupleAggregationBuffer(union2)) =>
        union1.union(union2.getResult)
        UnionTupleAggregationBuffer(union1)

      case (UpdatableTupleSketchBuffer(sketch1), UpdatableTupleSketchBuffer(sketch2)) =>
        createUnionWith(
          sketch1.compact().asInstanceOf[Sketch[Summary]],
          sketch2.compact().asInstanceOf[Sketch[Summary]])

      case (UpdatableTupleSketchBuffer(sketch1), FinalizedTupleSketch(sketch2)) =>
        createUnionWith(sketch1.compact().asInstanceOf[Sketch[Summary]], sketch2)

      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a CompactSketch derived from the input column or expression
   *
   * @param sketchState
   *   Union or UpdatableSketch instance used as an aggregation buffer
   * @return
   *   A CompactSketch binary representation
   */
  override def eval(sketchState: TupleSketchState): Any = {
    sketchState.eval()
  }

  /** Convert the underlying UpdatableSketch/Union into a CompactSketch byte array. */
  override def serialize(sketchState: TupleSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Wrap the byte array into a CompactSketch instance. */
  override def deserialize(buffer: Array[Byte]): TupleSketchState = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        ThetaSketchUtils.heapifyTupleSketch(buffer, summaryTypeInput, prettyName))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleUnionAgg function unions multiple TupleSketch binary representations to produce a
 * single merged TupleSketch. This is useful for combining pre-aggregated TupleSketch results from
 * different partitions or data sources.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation) to be unioned
 * @param lgNomEntriesExpr
 *   the log-base-2 of nomEntries decides the number of buckets for the union operation
 * @param summaryType
 *   the type of summary in the input sketches (double, integer, string)
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
    _FUNC_(expr[, lgNomEntries[, summaryType[, mode]]]) - Returns the unioned TupleSketch compact binary representation.
      `expr` should be a binary TupleSketch representation.
      `lgNomEntries` (optional) is the log-base-2 of nominal entries for the union operation. Default is 12.
      `summaryType` (optional) is the type of summary in the sketches (double, integer, string). Default is double.
      `mode` (optional) is the aggregation mode for numeric summaries during union (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg(struct(col, 1.0D)) as sketch FROM VALUES (1), (2), (3) tab(col) UNION ALL SELECT tuple_sketch_agg(struct(col, 1.0D)) as sketch FROM VALUES (2), (3), (4) tab(col));
       4.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleUnionAgg(
    child: Expression,
    lgNomEntriesExpr: Option[Expression],
    summaryTypeExpr: Expression,
    modeExpr: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with TupleSketchAggregateBase
    with QuaternaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(
      child,
      Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS)),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntriesExpr: Expression) = {
    this(
      child,
      Some(lgNomEntriesExpr),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, lgNomEntriesExpr: Expression, summaryTypeExpr: Expression) = {
    this(child, Some(lgNomEntriesExpr), summaryTypeExpr, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(
      child: Expression,
      lgNomEntriesExpr: Expression,
      summaryTypeExpr: Expression,
      modeExpr: Expression) = {
    this(child, Some(lgNomEntriesExpr), summaryTypeExpr, modeExpr, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): TupleUnionAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleUnionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionAgg =
    copy(
      child = newFirst,
      lgNomEntriesExpr = Some(newSecond),
      summaryTypeExpr = newThird,
      modeExpr = newFourth)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_union_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }

    val lgNomEntriesCheck = checkLgNomEntriesParameter()
    if (lgNomEntriesCheck.isFailure) {
      return lgNomEntriesCheck
    }

    val summaryTypeCheck = checkSummaryTypeParameter()
    if (summaryTypeCheck.isFailure) {
      return summaryTypeCheck
    }

    checkModeParameter()
  }

  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression =
    lgNomEntriesExpr.getOrElse(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))
  override def third: Expression = summaryTypeExpr
  override def fourth: Expression = modeExpr

  private lazy val summarySetOperationsInput =
    ThetaSketchUtils.getSummarySetOperations(summaryTypeInput, modeInput)

  /**
   * Instantiate a Union instance using the lgNomEntries param and summary set operations.
   *
   * @return
   *   a Union instance wrapped with UnionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    val union = new Union(1 << lgNomEntriesInput, summarySetOperationsInput)
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
  override def update(unionBuffer: TupleSketchState, input: InternalRow): TupleSketchState = {
    // Get the binary sketch from the input
    val sketchBytes = child.eval(input)

    // Return early for null values
    if (sketchBytes == null) return unionBuffer

    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val inputSketch = ThetaSketchUtils.heapifyTupleSketch(bytes, summaryTypeInput, prettyName)

    val union = unionBuffer match {
      case UnionTupleAggregationBuffer(existingUnion) => existingUnion
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    // Merge it with the buffer
    union.union(inputSketch)
    UnionTupleAggregationBuffer(union)
  }

  /**
   * Merges an input TupleSketch state into the Union aggregation buffer.
   *
   * @param unionBuffer
   *   The Union instance used to store the aggregation result
   * @param input
   *   An input Union or CompactSketch instance
   */
  override def merge(unionBuffer: TupleSketchState, input: TupleSketchState): TupleSketchState = {

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
  override def eval(sketchState: TupleSketchState): Any = {
    sketchState.eval()
  }

  /** Convert the underlying Union into a CompactSketch byte array. */
  override def serialize(sketchState: TupleSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Deserialize a byte array into a CompactSketch instance. */
  override def deserialize(buffer: Array[Byte]): TupleSketchState = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        ThetaSketchUtils.heapifyTupleSketch(buffer, summaryTypeInput, prettyName))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The TupleIntersectionAgg function computes the intersection of multiple TupleSketch binary
 * representations to produce a single TupleSketch containing only the elements common to all
 * input sketches. This is useful for finding overlapping unique values across different datasets.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param child
 *   child expression (binary TupleSketch representation) to be intersected
 * @param summaryType
 *   the type of summary in the input sketches (double, integer, string)
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
    _FUNC_(expr[, summaryType[, mode]]) - Returns the intersected TupleSketch compact binary representation.
      `expr` should be a binary TupleSketch representation.
      `summaryType` (optional) is the type of summary in the sketches (double, integer, string). Default is double.
      `mode` (optional) is the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone). Default is sum. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(sketch)) FROM (SELECT tuple_sketch_agg(struct(col, 1.0D)) as sketch FROM VALUES (1), (2), (3) tab(col) UNION ALL SELECT tuple_sketch_agg(struct(col, 1.0D)) as sketch FROM VALUES (2), (3), (4) tab(col));
       2.0
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionAgg(
    child: Expression,
    summaryTypeExpr: Expression,
    modeExpr: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with TupleSketchAggregateBase
    with TernaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(
      child,
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM),
      0,
      0)
  }

  def this(child: Expression, summaryTypeExpr: Expression) = {
    this(child, summaryTypeExpr, Literal(ThetaSketchUtils.MODE_SUM), 0, 0)
  }

  def this(child: Expression, summaryTypeExpr: Expression, modeExpr: Expression) = {
    this(child, summaryTypeExpr, modeExpr, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): TupleIntersectionAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): TupleIntersectionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionAgg =
    copy(child = newFirst, summaryTypeExpr = newSecond, modeExpr = newThird)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_intersection_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }

    val summaryTypeCheck = checkSummaryTypeParameter()
    if (summaryTypeCheck.isFailure) {
      return summaryTypeCheck
    }

    checkModeParameter()
  }

  override def nullable: Boolean = false

  // lgNomEntriesExpr is not used in Tuple Sketch intersection.
  override val lgNomEntriesExpr: Option[Expression] = None

  override def first: Expression = child
  override def second: Expression = summaryTypeExpr
  override def third: Expression = modeExpr

  private lazy val summarySetOperationsInput =
    ThetaSketchUtils.getSummarySetOperations(summaryTypeInput, modeInput)

  /**
   * Instantiate an Intersection instance using the summary set operations.
   *
   * @return
   *   an Intersection instance wrapped with IntersectionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    val intersection = new Intersection(summarySetOperationsInput)
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
      intersectionBuffer: TupleSketchState,
      input: InternalRow): TupleSketchState = {
    // Get the binary sketch from the input
    val sketchBytes = child.eval(input)

    // Return early for null values
    if (sketchBytes == null) return intersectionBuffer

    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val inputSketch = ThetaSketchUtils.heapifyTupleSketch(bytes, summaryTypeInput, prettyName)

    val intersection = intersectionBuffer match {
      case IntersectionTupleAggregationBuffer(existingIntersection) => existingIntersection
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    // Merge it with the buffer
    intersection.intersect(inputSketch)
    IntersectionTupleAggregationBuffer(intersection)
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
      intersectionBuffer: TupleSketchState,
      input: TupleSketchState): TupleSketchState = {

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
  override def eval(sketchState: TupleSketchState): Any = {
    sketchState.eval()
  }

  /** Convert the underlying Intersection into a CompactSketch byte array. */
  override def serialize(sketchState: TupleSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Deserialize a byte array into a CompactSketch instance. */
  override def deserialize(buffer: Array[Byte]): TupleSketchState = {
    if (buffer.nonEmpty) {
      FinalizedTupleSketch(
        ThetaSketchUtils.heapifyTupleSketch(buffer, summaryTypeInput, prettyName))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * Base trait for TupleSketch aggregation functions that provides common functionality for
 * parameter validation.
 */
trait TupleSketchAggregateBase extends Expression {

  /** Optional log-base-2 of nominal entries (determines sketch size). */
  val lgNomEntriesExpr: Option[Expression]

  /** Summary type specification (double, integer, or string). */
  val summaryTypeExpr: Expression

  /** Aggregation mode for numeric summaries (sum, min, max, alwaysone). */
  val modeExpr: Expression

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /** Child expression for struct input validation (only used by TupleSketchAgg). */
  def child: Expression

  /**
   * Validates that lgNomEntries parameter is a constant and within valid range (4-26).
   */
  protected def checkLgNomEntriesParameter(): TypeCheckResult = {
    lgNomEntriesExpr match {
      case Some(expr) =>
        if (!expr.foldable) {
          return DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> "lgNomEntries",
              "inputType" -> "int",
              "inputExpr" -> expr.sql))
        } else if (expr.eval() == null) {
          return DataTypeMismatch(
            errorSubClass = "UNEXPECTED_NULL",
            messageParameters = Map("exprName" -> "lgNomEntries"))
        } else {
          val lgNomEntriesVal = expr.eval().asInstanceOf[Int]
          try {
            ThetaSketchUtils.checkLgNomLongs(lgNomEntriesVal, prettyName)
          } catch {
            case e: Exception =>
              return TypeCheckResult.TypeCheckFailure(e.getMessage)
          }
        }
      case None => // OK, will use default
    }
    TypeCheckResult.TypeCheckSuccess
  }

  /**
   * Validates that summaryType parameter is a constant string (double, integer, or string).
   */
  protected def checkSummaryTypeParameter(): TypeCheckResult = {
    if (!summaryTypeExpr.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "summaryType",
          "inputType" -> "string",
          "inputExpr" -> summaryTypeExpr.sql))
    } else if (summaryTypeExpr.eval() == null) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "summaryType"))
    } else {
      val summaryTypeStr = summaryTypeExpr.eval().asInstanceOf[UTF8String].toString
      try {
        ThetaSketchUtils.checkSummaryType(summaryTypeStr, prettyName)
      } catch {
        case e: Exception =>
          return TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  /**
   * Validates that mode parameter is a constant string (sum, min, max, alwaysone).
   */
  protected def checkModeParameter(): TypeCheckResult = {
    if (!modeExpr.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "mode",
          "inputType" -> "string",
          "inputExpr" -> modeExpr.sql))
    } else if (modeExpr.eval() == null) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "mode"))
    } else {
      val modeStr = modeExpr.eval().asInstanceOf[UTF8String].toString
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
   * Validates that the child struct has exactly 2 fields (key and summary value).
   * Only used by TupleSketchAgg.
   */
  protected def checkStructFieldCount(): TypeCheckResult = {
    child.dataType match {
      case st: StructType if st.fields.length != 2 =>
        TypeCheckResult.TypeCheckFailure(
          s"The struct input to $prettyName must have exactly 2 fields (key, summary value), " +
          s"but got ${st.fields.length} fields")
      case _ =>
        TypeCheckResult.TypeCheckSuccess
    }
  }

  /**
   * Validates and extracts the lgNomEntries parameter value. Ensures the value is a constant and
   * within valid range (4-26). Defaults to 12 if not specified.
   */
  protected lazy val lgNomEntriesInput: Int = {
    lgNomEntriesExpr match {
      case Some(expr) =>
        val lgNomEntriesVal = expr.eval().asInstanceOf[Int]
        lgNomEntriesVal
      case None => ThetaSketchUtils.DEFAULT_LG_NOM_LONGS
    }
  }

  /**
   * Validates and extracts the summary type parameter value. Ensures the value is a constant
   * string (double, integer, or string). Defaults to "double" if not specified.
   */
  protected lazy val summaryTypeInput: String = {
    summaryTypeExpr.eval().asInstanceOf[UTF8String].toString
  }

  /**
   * Validates and extracts the aggregation mode parameter value. Ensures the value is a constant
   * string and one of: sum, min, max, alwaysone. Defaults to "sum" if not specified.
   */
  protected lazy val modeInput: String = {
    modeExpr.eval().asInstanceOf[UTF8String].toString
  }
}
