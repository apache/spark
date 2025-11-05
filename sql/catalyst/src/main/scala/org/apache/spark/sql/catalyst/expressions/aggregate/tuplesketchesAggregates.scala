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

import java.util.Locale

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.tuple.{Intersection, Sketch, Sketches, Summary, SummaryDeserializer, SummaryFactory, SummarySetOperations, Union, UpdatableSketch, UpdatableSketchBuilder, UpdatableSummary}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer, IntegerSummaryFactory, IntegerSummarySetOperations}
import org.apache.datasketches.tuple.strings.{ArrayOfStringsSummary, ArrayOfStringsSummaryDeserializer, ArrayOfStringsSummaryFactory, ArrayOfStringsSummarySetOperations}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.{QuaternaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, ThetaSketchUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TypeCollection}
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
      > SELECT tuple_sketch_estimate(_FUNC_(struct(col, 1.0), 12, 'double', 'sum')) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchAgg(
    child: Expression,
    lgNomEntriesExpr: Option[Expression],
    summaryType: Option[Expression],
    mode: Option[Expression],
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with BaseTupleSketchAgg
    with QuaternaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(child, None, None, None, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, Some(lgNomEntries), None, None, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, summaryType: Expression) = {
    this(child, Some(lgNomEntries), Some(summaryType), None, 0, 0)
  }

  def this(
      child: Expression,
      lgNomEntries: Expression,
      summaryType: Expression,
      mode: Expression) = {
    this(child, Some(lgNomEntries), Some(summaryType), Some(mode), 0, 0)
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
      summaryType = Some(newThird),
      mode = Some(newFourth))

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        ArrayType(IntegerType),
        ArrayType(LongType),
        BinaryType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringTypeWithCollation(supportsTrimCollation = true)),
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression = Literal(lgNomEntriesInput)
  override def third: Expression = Literal(summaryTypeInput)
  override def fourth: Expression = Literal(modeInput)

  /**
   * Extract and cache the key and summary value types from the input struct. Field 0 is the key
   * type, Field 1 is the summary value type.
   */
  private lazy val structType = child.dataType.asInstanceOf[StructType]
  private lazy val keyType = structType.fields(0).dataType
  private lazy val valueType = structType.fields(1).dataType

  /**
   * Instantiate an UpdatableSketch instance using the lgNomEntries param and summary factory.
   *
   * @return
   *   an UpdatableSketch instance wrapped with UpdatableTupleSketchBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        val factory = summaryFactoryInput.asInstanceOf[DoubleSummaryFactory]
        val builder = new UpdatableSketchBuilder[java.lang.Double, DoubleSummary](factory)
        builder.setNominalEntries(1 << lgNomEntriesInput)
        val sketch: UpdatableSketch[java.lang.Double, DoubleSummary] = builder.build()
        UpdatableTupleSketchBuffer(sketch)

      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        val factory = summaryFactoryInput.asInstanceOf[IntegerSummaryFactory]
        val builder = new UpdatableSketchBuilder[java.lang.Integer, IntegerSummary](factory)
        builder.setNominalEntries(1 << lgNomEntriesInput)
        val sketch: UpdatableSketch[java.lang.Integer, IntegerSummary] = builder.build()
        UpdatableTupleSketchBuffer(sketch)

      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        val factory = summaryFactoryInput.asInstanceOf[ArrayOfStringsSummaryFactory]
        val builder = new UpdatableSketchBuilder[Array[String], ArrayOfStringsSummary](factory)
        builder.setNominalEntries(1 << lgNomEntriesInput)
        val sketch: UpdatableSketch[Array[String], ArrayOfStringsSummary] = builder.build()
        UpdatableTupleSketchBuffer(sketch)
    }
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
    val summary = (summaryTypeInput, summaryValue) match {
      case (ThetaSketchUtils.SUMMARY_TYPE_DOUBLE, d: Double) => d
      case (ThetaSketchUtils.SUMMARY_TYPE_DOUBLE, f: Float) => f.toDouble
      case (ThetaSketchUtils.SUMMARY_TYPE_INTEGER, i: Int) => i
      case (ThetaSketchUtils.SUMMARY_TYPE_STRING, s: UTF8String) => s
      case _ =>
        val actualType = summaryValue.getClass.getSimpleName
        throw QueryExecutionErrors
          .tupleInvalidSummaryValueType(prettyName, summaryTypeInput, actualType)
    }

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

    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        mergeTyped[DoubleSummary](updateBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        mergeTyped[IntegerSummary](updateBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        mergeTyped[ArrayOfStringsSummary](updateBuffer, input)
    }
  }

  private def mergeTyped[S <: Summary](
      updateBuffer: TupleSketchState,
      input: TupleSketchState): TupleSketchState = {

    def createUnionWith(
        sketch1: Sketch[S],
        sketch2: Sketch[S]): UnionTupleAggregationBuffer[S] = {
      val factory = summaryFactoryInput.asInstanceOf[SummaryFactory[S]]
      val summarySetOps = factory.newSummary().asInstanceOf[SummarySetOperations[S]]
      val union = new Union[S](1 << lgNomEntriesInput, summarySetOps)
      union.union(sketch1)
      union.union(sketch2)
      UnionTupleAggregationBuffer(union)
    }

    (updateBuffer, input) match {
      case (
            buf: UnionTupleAggregationBuffer[S] @unchecked,
            sketch: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        buf.union.union(sketch.sketch.compact())
        buf

      case (
            buf: UnionTupleAggregationBuffer[S] @unchecked,
            sketch: FinalizedTupleSketch[S] @unchecked) =>
        buf.union.union(sketch.sketch)
        buf

      case (
            union1: UnionTupleAggregationBuffer[S] @unchecked,
            union2: UnionTupleAggregationBuffer[S] @unchecked) =>
        union1.union.union(union2.union.getResult)
        union1

      case (
            sketch1: UpdatableTupleSketchBuffer[_, S] @unchecked,
            sketch2: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        createUnionWith(sketch1.sketch.compact(), sketch2.sketch.compact())

      case (
            sketch1: UpdatableTupleSketchBuffer[_, S] @unchecked,
            sketch2: FinalizedTupleSketch[S] @unchecked) =>
        createUnionWith(sketch1.sketch.compact(), sketch2.sketch)

      case (
            sketch1: FinalizedTupleSketch[S] @unchecked,
            sketch2: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        createUnionWith(sketch1.sketch, sketch2.sketch.compact())

      case (
            sketch1: FinalizedTupleSketch[S] @unchecked,
            sketch2: FinalizedTupleSketch[S] @unchecked) =>
        createUnionWith(sketch1.sketch, sketch2.sketch)

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
    deserializeToCompact(buffer)
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
      > SELECT tuple_sketch_estimate(
          _FUNC_(sketch, 12, 'double', 'sum')
        ) FROM (
          SELECT tuple_sketch_agg(struct(col, 1.0), 12, 'double', 'sum') as sketch
          FROM VALUES (1), (2), (3) tab(col)
          UNION ALL
          SELECT tuple_sketch_agg(struct(col, 1.0), 12, 'double', 'sum') as sketch
          FROM VALUES (2), (3), (4) tab(col)
        );
       4
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleUnionAgg(
    child: Expression,
    lgNomEntriesExpr: Option[Expression],
    summaryType: Option[Expression],
    mode: Option[Expression],
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with BaseTupleSketchAgg
    with QuaternaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(child, None, None, None, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, Some(lgNomEntries), None, None, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression, summaryType: Expression) = {
    this(child, Some(lgNomEntries), Some(summaryType), None, 0, 0)
  }

  def this(
      child: Expression,
      lgNomEntries: Expression,
      summaryType: Expression,
      mode: Expression) = {
    this(child, Some(lgNomEntries), Some(summaryType), Some(mode), 0, 0)
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
      summaryType = Some(newThird),
      mode = Some(newFourth))

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_union_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression = Literal(lgNomEntriesInput)
  override def third: Expression = Literal(summaryTypeInput)
  override def fourth: Expression = Literal(modeInput)

  /**
   * Instantiate a Union instance using the lgNomEntries param and summary set operations.
   *
   * @return
   *   a Union instance wrapped with UnionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        val summarySetOps = summarySetOperationsInput.asInstanceOf[DoubleSummarySetOperations]
        val union = new Union[DoubleSummary](1 << lgNomEntriesInput, summarySetOps)
        UnionTupleAggregationBuffer(union)

      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        val summarySetOps = summarySetOperationsInput
          .asInstanceOf[IntegerSummarySetOperations]
        val union = new Union[IntegerSummary](1 << lgNomEntriesInput, summarySetOps)
        UnionTupleAggregationBuffer(union)

      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        val summarySetOps = summarySetOperationsInput
          .asInstanceOf[ArrayOfStringsSummarySetOperations]
        val union = new Union[ArrayOfStringsSummary](1 << lgNomEntriesInput, summarySetOps)
        UnionTupleAggregationBuffer(union)
    }
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

    child.dataType match {
      case BinaryType => // Continue processing with a BinaryType.
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val inputSketch = deserializeToCompact(bytes)

    val union = unionBuffer match {
      case UnionTupleAggregationBuffer(existingUnion) => existingUnion
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    val sketchToMerge = inputSketch match {
      case FinalizedTupleSketch(sketch) => sketch
      case _ => throw throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    // Merge it with the buffer
    union.union(sketchToMerge)
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

    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        mergeTyped[DoubleSummary](unionBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        mergeTyped[IntegerSummary](unionBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        mergeTyped[ArrayOfStringsSummary](unionBuffer, input)
    }
  }

  private def mergeTyped[S <: Summary](
      unionBuffer: TupleSketchState,
      input: TupleSketchState): TupleSketchState = {

    (unionBuffer, input) match {
      // The input was serialized then deserialized.
      case (
            buf: UnionTupleAggregationBuffer[S] @unchecked,
            sketch: FinalizedTupleSketch[S] @unchecked) =>
        buf.union.union(sketch.sketch)
        buf
      // If both arguments are union objects, merge them directly.
      case (
            union1: UnionTupleAggregationBuffer[S] @unchecked,
            union2: UnionTupleAggregationBuffer[S] @unchecked) =>
        union1.union.union(union2.union.getResult)
        union1

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
    deserializeToCompact(buffer)
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
      > SELECT tuple_sketch_estimate(
          _FUNC_(sketch, 'double', 'sum')
        ) FROM (
          SELECT tuple_sketch_agg(struct(col, 1.0), 12, 'double', 'sum') as sketch
          FROM VALUES (1), (2), (3) tab(col)
          UNION ALL
          SELECT tuple_sketch_agg(struct(col, 1.0), 12, 'double', 'sum') as sketch
          FROM VALUES (2), (3), (4) tab(col)
        );
       2
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionAgg(
    child: Expression,
    summaryType: Option[Expression],
    mode: Option[Expression],
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[TupleSketchState]
    with BaseTupleSketchAgg
    with TernaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  def this(child: Expression) = {
    this(child, None, None, 0, 0)
  }

  def this(child: Expression, summaryType: Expression) = {
    this(child, Some(summaryType), None, 0, 0)
  }

  def this(child: Expression, summaryType: Expression, mode: Expression) = {
    this(child, Some(summaryType), Some(mode), 0, 0)
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
    copy(child = newFirst, summaryType = Some(newSecond), mode = Some(newThird))

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "tuple_intersection_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  // lgNomEntriesExpr is not used in Tuple Sketch intersection.
  override val lgNomEntriesExpr: Option[Expression] = None

  override def first: Expression = child
  override def second: Expression = Literal(summaryTypeInput)
  override def third: Expression = Literal(modeInput)

  /**
   * Instantiate an Intersection instance using the summary set operations.
   *
   * @return
   *   an Intersection instance wrapped with IntersectionTupleAggregationBuffer
   */
  override def createAggregationBuffer(): TupleSketchState = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        val summarySetOps = summarySetOperationsInput
          .asInstanceOf[DoubleSummarySetOperations]
        val intersection = new Intersection[DoubleSummary](summarySetOps)
        IntersectionTupleAggregationBuffer(intersection)

      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        val summarySetOps = summarySetOperationsInput
          .asInstanceOf[IntegerSummarySetOperations]
        val intersection = new Intersection[IntegerSummary](summarySetOps)
        IntersectionTupleAggregationBuffer(intersection)

      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        val summarySetOps = summarySetOperationsInput
          .asInstanceOf[ArrayOfStringsSummarySetOperations]
        val intersection = new Intersection[ArrayOfStringsSummary](summarySetOps)
        IntersectionTupleAggregationBuffer(intersection)
    }
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

    child.dataType match {
      case BinaryType => // Continue processing with a BinaryType.
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val inputSketch = deserializeToCompact(bytes)

    val intersection = intersectionBuffer match {
      case IntersectionTupleAggregationBuffer(existingIntersection) => existingIntersection
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    val sketchToMerge = inputSketch match {
      case FinalizedTupleSketch(sketch) => sketch
      case _ => throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName)
    }

    // Merge it with the buffer
    intersection.intersect(sketchToMerge)
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

    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        mergeTyped[DoubleSummary](intersectionBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        mergeTyped[IntegerSummary](intersectionBuffer, input)
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        mergeTyped[ArrayOfStringsSummary](intersectionBuffer, input)
    }
  }

  private def mergeTyped[S <: Summary](
      intersectionBuffer: TupleSketchState,
      input: TupleSketchState): TupleSketchState = {

    (intersectionBuffer, input) match {
      // The input was serialized then deserialized.
      case (
            buf: IntersectionTupleAggregationBuffer[S] @unchecked,
            sketch: FinalizedTupleSketch[S] @unchecked) =>
        buf.intersection.intersect(sketch.sketch)
        buf
      // If both arguments are intersection objects, merge them directly.
      case (
            intersection1: IntersectionTupleAggregationBuffer[S] @unchecked,
            intersection2: IntersectionTupleAggregationBuffer[S] @unchecked) =>
        intersection1.intersection.intersect(intersection2.intersection.getResult)
        intersection1

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
    deserializeToCompact(buffer)
  }
}

/**
 * Base trait for TupleSketch aggregation functions that provides common functionality for
 * parameter validation, factory creation, and sketch deserialization.
 */
trait BaseTupleSketchAgg {

  /** Child expression containing the input data (struct with key/value or binary sketch). */
  val child: Expression

  /** Optional log-base-2 of nominal entries (determines sketch size). */
  val lgNomEntriesExpr: Option[Expression]

  /** Optional summary type specification (double, integer, or string). */
  val summaryType: Option[Expression]

  /** Optional aggregation mode for numeric summaries (sum, min, max, alwaysone). */
  val mode: Option[Expression]

  /** Returns the pretty name of the aggregation function for error messages. */
  protected def prettyName: String

  /** Creates a new aggregation buffer (UpdatableSketch, Union, or Intersection). */
  protected def createAggregationBuffer(): TupleSketchState

  /**
   * Validates and extracts the lgNomEntries parameter value. Ensures the value is a constant and
   * within valid range (4-26). Defaults to 12 if not specified.
   */
  protected lazy val lgNomEntriesInput: Int = {
    lgNomEntriesExpr match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw QueryExecutionErrors.tupleSketchParameterMustBeConstant(
            prettyName,
            "lgNomEntries")
        }
        val lgNomEntriesVal = expr.eval().asInstanceOf[Int]
        ThetaSketchUtils.checkLgNomLongs(lgNomEntriesVal, prettyName)
        lgNomEntriesVal

      case None => ThetaSketchUtils.DEFAULT_LG_NOM_LONGS
    }
  }

  /**
   * Validates and extracts the summary type parameter value. Ensures the value is a constant
   * string (double, integer, or string). Defaults to "double" if not specified.
   */
  protected lazy val summaryTypeInput: String = {
    summaryType match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw QueryExecutionErrors.tupleSketchParameterMustBeConstant(prettyName, "summaryType")
        }
        val summaryTypeStr = expr
          .eval()
          .asInstanceOf[UTF8String]
          .toString
          .toLowerCase(Locale.ROOT)
          .trim

        summaryTypeStr match {
          case str if ThetaSketchUtils.VALID_SUMMARY_TYPES.contains(str) => str
          case other =>
            throw QueryExecutionErrors.tupleInvalidSummaryType(
              prettyName,
              other,
              ThetaSketchUtils.VALID_SUMMARY_TYPES)
        }

      case None => ThetaSketchUtils.SUMMARY_TYPE_DOUBLE
    }
  }

  /**
   * Validates and extracts the aggregation mode parameter value. Ensures the value is a constant
   * string and one of: sum, min, max, alwaysone. Defaults to "sum" if not specified.
   */
  protected lazy val modeInput: String = {
    mode match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw new QueryExecutionErrors.tupleSketchParameterMustBeConstant(prettyName, "mode")
        }
        val modeStr = expr
          .eval()
          .asInstanceOf[UTF8String]
          .toString
          .toLowerCase(Locale.ROOT)
          .trim

        modeStr match {
          case str if ThetaSketchUtils.VALID_MODES.contains(str) => str
          case other =>
            throw QueryExecutionErrors.tupleInvalidMode(
              prettyName,
              other,
              ThetaSketchUtils.VALID_MODES)
        }

      case None => ThetaSketchUtils.MODE_SUM
    }
  }

  /**
   * Converts the mode string input to DoubleSummary.Mode enum. Used for double summary type
   * operations.
   */
  protected lazy val doubleModeInput: DoubleSummary.Mode = {
    modeInput match {
      case ThetaSketchUtils.MODE_SUM => DoubleSummary.Mode.Sum
      case ThetaSketchUtils.MODE_MIN => DoubleSummary.Mode.Min
      case ThetaSketchUtils.MODE_MAX => DoubleSummary.Mode.Max
      case ThetaSketchUtils.MODE_ALWAYSONE => DoubleSummary.Mode.AlwaysOne
    }
  }

  /**
   * Converts the mode string input to IntegerSummary.Mode enum. Used for integer summary type
   * operations.
   */
  protected lazy val integerModeInput: IntegerSummary.Mode = {
    modeInput match {
      case ThetaSketchUtils.MODE_SUM => IntegerSummary.Mode.Sum
      case ThetaSketchUtils.MODE_MIN => IntegerSummary.Mode.Min
      case ThetaSketchUtils.MODE_MAX => IntegerSummary.Mode.Max
      case ThetaSketchUtils.MODE_ALWAYSONE => IntegerSummary.Mode.AlwaysOne
    }
  }

  /**
   * Creates the appropriate SummaryFactory based on summary type and mode. Used for creating
   * UpdatableSketch instances that need to construct new summary objects. For numeric types
   * (double/integer), the factory is configured with the aggregation mode.
   */
  protected lazy val summaryFactoryInput: SummaryFactory[_] = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        new DoubleSummaryFactory(doubleModeInput)
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        new IntegerSummaryFactory(integerModeInput)
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummaryFactory()
    }
  }

  /**
   * Creates the appropriate SummarySetOperations based on summary type and mode. Used for Union
   * and Intersection operations that need to merge summary values from multiple sketches
   * according to the specified aggregation mode.
   */
  protected lazy val summarySetOperationsInput: SummarySetOperations[_] = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        new DoubleSummarySetOperations(doubleModeInput)
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        new IntegerSummarySetOperations(integerModeInput, integerModeInput)
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummarySetOperations()
    }
  }

  /**
   * Creates the appropriate SummaryDeserializer based on summary type. Used for deserializing
   * binary sketch representations back into CompactSketch objects.
   */
  protected lazy val summaryDeserializer: SummaryDeserializer[_] = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        new DoubleSummaryDeserializer()
      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        new IntegerSummaryDeserializer()
      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        new ArrayOfStringsSummaryDeserializer()
    }
  }

  /**
   * Deserializes a binary sketch representation into a CompactSketch wrapped in
   * FinalizedTupleSketch state. If the buffer is empty, creates a new aggregation buffer. Uses
   * the appropriate deserializer based on the configured summary type.
   *
   * @param buffer
   *   The binary sketch data to deserialize
   * @return
   *   A TupleSketchState containing the deserialized sketch
   */
  protected def deserializeToCompact(buffer: Array[Byte]): TupleSketchState = {
    if (buffer.isEmpty) {
      this.createAggregationBuffer()
    } else {
      val mem = Memory.wrap(buffer)
      try {
        val sketch = Sketches.heapifySketch(mem, summaryDeserializer)
      } catch {
        case e: Exception =>
          throw QueryExecutionErrors.tupleInvalidInputSketchBuffer(prettyName, e.getMessage)
      }
      FinalizedTupleSketch(sketch)
    }
  }
}
