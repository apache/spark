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
import org.apache.datasketches.tuple.{Sketch, Sketches, Summary, SummaryFactory, SummarySetOperations, Union, UpdatableSketch, UpdatableSketchBuilder, UpdatableSummary}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryDeserializer, DoubleSummaryFactory}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryDeserializer, IntegerSummaryFactory}
import org.apache.datasketches.tuple.strings.{ArrayOfStringsSummary, ArrayOfStringsSummaryDeserializer, ArrayOfStringsSummaryFactory}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, ThetaSketchUtils}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

sealed trait TupleSketchState {
  def serialize(): Array[Byte]
  def eval(): Array[Byte]
}
case class UpdatableTupleSketchBuffer[U, S <: UpdatableSummary[U]](
    sketch: UpdatableSketch[U, S]) extends TupleSketchState {
  override def serialize(): Array[Byte] = sketch.compact.toByteArray
  override def eval(): Array[Byte] = sketch.compact.toByteArray
}
case class UnionTupleAggregationBuffer[S <: Summary](union: Union[S]) extends TupleSketchState {
  override def serialize(): Array[Byte] = union.getResult.toByteArray
  override def eval(): Array[Byte] = union.getResult.toByteArray
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
  override def second: Expression =
    lgNomEntriesExpr.getOrElse(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))
  override def third: Expression =
    summaryType.getOrElse(Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE))
  override def fourth: Expression = mode.getOrElse(Literal(ThetaSketchUtils.MODE_SUM))

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
   * Evaluate the input row and update the UpdatableSketch instance with the row's key and
   * summary value. The update function only supports a subset of Spark SQL types, and an
   * exception will be thrown for unsupported types.
   * Notes:
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
      case _ => throw new IllegalArgumentException("Invalid bytes")
    }

    // Convert summary value based on summaryTypeInput
    val summary = (summaryTypeInput, summaryValue) match {
      case (ThetaSketchUtils.SUMMARY_TYPE_DOUBLE, d: Double) => d
      case (ThetaSketchUtils.SUMMARY_TYPE_DOUBLE, f: Float) => f.toDouble
      case (ThetaSketchUtils.SUMMARY_TYPE_INTEGER, i: Int) => i
      case (ThetaSketchUtils.SUMMARY_TYPE_STRING, s: UTF8String) => s
      case _ => throw new IllegalArgumentException(s"Invalid summary type and value combination")
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

    def createUnionWith(sketch1: Sketch[S], sketch2: Sketch[S]): UnionTupleAggregationBuffer[S] = {
      val factory = summaryFactoryInput.asInstanceOf[SummaryFactory[S]]
      val summarySetOps = factory.newSummary().asInstanceOf[SummarySetOperations[S]]
      val union = new Union[S](1 << lgNomEntriesInput, summarySetOps)
      union.union(sketch1)
      union.union(sketch2)
      UnionTupleAggregationBuffer(union)
    }

    (updateBuffer, input) match {
      case (buf: UnionTupleAggregationBuffer[S] @unchecked,
            sketch: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        buf.union.union(sketch.sketch.compact())
        buf

      case (buf: UnionTupleAggregationBuffer[S] @unchecked,
            sketch: FinalizedTupleSketch[S] @unchecked) =>
        buf.union.union(sketch.sketch)
        buf

      case (union1: UnionTupleAggregationBuffer[S] @unchecked,
            union2: UnionTupleAggregationBuffer[S] @unchecked) =>
        union1.union.union(union2.union.getResult)
        union1

      case (sketch1: UpdatableTupleSketchBuffer[_, S] @unchecked,
            sketch2: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        createUnionWith(sketch1.sketch.compact(), sketch2.sketch.compact())

      case (sketch1: UpdatableTupleSketchBuffer[_, S] @unchecked,
            sketch2: FinalizedTupleSketch[S] @unchecked) =>
        createUnionWith(sketch1.sketch.compact(), sketch2.sketch)

      case (sketch1: FinalizedTupleSketch[S] @unchecked,
            sketch2: UpdatableTupleSketchBuffer[_, S] @unchecked) =>
        createUnionWith(sketch1.sketch, sketch2.sketch.compact())

      case (sketch1: FinalizedTupleSketch[S] @unchecked,
            sketch2: FinalizedTupleSketch[S] @unchecked) =>
        createUnionWith(sketch1.sketch, sketch2.sketch)

      case _ => throw new IllegalArgumentException("Invalid sketch state combination")
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

trait BaseTupleSketchAgg {
  val child: Expression
  val lgNomEntriesExpr: Option[Expression]
  val summaryType: Option[Expression]
  val mode: Option[Expression]

  protected def prettyName: String

  protected def createAggregationBuffer(): TupleSketchState

  protected lazy val lgNomEntriesInput: Int = {
    lgNomEntriesExpr match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw new IllegalArgumentException("must be const")
        }
        val lgNomEntriesVal = expr.eval().asInstanceOf[Int]
        ThetaSketchUtils.checkLgNomLongs(lgNomEntriesVal, prettyName)
        lgNomEntriesVal

      case None => ThetaSketchUtils.DEFAULT_LG_NOM_LONGS
    }
  }

  protected lazy val summaryTypeInput: String = {
    summaryType match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw new IllegalArgumentException("must be const")
        }
        expr.eval().asInstanceOf[UTF8String].toString.
          toLowerCase(Locale.ROOT).trim

      case None => ThetaSketchUtils.SUMMARY_TYPE_DOUBLE
    }
  }

  protected lazy val modeInput: String = {
    mode match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw new IllegalArgumentException("must be const")
        }
        val modeStr = expr.eval().asInstanceOf[UTF8String].toString
          .toLowerCase(Locale.ROOT).trim
        modeStr match {
          case ThetaSketchUtils.MODE_SUM | ThetaSketchUtils.MODE_MIN |
               ThetaSketchUtils.MODE_MAX | ThetaSketchUtils.MODE_ALWAYSONE => modeStr
          case other => throw new IllegalArgumentException(s"Unknown mode: $other")
        }

      case None => ThetaSketchUtils.MODE_SUM
    }
  }

  protected lazy val summaryFactoryInput: SummaryFactory[_] = {
    summaryTypeInput match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        val numericMode = modeInput match {
          case ThetaSketchUtils.MODE_SUM => DoubleSummary.Mode.Sum
          case ThetaSketchUtils.MODE_MIN => DoubleSummary.Mode.Min
          case ThetaSketchUtils.MODE_MAX => DoubleSummary.Mode.Max
          case ThetaSketchUtils.MODE_ALWAYSONE => DoubleSummary.Mode.AlwaysOne
        }
        new DoubleSummaryFactory(numericMode)

      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        val numericMode = modeInput match {
          case ThetaSketchUtils.MODE_SUM => IntegerSummary.Mode.Sum
          case ThetaSketchUtils.MODE_MIN => IntegerSummary.Mode.Min
          case ThetaSketchUtils.MODE_MAX => IntegerSummary.Mode.Max
          case ThetaSketchUtils.MODE_ALWAYSONE => IntegerSummary.Mode.AlwaysOne
        }
        new IntegerSummaryFactory(numericMode)

      case ThetaSketchUtils.SUMMARY_TYPE_STRING => new ArrayOfStringsSummaryFactory()
      case other => throw new IllegalArgumentException(s"Unknown summary type: $other")
    }
  }

  protected def deserializeToCompact(buffer: Array[Byte]): TupleSketchState = {
    if (buffer.isEmpty) {
      this.createAggregationBuffer()
    } else {
      val mem = Memory.wrap(buffer)

      val sketch = summaryTypeInput match {
        case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
          Sketches.heapifySketch(mem, new DoubleSummaryDeserializer())

        case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
          Sketches.heapifySketch(mem, new IntegerSummaryDeserializer())

        case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
          Sketches.heapifySketch(mem, new ArrayOfStringsSummaryDeserializer())
      }

      FinalizedTupleSketch(sketch)
    }
  }
}
