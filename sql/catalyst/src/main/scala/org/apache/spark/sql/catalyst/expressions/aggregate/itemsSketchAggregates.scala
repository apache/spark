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

import org.apache.datasketches.frequencies.ItemsSketch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, ItemsSketchSerDeHelper, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.ItemsSketchUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * The ItemsSketchAgg function builds a DataSketches ItemsSketch from input values and outputs
 * the binary representation. The ItemsSketch is a frequency estimation sketch that tracks
 * the approximate frequency of items in a data stream.
 *
 * The output binary format embeds the data type DDL alongside the sketch bytes so that
 * downstream scalar functions (e.g., items_sketch_get_frequent_items) can deserialize
 * without requiring explicit type information.
 *
 * See [[https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html]]
 * for more information.
 *
 * @param left
 *   child expression whose values will be tracked
 * @param right
 *   the maxMapSize parameter (must be a power of 2) controlling sketch accuracy
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, maxMapSize) - Returns the ItemsSketch binary representation tracking
      approximate frequency of items.
      `maxMapSize` (optional) controls the accuracy of the sketch. Must be a power of 2,
      between 8 and 67108864 (default 4096). Larger values use more memory but provide
      more accurate frequency estimates. """,
  examples = """
    Examples:
      > SELECT items_sketch_get_frequent_items(_FUNC_(col), 'NO_FALSE_POSITIVES') FROM VALUES ('a'), ('a'), ('a'), ('b'), ('c') tab(col);
       [{"item":"a","estimate":3,"lowerBound":3,"upperBound":3},{"item":"c","estimate":1,"lowerBound":1,"upperBound":1},{"item":"b","estimate":1,"lowerBound":1,"upperBound":1}]
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchAgg(
    left: Expression,
    right: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[ItemsSketch[Any]]
    with BinaryLike[Expression]
    with ImplicitCastInputTypes {

  private lazy val itemDataType: DataType = left.dataType

  lazy val maxMapSize: Int = {
    if (!right.foldable) {
      throw QueryExecutionErrors.itemsSketchMaxMapSizeMustBeConstant(prettyName)
    }
    val v = right.eval().asInstanceOf[Int]
    ItemsSketchUtils.checkMaxMapSize(v, prettyName)
    v
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(ItemsSketchUtils.DEFAULT_MAX_MAP_SIZE), 0, 0)
  }

  def this(child: Expression, maxMapSize: Expression) = {
    this(child, maxMapSize, 0, 0)
  }

  def this(child: Expression, maxMapSize: Int) = {
    this(child, Literal(maxMapSize), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ItemsSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ItemsSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ItemsSketchAgg =
    copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "items_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ItemsSketchUtils.isDataTypeSupported(itemDataType)) {
      TypeCheckFailure(s"${itemDataType.typeName} columns are not supported")
    } else if (!right.foldable) {
      TypeCheckFailure("maxMapSize must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): ItemsSketch[Any] = {
    ItemsSketchUtils.createItemsSketch(itemDataType, maxMapSize)
  }

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] = {
    val v = left.eval(input)
    if (v == null) return buffer

    itemDataType match {
      case _: BooleanType =>
        buffer.asInstanceOf[ItemsSketch[Boolean]].update(v.asInstanceOf[Boolean])
      case _: ByteType =>
        buffer.asInstanceOf[ItemsSketch[Number]].update(v.asInstanceOf[Byte])
      case _: ShortType =>
        buffer.asInstanceOf[ItemsSketch[Number]].update(v.asInstanceOf[Short])
      case _: IntegerType =>
        buffer.asInstanceOf[ItemsSketch[Number]].update(v.asInstanceOf[Int])
      case _: LongType =>
        buffer.asInstanceOf[ItemsSketch[Long]].update(v.asInstanceOf[Long])
      case _: FloatType =>
        buffer.asInstanceOf[ItemsSketch[Number]].update(v.asInstanceOf[Float])
      case _: DoubleType =>
        buffer.asInstanceOf[ItemsSketch[Double]].update(v.asInstanceOf[Double])
      case _: DateType =>
        buffer.asInstanceOf[ItemsSketch[Number]].update(v.asInstanceOf[Int])
      case _: TimestampType | _: TimestampNTZType =>
        buffer.asInstanceOf[ItemsSketch[Long]].update(v.asInstanceOf[Long])
      case _: StringType =>
        buffer.asInstanceOf[ItemsSketch[String]].update(v.asInstanceOf[UTF8String].toString)
      case _: DecimalType =>
        buffer.asInstanceOf[ItemsSketch[Decimal]].update(v.asInstanceOf[Decimal])
    }

    buffer
  }

  override def merge(buffer: ItemsSketch[Any], input: ItemsSketch[Any]): ItemsSketch[Any] = {
    buffer.merge(input)
    buffer
  }

  /**
   * Returns the sketch as a binary blob with embedded type information.
   * Format: [ddlLength (4 bytes)][ddlBytes (n bytes)][sketchBytes (remaining)]
   */
  override def eval(buffer: ItemsSketch[Any]): Any = {
    ItemsSketchSerDeHelper.serialize(buffer, itemDataType)
  }

  /**
   * Internal serialization for shuffle/spill (no type prefix needed since
   * deserialization happens within the same aggregate instance which knows the type).
   */
  override def serialize(buffer: ItemsSketch[Any]): Array[Byte] = {
    val serDe = ItemsSketchUtils.genSketchSerDe(itemDataType)
    buffer.toByteArray(serDe)
  }

  override def deserialize(bytes: Array[Byte]): ItemsSketch[Any] = {
    if (bytes.nonEmpty) {
      ItemsSketchUtils.deserializeSketch(bytes, itemDataType, prettyName)
    } else {
      createAggregationBuffer()
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, maxMapSize) - Returns the ItemsSketch binary representation tracking
      approximate frequency of items.
      `expr` is the expression containing the values to track.
      `maxMapSize` (optional) controls the accuracy of the sketch. Must be a power of 2,
      between 8 and 67108864 (default 4096). """,
  examples = """
    Examples:
      > SELECT items_sketch_get_frequent_items(_FUNC_(col), 'NO_FALSE_POSITIVES') FROM VALUES ('a'), ('a'), ('a'), ('b'), ('c') tab(col);
       [{"item":"a","estimate":3,"lowerBound":3,"upperBound":3},{"item":"c","estimate":1,"lowerBound":1,"upperBound":1},{"item":"b","estimate":1,"lowerBound":1,"upperBound":1}]
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object ItemsSketchAggExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("expr"),
    InputParameter("maxMapSize", Some(Literal(ItemsSketchUtils.DEFAULT_MAX_MAP_SIZE)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 2)
    new ItemsSketchAgg(expressions(0), expressions(1))
  }
}

/**
 * The ItemsSketchMergeAgg function ingests and merges ItemsSketch binary representations
 * previously produced by the ItemsSketchAgg function and outputs the merged ItemsSketch.
 *
 * The input binary blobs must contain embedded type information (as produced by
 * [[ItemsSketchSerDeHelper.serialize]]). The data type is extracted from the first
 * non-null input and all subsequent inputs must have the same embedded type.
 *
 * See [[https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html]]
 * for more information.
 *
 * @param left
 *   child expression containing ItemsSketch binary representations
 * @param right
 *   the maxMapSize parameter (must be a power of 2) controlling merged sketch accuracy
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, maxMapSize) - Merges ItemsSketch binary representations produced by
      `items_sketch_agg` into a single ItemsSketch.
      `maxMapSize` (optional) controls the accuracy of the merged sketch. Must be a power of 2,
      between 8 and 67108864 (default 4096). """,
  examples = """
    Examples:
      > SELECT items_sketch_get_frequent_items(_FUNC_(sketch), 'NO_FALSE_POSITIVES') FROM (SELECT items_sketch_agg(col) as sketch FROM VALUES ('a'), ('a'), ('a') tab(col) UNION ALL SELECT items_sketch_agg(col) as sketch FROM VALUES ('a'), ('a') tab(col));
       [{"item":"a","estimate":5,"lowerBound":5,"upperBound":5}]
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchMergeAgg(
    left: Expression,
    right: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[Option[(ItemsSketch[Any], DataType)]]
    with BinaryLike[Expression]
    with ImplicitCastInputTypes {

  lazy val maxMapSize: Int = {
    if (!right.foldable) {
      throw QueryExecutionErrors.itemsSketchMaxMapSizeMustBeConstant(prettyName)
    }
    val v = right.eval().asInstanceOf[Int]
    ItemsSketchUtils.checkMaxMapSize(v, prettyName)
    v
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(ItemsSketchUtils.DEFAULT_MAX_MAP_SIZE), 0, 0)
  }

  def this(child: Expression, maxMapSize: Expression) = {
    this(child, maxMapSize, 0, 0)
  }

  def this(child: Expression, maxMapSize: Int) = {
    this(child, Literal(maxMapSize), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ItemsSketchMergeAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): ItemsSketchMergeAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ItemsSketchMergeAgg =
    copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "items_sketch_merge_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!right.foldable) {
      TypeCheckFailure("maxMapSize must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): Option[(ItemsSketch[Any], DataType)] = None

  override def update(
      buffer: Option[(ItemsSketch[Any], DataType)],
      input: InternalRow): Option[(ItemsSketch[Any], DataType)] = {
    val v = left.eval(input)
    if (v == null) return buffer

    val bytes = v.asInstanceOf[Array[Byte]]
    val (inputSketch, inputDataType) =
      ItemsSketchSerDeHelper.deserialize(bytes, prettyName)

    buffer match {
      case None =>
        // First sketch: create a fresh buffer and merge the input into it
        val fresh = ItemsSketchUtils.createItemsSketch(inputDataType, maxMapSize)
        fresh.merge(inputSketch)
        Some((fresh, inputDataType))
      case Some((existing, dt)) =>
        existing.merge(inputSketch)
        Some((existing, dt))
    }
  }

  override def merge(
      buffer: Option[(ItemsSketch[Any], DataType)],
      input: Option[(ItemsSketch[Any], DataType)]
  ): Option[(ItemsSketch[Any], DataType)] = {
    (buffer, input) match {
      case (None, right) => right
      case (left, None) => left
      case (Some((sketch1, dt)), Some((sketch2, _))) =>
        sketch1.merge(sketch2)
        Some((sketch1, dt))
    }
  }

  override def eval(buffer: Option[(ItemsSketch[Any], DataType)]): Any = {
    buffer match {
      case Some((sketch, dt)) =>
        ItemsSketchSerDeHelper.serialize(sketch, dt)
      case None =>
        // No input rows: return an empty sketch for StringType as a sensible default
        val emptySketch = ItemsSketchUtils.createItemsSketch(StringType, maxMapSize)
        ItemsSketchSerDeHelper.serialize(
          emptySketch.asInstanceOf[ItemsSketch[Any]], StringType)
    }
  }

  override def serialize(buffer: Option[(ItemsSketch[Any], DataType)]): Array[Byte] = {
    buffer match {
      case Some((sketch, dt)) =>
        ItemsSketchSerDeHelper.serialize(sketch, dt)
      case None =>
        Array.emptyByteArray
    }
  }

  override def deserialize(bytes: Array[Byte]): Option[(ItemsSketch[Any], DataType)] = {
    if (bytes.nonEmpty) {
      val (sketch, dt) = ItemsSketchSerDeHelper.deserialize(bytes, prettyName)
      Some((sketch, dt))
    } else {
      None
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, maxMapSize) - Merges ItemsSketch binary representations produced by
      `items_sketch_agg` into a single ItemsSketch.
      `expr` is a column of binary ItemsSketch representations.
      `maxMapSize` (optional) controls the accuracy of the merged sketch. Must be a power of 2,
      between 8 and 67108864 (default 4096). """,
  examples = """
    Examples:
      > SELECT items_sketch_get_frequent_items(_FUNC_(sketch), 'NO_FALSE_POSITIVES') FROM (SELECT items_sketch_agg(col) as sketch FROM VALUES ('a'), ('a'), ('a') tab(col) UNION ALL SELECT items_sketch_agg(col) as sketch FROM VALUES ('a'), ('a') tab(col));
       [{"item":"a","estimate":5,"lowerBound":5,"upperBound":5}]
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object ItemsSketchMergeAggExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("expr"),
    InputParameter("maxMapSize", Some(Literal(ItemsSketchUtils.DEFAULT_MAX_MAP_SIZE)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 2)
    new ItemsSketchMergeAgg(expressions(0), expressions(1))
  }
}
