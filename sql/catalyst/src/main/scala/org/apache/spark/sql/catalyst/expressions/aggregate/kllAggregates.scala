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

import org.apache.datasketches.kll.{KllDoublesSketch, KllFloatsSketch, KllLongsSketch, KllSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, TypeCollection}

/**
 * The KllSketchAggBigint function utilizes an Apache DataSketches KllLongsSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllLongsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   Default is 200 (normalized rank error ~1.65%). Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Returns the KllLongsSketch compact binary representation.
      The optional k parameter controls the size and accuracy of the sketch (default 200, range 8-65535).
      Larger k values provide more accurate quantile estimates but result in larger, slower sketches.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_bigint(_FUNC_(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
      > SELECT LENGTH(kll_sketch_to_string_bigint(_FUNC_(col, 400))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggBigint(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllLongsSketch]
    with KllSketchAggBase
    with ExpectsInputTypes {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def children: Seq[Expression] = child +: kExpr.toSeq

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllSketchAggBigint =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllSketchAggBigint =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllSketchAggBigint = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = {
    val baseTypes = Seq(
      TypeCollection(
        ByteType,
        IntegerType,
        LongType,
        ShortType))
    if (kExpr.isDefined) baseTypes :+ IntegerType else baseTypes
  }
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_bigint"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkKInputDataTypes()
    }
  }

  override def createAggregationBuffer(): KllLongsSketch =
    KllLongsSketch.newHeapInstance(kValue)

  /**
   * Evaluate the input row and update the KllLongsSketch instance with the row's value. The update
   * function only supports a subset of Spark SQL types, and an exception will be thrown for
   * unsupported types.
   * Note, null values are ignored.
   */
  override def update(sketch: KllLongsSketch, input: InternalRow): KllLongsSketch = {
    val v = child.eval(input)
    if (v == null) {
      sketch
    } else {
      // Handle the different data types for sketch updates.
      child.dataType match {
        case ByteType =>
          sketch.update(v.asInstanceOf[Byte].toLong)
        case IntegerType =>
          sketch.update(v.asInstanceOf[Int].toLong)
        case LongType =>
          sketch.update(v.asInstanceOf[Long])
        case ShortType =>
          sketch.update(v.asInstanceOf[Short].toLong)
        case _ =>
          throw unexpectedInputDataTypeError(child)
      }
      sketch
    }
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllLongsSketch, input: KllLongsSketch): KllLongsSketch = {
    try {
      updateBuffer.merge(input)
      updateBuffer
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketch: KllLongsSketch): Any = sketch.toByteArray

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketch: KllLongsSketch): Array[Byte] = sketch.toByteArray

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllLongsSketch = if (buffer.nonEmpty) {
    try {
      KllLongsSketch.heapify(Memory.wrap(buffer))
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  } else {
    this.createAggregationBuffer()
  }
}

/**
 * The KllSketchAggFloat function utilizes an Apache DataSketches KllFloatsSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllFloatsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   Default is 200 (normalized rank error ~1.65%). Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Returns the KllFloatsSketch compact binary representation.
      The optional k parameter controls the size and accuracy of the sketch (default 200, range 8-65535).
      Larger k values provide more accurate quantile estimates but result in larger, slower sketches.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_float(_FUNC_(col))) > 0 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
      > SELECT LENGTH(kll_sketch_to_string_float(_FUNC_(col, 400))) > 0 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggFloat(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllFloatsSketch]
        with KllSketchAggBase
        with ExpectsInputTypes {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def children: Seq[Expression] = child +: kExpr.toSeq

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllSketchAggFloat =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllSketchAggFloat =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllSketchAggFloat = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = {
    val baseTypes = Seq(FloatType)
    if (kExpr.isDefined) baseTypes :+ IntegerType else baseTypes
  }
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_float"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkKInputDataTypes()
    }
  }

  override def createAggregationBuffer(): KllFloatsSketch =
    KllFloatsSketch.newHeapInstance(kValue)

  /**
   * Evaluate the input row and update the KllFloatsSketch instance with the row's value. The update
   * function only supports FloatType to avoid precision loss from integer-to-float conversion.
   * Users should use kll_sketch_agg_bigint for integer types.
   * Note, Null values are ignored.
   */
  override def update(sketch: KllFloatsSketch, input: InternalRow): KllFloatsSketch = {
    val v = child.eval(input)
    if (v == null) {
      sketch
    } else {
      // Handle the different data types for sketch updates.
      child.dataType match {
        case FloatType =>
          sketch.update(v.asInstanceOf[Float])
        case _ =>
          throw unexpectedInputDataTypeError(child)
      }
      sketch
    }
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllFloatsSketch, input: KllFloatsSketch): KllFloatsSketch = {
    try {
      updateBuffer.merge(input)
      updateBuffer
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketch: KllFloatsSketch): Any = sketch.toByteArray

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketch: KllFloatsSketch): Array[Byte] = sketch.toByteArray

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllFloatsSketch = if (buffer.nonEmpty) {
    try {
      KllFloatsSketch.heapify(Memory.wrap(buffer))
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  } else {
    this.createAggregationBuffer()
  }
}

/**
 * The KllSketchAggDouble function utilizes an Apache DataSketches KllDoublesSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllDoublesSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   Default is 200 (normalized rank error ~1.65%). Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Returns the KllDoublesSketch compact binary representation.
      The optional k parameter controls the size and accuracy of the sketch (default 200, range 8-65535).
      Larger k values provide more accurate quantile estimates but result in larger, slower sketches.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_double(_FUNC_(col))) > 0 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
      > SELECT LENGTH(kll_sketch_to_string_double(_FUNC_(col, 400))) > 0 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggDouble(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllDoublesSketch]
        with KllSketchAggBase
        with ExpectsInputTypes {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def children: Seq[Expression] = child +: kExpr.toSeq

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllSketchAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllSketchAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllSketchAggDouble = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = {
    val baseTypes = Seq(TypeCollection(FloatType, DoubleType))
    if (kExpr.isDefined) baseTypes :+ IntegerType else baseTypes
  }
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_double"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkKInputDataTypes()
    }
  }

  override def createAggregationBuffer(): KllDoublesSketch =
    KllDoublesSketch.newHeapInstance(kValue)

  /**
   * Evaluate the input row and update the KllDoublesSketch instance with the row's value.
   * The update function only supports FloatType and DoubleType to avoid precision loss from
   * integer-to-double conversion. Users should use kll_sketch_agg_bigint for integer types.
   * Note, Null values are ignored.
   */
  override def update(sketch: KllDoublesSketch, input: InternalRow): KllDoublesSketch = {
    val v = child.eval(input)
    if (v == null) {
      sketch
    } else {
      // Handle the different data types for sketch updates.
      child.dataType match {
        case DoubleType =>
          sketch.update(v.asInstanceOf[Double])
        case FloatType =>
          sketch.update(v.asInstanceOf[Float].toDouble)
        case _ =>
          throw unexpectedInputDataTypeError(child)
      }
      sketch
    }
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllDoublesSketch, input: KllDoublesSketch): KllDoublesSketch = {
    try {
      updateBuffer.merge(input)
      updateBuffer
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketch: KllDoublesSketch): Any = sketch.toByteArray

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketch: KllDoublesSketch): Array[Byte] = sketch.toByteArray

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllDoublesSketch = if (buffer.nonEmpty) {
    try {
      KllDoublesSketch.heapify(Memory.wrap(buffer))
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
    }
  } else {
    this.createAggregationBuffer()
  }
}

/**
 * The KllMergeAggBigint function merges multiple Apache DataSketches KllLongsSketch instances
 * that have been serialized to binary format. This is useful for combining sketches created
 * in separate aggregations (e.g., from different partitions or time windows).
 * It outputs the merged binary representation of the KllLongsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression containing binary KllLongsSketch representations to merge
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   If not specified, the merged sketch adopts the k value from the first input sketch.
 *   If specified, the value is used to initialize the aggregation buffer. The merge operation
 *   can handle input sketches with different k values. Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Merges binary KllLongsSketch representations and returns the merged sketch.
      The input expression should contain binary sketch representations (e.g., from kll_sketch_agg_bigint).
      The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
      If k is not specified, the merged sketch adopts the k value from the first input sketch.
  """,
  examples = """
    Examples:
      > SELECT kll_sketch_get_n_bigint(_FUNC_(sketch)) FROM (SELECT kll_sketch_agg_bigint(col) as sketch FROM VALUES (1), (2), (3) tab(col) UNION ALL SELECT kll_sketch_agg_bigint(col) as sketch FROM VALUES (4), (5), (6) tab(col)) t;
       6
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllMergeAggBigint(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends KllMergeAggBase[KllLongsSketch] {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllMergeAggBigint =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllMergeAggBigint =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllMergeAggBigint = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def prettyName: String = "kll_merge_agg_bigint"

  // Factory method implementations
  protected def newHeapInstance(k: Int): KllLongsSketch = KllLongsSketch.newHeapInstance(k)
  protected def wrapSketch(bytes: Array[Byte]): KllLongsSketch =
    KllLongsSketch.wrap(Memory.wrap(bytes))
  protected def heapifySketch(bytes: Array[Byte]): KllLongsSketch =
    KllLongsSketch.heapify(Memory.wrap(bytes))
  protected def toByteArray(sketch: KllLongsSketch): Array[Byte] = sketch.toByteArray
}

/**
 * The KllMergeAggFloat function merges multiple Apache DataSketches KllFloatsSketch instances
 * that have been serialized to binary format. This is useful for combining sketches created
 * in separate aggregations (e.g., from different partitions or time windows).
 * It outputs the merged binary representation of the KllFloatsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression containing binary KllFloatsSketch representations to merge
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   If not specified, the merged sketch adopts the k value from the first input sketch.
 *   If specified, the value is used to initialize the aggregation buffer. The merge operation
 *   can handle input sketches with different k values. Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Merges binary KllFloatsSketch representations and returns the merged sketch.
      The input expression should contain binary sketch representations (e.g., from kll_sketch_agg_float).
      The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
      If k is not specified, the merged sketch adopts the k value from the first input sketch.
  """,
  examples = """
    Examples:
      > SELECT kll_sketch_get_n_float(_FUNC_(sketch)) FROM (SELECT kll_sketch_agg_float(col) as sketch FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)) tab(col) UNION ALL SELECT kll_sketch_agg_float(col) as sketch FROM VALUES (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)), (CAST(6.0 AS FLOAT)) tab(col)) t;
       6
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllMergeAggFloat(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends KllMergeAggBase[KllFloatsSketch] {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllMergeAggFloat =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllMergeAggFloat =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllMergeAggFloat = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def prettyName: String = "kll_merge_agg_float"

  // Factory method implementations
  protected def newHeapInstance(k: Int): KllFloatsSketch = KllFloatsSketch.newHeapInstance(k)
  protected def wrapSketch(bytes: Array[Byte]): KllFloatsSketch =
    KllFloatsSketch.wrap(Memory.wrap(bytes))
  protected def heapifySketch(bytes: Array[Byte]): KllFloatsSketch =
    KllFloatsSketch.heapify(Memory.wrap(bytes))
  protected def toByteArray(sketch: KllFloatsSketch): Array[Byte] = sketch.toByteArray
}

/**
 * The KllMergeAggDouble function merges multiple Apache DataSketches KllDoublesSketch instances
 * that have been serialized to binary format. This is useful for combining sketches created
 * in separate aggregations (e.g., from different partitions or time windows).
 * It outputs the merged binary representation of the KllDoublesSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression containing binary KllDoublesSketch representations to merge
 * @param kExpr
 *   optional expression for the k parameter from the Apache DataSketches library that controls
 *   the size and accuracy of the sketch. Must be a constant integer between 8 and 65535.
 *   If not specified, the merged sketch adopts the k value from the first input sketch.
 *   If specified, the value is used to initialize the aggregation buffer. The merge operation
 *   can handle input sketches with different k values. Larger k values provide more accurate
 *   estimates but result in larger, slower sketches.
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Merges binary KllDoublesSketch representations and returns the merged sketch.
      The input expression should contain binary sketch representations (e.g., from kll_sketch_agg_double).
      The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
      If k is not specified, the merged sketch adopts the k value from the first input sketch.
  """,
  examples = """
    Examples:
      > SELECT kll_sketch_get_n_double(_FUNC_(sketch)) FROM (SELECT kll_sketch_agg_double(col) as sketch FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) tab(col) UNION ALL SELECT kll_sketch_agg_double(col) as sketch FROM VALUES (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)), (CAST(6.0 AS DOUBLE)) tab(col)) t;
       6
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllMergeAggDouble(
    child: Expression,
    kExpr: Option[Expression] = None,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends KllMergeAggBase[KllDoublesSketch] {
  def this(child: Expression) = this(child, None, 0, 0)
  def this(child: Expression, kExpr: Expression) = this(child, Some(kExpr), 0, 0)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): KllMergeAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): KllMergeAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KllMergeAggDouble = {
    if (newChildren.length == 1) {
      copy(child = newChildren(0), kExpr = None)
    } else {
      copy(child = newChildren(0), kExpr = Some(newChildren(1)))
    }
  }

  override def prettyName: String = "kll_merge_agg_double"

  // Factory method implementations
  protected def newHeapInstance(k: Int): KllDoublesSketch = KllDoublesSketch.newHeapInstance(k)
  protected def wrapSketch(bytes: Array[Byte]): KllDoublesSketch =
    KllDoublesSketch.wrap(Memory.wrap(bytes))
  protected def heapifySketch(bytes: Array[Byte]): KllDoublesSketch =
    KllDoublesSketch.heapify(Memory.wrap(bytes))
  protected def toByteArray(sketch: KllDoublesSketch): Array[Byte] = sketch.toByteArray
}

/**
 * Base abstract class for KLL merge aggregate functions that provides common implementation
 * for merging serialized KLL sketches with optional k parameter.
 *
 * @tparam T The KLL sketch type (KllLongsSketch, KllFloatsSketch, or KllDoublesSketch)
 */
abstract class KllMergeAggBase[T <: KllSketch]
    extends TypedImperativeAggregate[Option[T]]
    with KllSketchAggBase
    with ExpectsInputTypes {

  def child: Expression

  // Abstract factory methods for sketch-specific instantiation
  protected def newHeapInstance(k: Int): T
  protected def wrapSketch(bytes: Array[Byte]): T
  protected def heapifySketch(bytes: Array[Byte]): T
  protected def toByteArray(sketch: T): Array[Byte]

  // Common implementations for all merge aggregates
  override def children: Seq[Expression] = child +: kExpr.toSeq

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[AbstractDataType] = {
    val baseTypes = Seq(BinaryType)
    if (kExpr.isDefined) baseTypes :+ IntegerType else baseTypes
  }

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkKInputDataTypes()
    }
  }

  /**
   * Defer instantiation of the sketch instance until we've deserialized
   * our first sketch (if kExpr was not provided), and use that sketch's k value.
   *
   * @return None if kExpr was not provided, otherwise Some(sketch with specified k)
   */
  override def createAggregationBuffer(): Option[T] = {
    if (kExpr.isDefined) {
      Some(newHeapInstance(kValue))
    } else {
      None
    }
  }

  /**
   * Evaluate the input row and wrap the binary sketch, then merge it into
   * the current aggregation buffer.
   * Note, null values are ignored.
   */
  override def update(sketchOption: Option[T], input: InternalRow): Option[T] = {
    val v = child.eval(input)
    if (v == null) {
      sketchOption
    } else {
      try {
        val sketchBytes = v.asInstanceOf[Array[Byte]]
        val inputSketch = wrapSketch(sketchBytes)
        val sketch = sketchOption.getOrElse(newHeapInstance(inputSketch.getK()))
        sketch.merge(inputSketch)
        Some(sketch)
      } catch {
        case _: Exception =>
          throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
      }
    }
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBufferOption: Option[T], inputOption: Option[T]): Option[T] = {
    (updateBufferOption, inputOption) match {
      case (Some(updateBuffer), Some(input)) =>
        try {
          updateBuffer.merge(input)
          Some(updateBuffer)
        } catch {
          case _: Exception =>
            throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
        }
      case (Some(_), None) => updateBufferOption
      case (None, Some(_)) => inputOption
      case (None, None) => None
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketchOption: Option[T]): Any = {
    sketchOption match {
      case Some(sketch) => toByteArray(sketch)
      case None => toByteArray(newHeapInstance(kValue))
    }
  }

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketchOption: Option[T]): Array[Byte] = {
    sketchOption match {
      case Some(sketch) => toByteArray(sketch)
      case None => toByteArray(newHeapInstance(kValue))
    }
  }

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): Option[T] = {
    if (buffer.nonEmpty) {
      try {
        Some(heapifySketch(buffer))
      } catch {
        case _: Exception =>
          throw QueryExecutionErrors.kllInvalidInputSketchBuffer(prettyName)
      }
    } else {
      createAggregationBuffer()
    }
  }
}

/**
 * Common trait for KLL sketch aggregate functions that support an optional k parameter.
 */
trait KllSketchAggBase {
  def kExpr: Option[Expression]
  def prettyName: String

  // Constants from the Apache DataSketches library.
  private val MIN_K = 8
  private val MAX_K = 65535
  private val DEFAULT_K = 200

  // Validate and extract k value
  protected lazy val kValue: Int = {
    kExpr match {
      case Some(expr) =>
        if (!expr.foldable) {
          throw QueryExecutionErrors.kllSketchKMustBeConstantError(prettyName)
        }
        val k = expr.eval().asInstanceOf[Int]
        if (k < MIN_K || k > MAX_K) {
          throw QueryExecutionErrors.kllSketchKOutOfRangeError(prettyName, k)
        }
        k
      case None => DEFAULT_K
    }
  }

  protected def checkKInputDataTypes(): TypeCheckResult = {
    kExpr match {
      case Some(expr) =>
        if (!expr.foldable) {
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> "k",
              "inputType" -> "int",
              "inputExpr" -> expr.sql))
        } else if (expr.eval() == null) {
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_NULL",
            messageParameters = Map("exprName" -> "k"))
        } else {
          // Trigger validation
          try {
            kValue
            TypeCheckResult.TypeCheckSuccess
          } catch {
            case e: Exception => TypeCheckResult.TypeCheckFailure(e.getMessage)
          }
        }
      case None => TypeCheckResult.TypeCheckSuccess
    }
  }

  protected def unexpectedInputDataTypeError(
      child: Expression): SparkUnsupportedOperationException =
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_3121",
      messageParameters = Map("dataType" -> child.dataType.toString))
}
