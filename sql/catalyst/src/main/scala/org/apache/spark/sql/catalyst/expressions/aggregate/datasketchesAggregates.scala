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

import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.apache.datasketches.memory.WritableMemory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This datasketchesAggregates file is intended to encapsulate all of the
 * aggregate functions that utilize Datasketches sketch objects as intermediate
 * aggregation buffers.
 *
 * The HllSketchAggregate sealed trait is meant to be extended by the aggregate
 * functions which utilize instances of HllSketch to count uniques.
 */
sealed trait HllSketchAggregate
  extends TypedImperativeAggregate[HllSketch] with UnaryLike[Expression] {

  // These are used as params when instantiating the HllSketch object
  // and are set by the case classes' args that extend this trait

  def lgConfigK: Int
  def tgtHllType: String

  // From here on, these are the shared default implementations for TypedImperativeAggregate

  /** Aggregate functions which utilize HllSketch instances should never return null */
  override def nullable: Boolean = false

  /**
   * Instantiate an HllSketch instance using the lgConfigK and tgtHllType params.
   *
   * @return an HllSketch instance
   */
  override def createAggregationBuffer(): HllSketch = {
    new HllSketch(lgConfigK, TgtHllType.valueOf(tgtHllType.toUpperCase(Locale.ROOT)))
  }

  /**
   * Evaluate the input row and update the HllSketch instance with the row's value.
   * The update function only supports a subset of Spark SQL types, and an
   * UnsupportedOperationException will be thrown for unsupported types.
   *
   * @param sketch The HllSketch instance.
   * @param input  an input row
   */
  override def update(sketch: HllSketch, input: InternalRow): HllSketch = {
    val v = child.eval(input)
    if (v != null) {
      child.dataType match {
        // Update implemented for a subset of types supported by HllSketch
        // Spark SQL doesn't have equivalent types for ByteBuffer or char[] so leave those out
        // Leaving out support for Array types, as unique counting these aren't a common use case
        // Leaving out support for floating point types (IE DoubleType) due to imprecision
        // TODO: implement support for decimal/datetime/interval types
        case IntegerType => sketch.update(v.asInstanceOf[Int])
        case LongType => sketch.update(v.asInstanceOf[Long])
        case StringType => sketch.update(v.asInstanceOf[UTF8String].toString)
        case dataType => throw new UnsupportedOperationException(
          s"A HllSketch instance cannot be updates with a Spark ${dataType.toString} type")
      }
    }
    sketch
  }

  /**
   * Merges an input HllSketch into the sketch which is acting as the aggregation buffer.
   *
   * @param sketch the HllSketch instance used to store the aggregation result.
   * @param input an input HllSketch instance
   */
  override def merge(sketch: HllSketch, input: HllSketch): HllSketch = {
    // minor optimization: HllSketches configured as HLL_8 can be converted directly to Unions
    if (sketch.getTgtHllType == TgtHllType.HLL_8) {
      val union = Union.writableWrap(WritableMemory.writableWrap(sketch.toUpdatableByteArray))
      union.update(input)
      union.getResult
    } else {
      val union = new Union(sketch.getLgConfigK)
      union.update(sketch)
      union.update(input)
      union.getResult(sketch.getTgtHllType)
    }
  }

  /** Convert the underlying HllSketch into an updateable byte array  */
  override def serialize(sketch: HllSketch): Array[Byte] = {
    sketch.toCompactByteArray
  }

  /** De-serializes the updateable byte array into a HllSketch instance */
  override def deserialize(buffer: Array[Byte]): HllSketch = {
    HllSketch.heapify(buffer)
  }
}

/**
 * The HllSketchEstimate function utilizes a Datasketches HllSketch instance to
 * probabilistically count the number of unique values in a given column.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information
 *
 * @param child child expression against which unique counting will occur
 * @param lgConfigK the log-base-2 of K, where K is the number of buckets or slots for the sketch
 * @param tgtHllType the target type of the HllSketch to be used (HLL_4, HLL_6, HLL_8)
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, lgConfigK, tgtHllType]) - Returns the estimated number of unique values.
      `lgConfigK` the log-base-2 of K, where K is the number of buckets or slots for the HllSketch.
      `tgtHllType` the target type of the HllSketch to be used (HLL_4, HLL_6, HLL_8). """,
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
case class HllSketchEstimate(
    child: Expression,
    lgConfigK: Int = HllSketch.DEFAULT_LG_K,
    tgtHllType: String = HllSketch.DEFAULT_HLL_TYPE.toString,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HllSketchAggregate {

  // This constructor seems to be only necessary for the ExpressionSchemaSuite to pass

  def this(child: Expression) = {
    this(child, HllSketch.DEFAULT_LG_K, HllSketch.DEFAULT_HLL_TYPE.toString, 0, 0)
  }

  // These copy constructors are repeated in every case class

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HllSketchEstimate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketchEstimate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HllSketchEstimate =
    copy(child = newChild)

  // Implementations specific to HllSketchEstimate

  override def prettyName: String = "hllsketch_estimate"

  override def dataType: DataType = LongType

  /**
   * Returns the estimated number of unique values
   *
   * @param sketch HllSketch instance used as an aggregation buffer
   * @return A long value representing the number of uniques
   */
  override def eval(sketch: HllSketch): Any = {
    sketch.getEstimate.toLong
  }
}

/**
 * The HllSketchBinary function utilizes a Datasketches HllSketch instance to
 * probabilistically count the number of unique values in a given column, and
 * outputs the compact binary representation of the HllSketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information
 *
 * @param child child expression against which unique counting will occur
 * @param lgConfigK the log-base-2 of K, where K is the number of buckets or slots for the sketch
 * @param tgtHllType the target type of the HllSketch to be used (HLL_4, HLL_6, HLL_8)
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, lgConfigK, tgtHllType]) - Returns the HllSketch's compact binary representation.
      `lgConfigK` the log-base-2 of K, where K is the number of buckets or slots for the HllSketch.
      `tgtHllType` the target type of the HllSketch to be used (HLL_4, HLL_6, HLL_8). """,
  examples = """
    Examples:
      > SELECT hllsketch_binary_estimate(_FUNC_(col1))
      FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
case class HllSketchBinary(
    child: Expression,
    lgConfigK: Int = HllSketch.DEFAULT_LG_K,
    tgtHllType: String = HllSketch.DEFAULT_HLL_TYPE.toString,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HllSketchAggregate {

  // This constructor seems to be only necessary for the ExpressionSchemaSuite to pass

  def this(child: Expression) = {
    this(child, HllSketch.DEFAULT_LG_K, HllSketch.DEFAULT_HLL_TYPE.toString, 0, 0)
  }

  // These copy constructors are repeated in every case class

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HllSketchBinary =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketchBinary =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HllSketchBinary =
    copy(child = newChild)

  // Implementations specific to HllSketchBinary

  override def prettyName: String = "hllsketch_binary"

  override def dataType: DataType = BinaryType

  /**
   * Returns the compact byte array associated with the HllSketch
   *
   * @param sketch HllSketch instance used as an aggregation buffer
   * @return A binary value which can be evaluated or merged
   */
  override def eval(sketch: HllSketch): Any = {
    sketch.toCompactByteArray
  }
}

/**
 * The HllSketchUnionEstimate function ingests and merges Datasketches HllSketch
 * instances previously produced by the HllSketchBinary function, and
 * outputs the estimated unique count from the merged HllSketches.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information
 *
 * @param child child expression against which unique counting will occur
 * @param lgMaxK The largest maximum size for lgConfigK for the union operation.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, lgMaxK]) - Returns the estimated number of unique values.
      `lgMaxK` The largest maximum size for lgConfigK for the union operation.""",
  examples = """
    Examples:
      > SELECT _FUNC_(hllsketch_binary(col1))
      FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
case class HllSketchUnionEstimate(
    child: Expression,
    lgMaxK: Int = HllSketch.DEFAULT_LG_K,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  // Unfortunately, we can't extend HllSketchAggregate as there's no quick
  // way to convert between HllSketch and Union instances (even though Union
  // is just a wrapper around HllSketch).
  extends TypedImperativeAggregate[Union] with UnaryLike[Expression] {

  // This constructor seems to be only necessary for the ExpressionSchemaSuite to pass

  def this(child: Expression) = {
    this(child, HllSketch.DEFAULT_LG_K, 0, 0)
  }

  // These copy constructors are repeated in every case class

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int):
  HllSketchUnionEstimate = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketchUnionEstimate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HllSketchUnionEstimate =
    copy(child = newChild)

  // Implementations specific to HllSketchUnionEstimate

  override def prettyName: String = "hllsketch_union_estimate"

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  /**
   * Instantiate an Union instance using the lgMaxK param.
   *
   * @return an Union instance
   */
  override def createAggregationBuffer(): Union = {
    new Union(lgMaxK)
  }

  /**
   * Update the Union instance with the HllSketch byte array obtained from the row.
   *
   * @param union The Union instance.
   * @param input an input row
   */
  override def update(union: Union, input: InternalRow): Union = {
    val v = child.eval(input)
    if (v != null) {
      child.dataType match {
        case BinaryType =>
          union.update(HllSketch.wrap(WritableMemory.writableWrap(v.asInstanceOf[Array[Byte]])))
        case _ => throw new UnsupportedOperationException(
          s"A Union instance can only be updated with a valid HllSketch byte array")
      }
    }
    union
  }

  /**
   * Merges an input Union into the union which is acting as the aggregation buffer.
   *
   * @param union the Union instance used to store the aggregation result.
   * @param input an input Union instance
   */
  override def merge(union: Union, input: Union): Union = {
    union.update(input.getResult)
    union
  }

  /**
   * Returns the estimated number of unique values derived from the merged HllSketches
   *
   * @param union Union instance used as an aggregation buffer
   * @return A long value representing the number of uniques
   */
  override def eval(union: Union): Any = {
    union.getResult.getEstimate.toLong
  }

  /** Convert the underlying Union into an updateable byte array  */
  override def serialize(union: Union): Array[Byte] = {
    union.toCompactByteArray
  }

  /** De-serializes the updateable byte array into a Union instance */
  override def deserialize(buffer: Array[Byte]): Union = {
    Union.heapify(buffer)
  }
}
