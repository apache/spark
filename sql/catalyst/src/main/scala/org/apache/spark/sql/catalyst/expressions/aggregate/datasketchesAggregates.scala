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

import org.apache.datasketches.SketchesArgumentException
import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BinaryType, DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String


/**
 * The HllSketchAgg function utilizes a Datasketches HllSketch instance to
 * probabilistically count the number of unique values in a given column, and
 * outputs the binary representation of the HllSketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information
 *
 * @param child child expression against which unique counting will occur
 * @param lgConfigK the log-base-2 of K, where K is the number of buckets or slots for the sketch
 * @param tgtHllType the target type of the HllSketch to be used (HLL_4, HLL_6, HLL_8)
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgConfigK, tgtHllType) - Returns the HllSketch's compact binary representation.
      `lgConfigK` (optional) the log-base-2 of K, with K = the number of buckets for the HllSketch.
      `tgtHllType` (optional) the target type of the HllSketch (HLL_4, HLL_6, HLL_8). """,
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(col1, 12, 'HLL_4'))
      FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
case class HllSketchAgg(
    child: Expression,
    lgConfigKExpression: Expression,
    tgtHllTypeExpression: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HllSketch] with TernaryLike[Expression] with ExpectsInputTypes {

  // Hllsketch config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val lgConfigK: Int = second.eval().asInstanceOf[Int]
  lazy val tgtHllType: TgtHllType = try {
    TgtHllType.valueOf(third.eval().asInstanceOf[UTF8String].toString.toUpperCase(Locale.ROOT))
  } catch {
    case _: IllegalArgumentException =>
      throw new SketchesArgumentException("Invalid tgtHllType value")
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(HllSketch.DEFAULT_LG_K), Literal(HllSketch.DEFAULT_HLL_TYPE.toString), 0, 0)
  }

  def this(child: Expression, lgConfigK: Expression) = {
    this(child, lgConfigK, Literal(HllSketch.DEFAULT_HLL_TYPE.toString), 0, 0)
  }

  def this(child: Expression, lgConfigK: Int) = {
    this(child, Literal(lgConfigK), Literal(HllSketch.DEFAULT_HLL_TYPE.toString), 0, 0)
  }

  def this(child: Expression, lgConfigK: Expression, tgtHllType: Expression) = {
    this(child, lgConfigK, tgtHllType, 0, 0)
  }

  def this(child: Expression, lgConfigK: Int, tgtHllType: String) = {
    this(child, Literal(lgConfigK), Literal(tgtHllType), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HllSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newFirst: Expression,
                                              newSecond: Expression,
                                              newThird: Expression): HllSketchAgg =
    copy(child = newFirst, lgConfigKExpression = newSecond, tgtHllTypeExpression = newThird)

  // Overrides for TernaryLike

  override def first: Expression = child

  override def second: Expression = lgConfigKExpression

  override def third: Expression = tgtHllTypeExpression

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "hll_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType, StringType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Instantiate an HllSketch instance using the lgConfigK and tgtHllType params.
   *
   * @return an HllSketch instance
   */
  override def createAggregationBuffer(): HllSketch = {
    new HllSketch(lgConfigK, tgtHllType)
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
    val v = first.eval(input)
    if (v != null) {
      first.dataType match {
        // Update implemented for a subset of types supported by HllSketch
        // Spark SQL doesn't have equivalent types for ByteBuffer or char[] so leave those out
        // Leaving out support for Array types, as unique counting these aren't a common use case
        // Leaving out support for floating point types (IE DoubleType) due to imprecision
        // TODO: implement support for decimal/datetime/interval types
        case IntegerType => sketch.update(v.asInstanceOf[Int])
        case LongType => sketch.update(v.asInstanceOf[Long])
        case StringType => sketch.update(v.asInstanceOf[UTF8String].toString)
        case BinaryType => sketch.update(v.asInstanceOf[Array[Byte]])
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
    val union = new Union(sketch.getLgConfigK)
    union.update(sketch)
    union.update(input)
    union.getResult(sketch.getTgtHllType)
  }

  /**
   * Returns an HllSketch derived from the input column or expression
   *
   * @param sketch HllSketch instance used as an aggregation buffer
   * @return A binary sketch which can be evaluated or merged
   */
  override def eval(sketch: HllSketch): Any = {
    sketch.toCompactByteArray
  }

  /** Convert the underlying HllSketch into an updatable byte array  */
  override def serialize(sketch: HllSketch): Array[Byte] = {
    sketch.toUpdatableByteArray
  }

  /** De-serializes the updatable byte array into a HllSketch instance */
  override def deserialize(buffer: Array[Byte]): HllSketch = {
    HllSketch.heapify(buffer)
  }
}

/**
 * The HllUnionAgg function ingests and merges Datasketches HllSketch
 * instances previously produced by the HllSketchBinary function, and
 * outputs the merged HllSketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information
 *
 * @param child child expression against which unique counting will occur
 * @param lgMaxK The largest maximum size for lgConfigK for the union operation.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgMaxK) - Returns the estimated number of unique values.
      `lgMaxK` (optional) The largest maximum size for lgConfigK for the union operation.""",
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(sketch, 12))
      FROM (
        SELECT hll_sketch_agg(col1) as sketch FROMVALUES (1), (1), (2), (2), (3) tab(col1)
        UNION ALL
        SELECT hll_sketch_agg(col1) as sketch FROMVALUES (4), (4), (5), (5), (6) tab(col1)
      );
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
case class HllUnionAgg(
    child: Expression,
    lgMaxKExpression: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Union] with BinaryLike[Expression] with ExpectsInputTypes {

  // Union config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val lgMaxK: Int = lgMaxKExpression.eval().asInstanceOf[Int]

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(HllSketch.DEFAULT_LG_K), 0, 0)
  }

  def this(child: Expression, lgMaxK: Expression) = {
    this(child, lgMaxK, 0, 0)
  }

  def this(child: Expression, lgMaxK: Int) = {
    this(child, Literal(lgMaxK), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int):
  HllUnionAgg = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllUnionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
  HllUnionAgg = copy(child = newLeft, lgMaxKExpression = newRight)

  // Overrides for BinaryLike

  override def left: Expression = child

  override def right: Expression = lgMaxKExpression

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "hll_union_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

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
          union.update(HllSketch.wrap(Memory.wrap(v.asInstanceOf[Array[Byte]])))
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
   * Returns an HllSketch derived from the merged HllSketches
   *
   * @param union Union instance used as an aggregation buffer
   * @return A binary sketch which can be evaluated or merged
   */
  override def eval(union: Union): Any = {
    union.toCompactByteArray
  }

  /** Convert the underlying Union into an updatable byte array  */
  override def serialize(union: Union): Array[Byte] = {
    union.toUpdatableByteArray
  }

  /** De-serializes the updatable byte array into a Union instance */
  override def deserialize(buffer: Array[Byte]): Union = {
    Union.heapify(buffer)
  }
}
