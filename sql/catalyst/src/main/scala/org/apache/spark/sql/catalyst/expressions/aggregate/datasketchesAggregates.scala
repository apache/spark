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

import org.apache.datasketches.common.SketchesArgumentException
import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.collation.CollationFactory
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCaseAccentSensitivity
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, DataType, IntegerType, LongType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String


/**
 * The HllSketchAgg function utilizes a Datasketches HllSketch instance to count a probabilistic
 * approximation of the number of unique values in a given column, and outputs the binary
 * representation of the HllSketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information.
 *
 * @param left child expression against which unique counting will occur
 * @param right the log-base-2 of K, where K is the number of buckets or slots for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgConfigK) - Returns the HllSketch's updatable binary representation.
      `lgConfigK` (optional) the log-base-2 of K, with K is the number of buckets or
      slots for the HllSketch. """,
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(col, 12)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "agg_funcs",
  since = "3.5.0")
// scalastyle:on line.size.limit
case class HllSketchAgg(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HllSketch] with BinaryLike[Expression] with ExpectsInputTypes {

  // Hllsketch config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val lgConfigK: Int = {
    val lgConfigK = right.eval().asInstanceOf[Int]
    HllSketchAgg.checkLgK(lgConfigK)
    lgConfigK
  }

  // The default target type (register size) to use.
  private val targetType = TgtHllType.HLL_8

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(HllSketch.DEFAULT_LG_K), 0, 0)
  }

  def this(child: Expression, lgConfigK: Expression) = {
    this(child, lgConfigK, 0, 0)
  }

  def this(child: Expression, lgConfigK: Int) = {
    this(child, Literal(lgConfigK), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HllSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): HllSketchAgg =
    copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "hll_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        IntegerType,
        LongType,
        StringTypeWithCaseAccentSensitivity(/* supportsTrimCollation = */ true),
        BinaryType),
      IntegerType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Instantiate an HllSketch instance using the lgConfigK param.
   *
   * @return an HllSketch instance
   */
  override def createAggregationBuffer(): HllSketch = {
    new HllSketch(lgConfigK, targetType)
  }

  /**
   * Evaluate the input row and update the HllSketch instance with the row's value. The update
   * function only supports a subset of Spark SQL types, and an exception will be thrown for
   * unsupported types.
   *
   * @param sketch The HllSketch instance.
   * @param input  an input row
   */
  override def update(sketch: HllSketch, input: InternalRow): HllSketch = {
    val v = left.eval(input)
    if (v != null) {
      left.dataType match {
        // This is implemented for a subset of input data types.
        // Spark SQL doesn't have equivalent types for ByteBuffer or char[] so leave those out.
        // We leave out support for Array types, as unique counting these aren't a common use case.
        // We leave out support for floating point types (such as DoubleType) due to imprecision.
        // TODO: implement support for decimal/datetime/interval types
        case IntegerType => sketch.update(v.asInstanceOf[Int])
        case LongType => sketch.update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          sketch.update(cKey.toString)
        case BinaryType => sketch.update(v.asInstanceOf[Array[Byte]])
        case dataType => throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3121",
          messageParameters = Map("dataType" -> dataType.toString))
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
    union.getResult(targetType)
  }

  /**
   * Returns an HllSketch derived from the input column or expression
   *
   * @param sketch HllSketch instance used as an aggregation buffer
   * @return A binary sketch which can be evaluated or merged
   */
  override def eval(sketch: HllSketch): Any = {
    sketch.toUpdatableByteArray
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
 * Companion object to HllSketchAgg, meant to encapsulate static
 * shared functions like checkLgK
 */
object HllSketchAgg {
  // lgConfigK min/max values copied from Dataksketches HllUtil. These
  // limits intended to to bound sketch error and size
  // https://datasketches.apache.org/docs/HLL/HLL.html
  private val minLgConfigK = 4
  private val maxLgConfigK = 21

  // Replicate Datasketches' HllUtil's checkLgK implementation, as we can't reference it directly.
  def checkLgK(lgConfigK: Int): Unit = {
    if (lgConfigK < minLgConfigK || lgConfigK > maxLgConfigK) {
      throw QueryExecutionErrors.hllInvalidLgK(function = "hll_sketch_agg",
        min = minLgConfigK, max = maxLgConfigK, value = lgConfigK)
    }
  }
}

/**
 * The HllUnionAgg function ingests and merges Datasketches HllSketch instances previously produced
 * by the HllSketchBinary function, and outputs the merged HllSketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information.
 *
 * @param left Child expression against which unique counting will occur
 * @param right Allow sketches with different lgConfigK values
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, allowDifferentLgConfigK) - Returns the estimated number of unique values.
      `allowDifferentLgConfigK` (optional) Allow sketches with different lgConfigK values
       to be unioned (defaults to false).""",
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(sketch, true)) FROM (SELECT hll_sketch_agg(col) as sketch FROM VALUES (1) tab(col) UNION ALL SELECT hll_sketch_agg(col, 20) as sketch FROM VALUES (1) tab(col));
       1
  """,
  group = "agg_funcs",
  since = "3.5.0")
// scalastyle:on line.size.limit
case class HllUnionAgg(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Option[Union]]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  // Union config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val allowDifferentLgConfigK: Boolean = {
    right.eval().asInstanceOf[Boolean]
  }

  // The default target type (register size) to use.
  private val targetType = TgtHllType.HLL_8

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(false), 0, 0)
  }

  def this(child: Expression, allowDifferentLgConfigK: Expression) = {
    this(child, allowDifferentLgConfigK, 0, 0)
  }

  def this(child: Expression, allowDifferentLgConfigK: Boolean) = {
    this(child, Literal(allowDifferentLgConfigK), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int):
  HllUnionAgg = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HllUnionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
  HllUnionAgg = copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "hll_union_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BooleanType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Defer instantiation of the Union instance until we've deserialized
   * our first hll sketch, and use that sketch's lgConfigK value
   *
   * @return None
   */
  override def createAggregationBuffer(): Option[Union] = {
    None
  }

  /**
   * Helper method to compare lgConfigKs and throw an exception if `allowDifferentLgConfigK` isn't
   * true and configs don't match.
   *
   * @param left An lgConfigK value
   * @param right An lgConfigK value
   */
  def compareLgConfigK(left: Int, right: Int): Unit = {
    if (!allowDifferentLgConfigK && left != right) {
      throw QueryExecutionErrors.hllUnionDifferentLgK(left, right, function = prettyName)
    }
  }

  /**
   * Update the Union instance with the HllSketch byte array obtained from the row.
   *
   * @param unionOption A previously initialized Union instance, or None
   * @param input An input row
   */
  override def update(unionOption: Option[Union], input: InternalRow): Option[Union] = {
    val v = left.eval(input)
    if (v != null) {
      left.dataType match {
        case BinaryType =>
          try {
            val sketch = HllSketch.wrap(Memory.wrap(v.asInstanceOf[Array[Byte]]))
            val union = unionOption.getOrElse(new Union(sketch.getLgConfigK))
            compareLgConfigK(union.getLgConfigK, sketch.getLgConfigK)
            union.update(sketch)
            Some(union)
          } catch {
            case _: SketchesArgumentException | _: java.lang.Error =>
              throw QueryExecutionErrors.hllInvalidInputSketchBuffer(prettyName)
          }
        case _ =>
          throw QueryExecutionErrors.hllInvalidInputSketchBuffer(prettyName)
      }
    } else {
      unionOption
    }
  }

  /**
   * Merges an input Union into the union which is acting as the aggregation buffer.
   *
   * @param unionOption The Union instance used to store the aggregation result
   * @param inputOption An input Union instance
   */
  override def merge(unionOption: Option[Union], inputOption: Option[Union]): Option[Union] = {
    (unionOption, inputOption) match {
      case (Some(union), Some(input)) =>
        compareLgConfigK(union.getLgConfigK, input.getLgConfigK)
        union.update(input.getResult(targetType))
        Some(union)
      // unclear if these scenarios can ever occur
      case (Some(_), None) =>
        unionOption
      case (None, Some(_)) =>
        inputOption
      case (None, None) =>
        unionOption
    }
  }

  /**
   * Returns an HllSketch derived from the merged HllSketches
   *
   * @param unionOption Union instance used as an aggregation buffer
   * @return A binary sketch which can be evaluated or merged
   */
  override def eval(unionOption: Option[Union]): Any = {
    unionOption match {
      case Some(union) => union.toUpdatableByteArray
      // unclear if these scenarios can ever occur
      case None => new Union().toUpdatableByteArray
    }
  }

  /** Convert the underlying Union into an updatable byte array  */
  override def serialize(unionOption: Option[Union]): Array[Byte] = {
    unionOption match {
      case Some(union) => union.toUpdatableByteArray
      // unclear if these scenarios can ever occur
      case None => new Array[Byte](0)
    }
  }

  /** De-serializes the updatable byte array into a Union instance */
  override def deserialize(buffer: Array[Byte]): Option[Union] = {
    if (buffer.length != 0) {
      Some(Union.heapify(buffer))
    // unclear if these scenarios can ever occur
    } else {
      None
    }
  }
}
