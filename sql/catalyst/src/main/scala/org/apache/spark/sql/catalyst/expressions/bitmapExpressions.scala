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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, LongType, StructType}

/** Shared constants and methods for bitmap functions. */
object BitmapFunctions {
  /** Number of bytes in a bitmap. */
  final val NUM_BYTES = 4 * 1024

  /** Number of bits in a bitmap. */
  final val NUM_BITS = 8 * NUM_BYTES

  /** Merges both bitmaps and writes into bitmap1. */
  def merge(bitmap1: Array[Byte], bitmap2: Array[Byte]): Unit = {
    for (i <- 0 until BitmapFunctions.NUM_BYTES) {
      bitmap1.update(i, ((bitmap1(i) & 0x0FF) | (bitmap2(i) & 0x0FF)).toByte)
    }
  }
}

@ExpressionDescription(
  usage = "_FUNC_(child) - Returns the bucket number for the given input child expression.",
  examples = """
    Examples:
      > SELECT _FUNC_(123);
       1
      > SELECT _FUNC_(0);
       0
  """,
  since = "3.5.0",
  group = "misc_funcs"
)
case class BitmapBucketNumber(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_bucket_number"

  protected override def nullSafeEval(input: Any): Any = {
    // inputs are already automatically cast to long.
    val value = input.asInstanceOf[Long]
    if (value > 0) {
      1 + (value - 1) / BitmapFunctions.NUM_BITS
    } else {
      value / BitmapFunctions.NUM_BITS
    }
  }

  override protected def withNewChildInternal(newChild: Expression): BitmapBucketNumber =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(child) - Returns the bit position for the given input child expression.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       0
      > SELECT _FUNC_(123);
       122
  """,
  since = "3.5.0",
  group = "misc_funcs"
)
case class BitmapBitPosition(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_bit_position"

  protected override def nullSafeEval(input: Any): Any = {
    // inputs are already automatically cast to long.
    val value = input.asInstanceOf[Long]
    if (value > 0) {
      // inputs: (1 -> NUM_BITS) map to positions (0 -> NUM_BITS - 1)
      (value - 1) % BitmapFunctions.NUM_BITS
    } else {
      (-value) % BitmapFunctions.NUM_BITS
    }
  }

  override protected def withNewChildInternal(newChild: Expression): BitmapBitPosition =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(child) - Returns the number of set bits in the child bitmap.",
  examples = """
    Examples:
      > SELECT _FUNC_(X '1010');
       2
      > SELECT _FUNC_(X 'FFFF');
       16
      > SELECT _FUNC_(X '0');
       0
  """,
  since = "3.5.0",
  group = "misc_funcs"
)
case class BitmapCount(child: Expression)
  extends UnaryExpression with CodegenFallback {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType != BinaryType) {
      TypeCheckResult.TypeCheckFailure("Bitmap must be a BinaryType")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_count"

  protected override def nullSafeEval(input: Any): Any = {
    val bitmap = input.asInstanceOf[Array[Byte]]
    bitmap.map(b => java.lang.Integer.bitCount(b & 0x0FF)).sum.toLong
  }

  override protected def withNewChildInternal(newChild: Expression): BitmapCount =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap with the positions of the bits set from all the values from
    the child expression. The child expression will most likely be bitmap_bit_position().
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(bitmap_bit_position(col)) FROM VALUES (1), (2), (3) AS tab(col);
       [07 00 00 ...]
      > SELECT _FUNC_(bitmap_bit_position(col)) FROM VALUES (1), (1), (1) AS tab(col);
       [01 00 00 ...]
  """,
  since = "3.5.0",
  group = "agg_funcs"
)
case class BitmapConstructAgg(child: Expression,
                              mutableAggBufferOffset: Int = 0,
                              inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate with ImplicitCastInputTypes with UnaryLike[Expression] {

  def this(child: Expression) = {
    this(child = child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bitmap_construct_agg"

  override protected def withNewChildInternal(newChild: Expression): BitmapConstructAgg =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = bitmapAttr :: Nil

  override def defaultResult: Option[Literal] =
    Option(Literal(Array.fill[Byte](BitmapFunctions.NUM_BYTES)(0)))

  override def inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // The aggregation buffer is a fixed size binary.
  private val bitmapAttr = AttributeReference("bitmap", BinaryType, nullable = false)()

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, Array.fill[Byte](BitmapFunctions.NUM_BYTES)(0))
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val position = child.eval(input)
    if (position != null) {
      val bitmap = buffer.getBinary(mutableAggBufferOffset)
      val bitPosition = position.asInstanceOf[Long]
      val bytePosition = (bitPosition / 8).toInt
      val bit = (bitPosition % 8).toInt

      bitmap.update(bytePosition, (bitmap(bytePosition) | (1 << bit)).toByte)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapFunctions.merge(bitmap1, bitmap2)
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getBinary(mutableAggBufferOffset)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap that is the bitwise OR of all of the bitmaps from the child
    expression. The input should be bitmaps created from bitmap_construct_agg().
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (X '10'), (X '20'), (X '40') AS tab(col);
       [70]
      > SELECT _FUNC_(col) FROM VALUES (X '10'), (X '10'), (X '10') AS tab(col);
       [10]
  """,
  since = "3.5.0",
  group = "agg_funcs"
)
case class BitmapOrAgg(child: Expression,
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate with UnaryLike[Expression] {

  def this(child: Expression) = {
    this(child = child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType != BinaryType) {
      TypeCheckResult.TypeCheckFailure("Bitmap must be a BinaryType")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bitmap_or_agg"

  override protected def withNewChildInternal(newChild: Expression): BitmapOrAgg =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = bitmapAttr :: Nil

  override def defaultResult: Option[Literal] =
    Option(Literal(Array.fill[Byte](BitmapFunctions.NUM_BYTES)(0)))

  override def inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // The aggregation buffer is a fixed size binary.
  private val bitmapAttr = AttributeReference("bitmap", BinaryType, false)()

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, Array.fill[Byte](BitmapFunctions.NUM_BYTES)(0))
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val input_bitmap = child.eval(input).asInstanceOf[Array[Byte]]
    if (input_bitmap != null) {
      val bitmap = buffer.getBinary(mutableAggBufferOffset)
      BitmapFunctions.merge(bitmap, input_bitmap)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapFunctions.merge(bitmap1, bitmap2)
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getBinary(mutableAggBufferOffset)
  }
}
