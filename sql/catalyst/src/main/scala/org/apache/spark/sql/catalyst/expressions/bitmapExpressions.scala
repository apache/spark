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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, LongType, StructType}

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
  extends UnaryExpression with RuntimeReplaceable with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_bucket_number"

  override lazy val replacement: Expression = StaticInvoke(
    classOf[BitmapExpressionUtils],
    LongType,
    "bitmapBucketNumber",
    Seq(child),
    inputTypes,
    returnNullable = false)

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
  extends UnaryExpression with RuntimeReplaceable with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_bit_position"

  override lazy val replacement: Expression = StaticInvoke(
    classOf[BitmapExpressionUtils],
    LongType,
    "bitmapBitPosition",
    Seq(child),
    inputTypes,
    returnNullable = false)

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
  extends UnaryExpression with RuntimeReplaceable {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType != BinaryType) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(BinaryType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = LongType

  override def prettyName: String = "bitmap_count"

  override lazy val replacement: Expression = StaticInvoke(
    classOf[BitmapExpressionUtils],
    LongType,
    "bitmapCount",
    Seq(child),
    Seq(BinaryType),
    returnNullable = false)

  override protected def withNewChildInternal(newChild: Expression): BitmapCount =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap with the positions of the bits set from all the values from
    the child expression. The child expression will most likely be bitmap_bit_position().
  """,
  // scalastyle:off line.size.limit
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(bitmap_bit_position(col))), 0, 6) FROM VALUES (1), (2), (3) AS tab(col);
       070000
      > SELECT substring(hex(_FUNC_(bitmap_bit_position(col))), 0, 6) FROM VALUES (1), (1), (1) AS tab(col);
       010000
  """,
  // scalastyle:on line.size.limit
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

  override def aggBufferSchema: StructType = DataTypeUtils.fromAttributes(aggBufferAttributes)

  // The aggregation buffer is a fixed size binary.
  private val bitmapAttr = AttributeReference("bitmap", BinaryType, nullable = false)()

  override def aggBufferAttributes: Seq[AttributeReference] = bitmapAttr :: Nil

  override def defaultResult: Option[Literal] =
    Option(Literal(Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0)))

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0))
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val position = child.eval(input)
    if (position != null) {
      val bitmap = buffer.getBinary(mutableAggBufferOffset)
      val bitPosition = position.asInstanceOf[Long]

      if (bitPosition < 0 || bitPosition >= (8 * bitmap.length)) {
        throw QueryExecutionErrors.invalidBitmapPositionError(bitPosition, bitmap.length)
      }

      val bytePosition = (bitPosition / 8).toInt
      val bit = (bitPosition % 8).toInt
      bitmap.update(bytePosition, (bitmap(bytePosition) | (1 << bit)).toByte)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapExpressionUtils.bitmapMerge(bitmap1, bitmap2)
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
  // scalastyle:off line.size.limit
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X '10'), (X '20'), (X '40') AS tab(col);
       700000
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X '10'), (X '10'), (X '10') AS tab(col);
       100000
  """,
  // scalastyle:on line.size.limit
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
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(BinaryType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)
        )
      )
    } else {
      TypeCheckSuccess
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

  override def aggBufferSchema: StructType = DataTypeUtils.fromAttributes(aggBufferAttributes)

  // The aggregation buffer is a fixed size binary.
  private val bitmapAttr = AttributeReference("bitmap", BinaryType, false)()

  override def aggBufferAttributes: Seq[AttributeReference] = bitmapAttr :: Nil

  override def defaultResult: Option[Literal] =
    Option(Literal(Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0)))

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0))
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val input_bitmap = child.eval(input).asInstanceOf[Array[Byte]]
    if (input_bitmap != null) {
      val bitmap = buffer.getBinary(mutableAggBufferOffset)
      BitmapExpressionUtils.bitmapMerge(bitmap, input_bitmap)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapExpressionUtils.bitmapMerge(bitmap1, bitmap2)
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getBinary(mutableAggBufferOffset)
  }
}
