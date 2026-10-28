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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, LongType, StructType}

@ExpressionDescription(
  usage = "_FUNC_(child) - Returns the bucket number for the given input child expression.",
  arguments = """
    Arguments:
      * child - The input expression to compute the bucket number for.
        An expression that evaluates to a long.
  """,
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
  arguments = """
    Arguments:
      * child - The input expression to compute the bit position for.
        An expression that evaluates to a long.
  """,
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
  arguments = """
    Arguments:
      * child - The bitmap whose set bits are counted. An expression that evaluates to a binary
          bitmap, typically produced by bitmap_construct_agg().
  """,
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

/** Base class for scalar bitmap binary operations. */
abstract class BitmapBinaryExpression extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override def nullIntolerant: Boolean = true

  protected def applyOperation(bitmap1: Array[Byte], bitmap2: Array[Byte]): Array[Byte]

  protected def genCodeOperation(
      bitmapUtils: String, bitmap1: String, bitmap2: String): String

  private def checkBitmapLength(bitmap: Array[Byte]): Unit = {
    if (bitmap.length > BitmapExpressionUtils.NUM_BYTES) {
      throw QueryExecutionErrors.bitmapInputTooLargeError(
        bitmap.length, BitmapExpressionUtils.NUM_BYTES)
    }
  }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val bitmap1 = input1.asInstanceOf[Array[Byte]]
    val bitmap2 = input2.asInstanceOf[Array[Byte]]
    checkBitmapLength(bitmap1)
    checkBitmapLength(bitmap2)
    applyOperation(bitmap1, bitmap2)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val bitmapUtils = classOf[BitmapExpressionUtils].getName
    val errors = QueryExecutionErrors.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (bitmap1, bitmap2) => {
      s"""
         |if ($bitmap1.length > ${BitmapExpressionUtils.NUM_BYTES}) {
         |  throw $errors.bitmapInputTooLargeError(
         |    $bitmap1.length, ${BitmapExpressionUtils.NUM_BYTES});
         |}
         |if ($bitmap2.length > ${BitmapExpressionUtils.NUM_BYTES}) {
         |  throw $errors.bitmapInputTooLargeError(
         |    $bitmap2.length, ${BitmapExpressionUtils.NUM_BYTES});
         |}
         |${ev.value} = ${genCodeOperation(bitmapUtils, bitmap1, bitmap2)};
         |""".stripMargin
    })
  }
}

@ExpressionDescription(
  usage = "_FUNC_(left, right) - Returns a bitmap that is the bitwise AND of two input bitmaps.",
  arguments = """
    Arguments:
      * left - A binary bitmap.
      * right - A binary bitmap.
  """,
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(X 'F0', X '70')), 0, 2);
       70
  """,
  note = """
    Inputs use Spark's Binary bitmap representation, not a RoaringBitmap serialization. Each
    input may contain 0 to 4096 bytes; missing bytes are treated as zero. The result is always a
    4096-byte Binary value. NULL input returns NULL, and inputs longer than 4096 bytes raise
    BITMAP_INPUT_TOO_LARGE. Both inputs must use the same bit-position mapping. If they were
    constructed by grouping bitmap_bit_position values by bitmap_bucket_number, they must
    represent the same bucket because the bitmap bytes do not retain bucket metadata. This scalar
    function combines two bitmaps from the same row; use bitmap_*_agg to combine bitmaps across
    rows.
  """,
  since = "4.4.0",
  group = "misc_funcs"
)
case class BitmapAnd(left: Expression, right: Expression) extends BitmapBinaryExpression {

  override def prettyName: String = "bitmap_and"

  override protected def applyOperation(
      bitmap1: Array[Byte], bitmap2: Array[Byte]): Array[Byte] =
    BitmapExpressionUtils.bitmapAnd(bitmap1, bitmap2)

  override protected def genCodeOperation(
      bitmapUtils: String, bitmap1: String, bitmap2: String): String =
    s"$bitmapUtils.bitmapAnd($bitmap1, $bitmap2)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BitmapAnd =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(left, right) - Returns a bitmap that is the bitwise OR of two input bitmaps.",
  arguments = """
    Arguments:
      * left - A binary bitmap.
      * right - A binary bitmap.
  """,
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(X '10', X '20')), 0, 2);
       30
  """,
  note = """
    Inputs use Spark's Binary bitmap representation, not a RoaringBitmap serialization. Each
    input may contain 0 to 4096 bytes; missing bytes are treated as zero. The result is always a
    4096-byte Binary value. NULL input returns NULL, and inputs longer than 4096 bytes raise
    BITMAP_INPUT_TOO_LARGE. Both inputs must use the same bit-position mapping. If they were
    constructed by grouping bitmap_bit_position values by bitmap_bucket_number, they must
    represent the same bucket because the bitmap bytes do not retain bucket metadata. This scalar
    function combines two bitmaps from the same row; use bitmap_*_agg to combine bitmaps across
    rows.
  """,
  since = "4.4.0",
  group = "misc_funcs"
)
case class BitmapOr(left: Expression, right: Expression) extends BitmapBinaryExpression {

  override def prettyName: String = "bitmap_or"

  override protected def applyOperation(
      bitmap1: Array[Byte], bitmap2: Array[Byte]): Array[Byte] =
    BitmapExpressionUtils.bitmapOr(bitmap1, bitmap2)

  override protected def genCodeOperation(
      bitmapUtils: String, bitmap1: String, bitmap2: String): String =
    s"$bitmapUtils.bitmapOr($bitmap1, $bitmap2)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BitmapOr =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(left, right) - Returns a bitmap that is the bitwise AND NOT of two bitmaps.",
  arguments = """
    Arguments:
      * left - A binary bitmap.
      * right - A binary bitmap.
  """,
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(X 'F0', X '70')), 0, 2);
       80
  """,
  note = """
    Inputs use Spark's Binary bitmap representation, not a RoaringBitmap serialization. Each
    input may contain 0 to 4096 bytes; missing bytes are treated as zero. The result is always a
    4096-byte Binary value. NULL input returns NULL, and inputs longer than 4096 bytes raise
    BITMAP_INPUT_TOO_LARGE. Both inputs must use the same bit-position mapping. If they were
    constructed by grouping bitmap_bit_position values by bitmap_bucket_number, they must
    represent the same bucket because the bitmap bytes do not retain bucket metadata. This scalar
    function combines two bitmaps from the same row; use bitmap_*_agg to combine bitmaps across
    rows.
  """,
  since = "4.4.0",
  group = "misc_funcs"
)
case class BitmapAndNot(left: Expression, right: Expression) extends BitmapBinaryExpression {

  override def prettyName: String = "bitmap_andnot"

  override protected def applyOperation(
      bitmap1: Array[Byte], bitmap2: Array[Byte]): Array[Byte] =
    BitmapExpressionUtils.bitmapAndNot(bitmap1, bitmap2)

  override protected def genCodeOperation(
      bitmapUtils: String, bitmap1: String, bitmap2: String): String =
    s"$bitmapUtils.bitmapAndNot($bitmap1, $bitmap2)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BitmapAndNot =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(left, right) - Returns a bitmap that is the bitwise XOR of two input bitmaps.",
  arguments = """
    Arguments:
      * left - A binary bitmap.
      * right - A binary bitmap.
  """,
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(X 'F0', X '70')), 0, 2);
       80
  """,
  note = """
    Inputs use Spark's Binary bitmap representation, not a RoaringBitmap serialization. Each
    input may contain 0 to 4096 bytes; missing bytes are treated as zero. The result is always a
    4096-byte Binary value. NULL input returns NULL, and inputs longer than 4096 bytes raise
    BITMAP_INPUT_TOO_LARGE. Both inputs must use the same bit-position mapping. If they were
    constructed by grouping bitmap_bit_position values by bitmap_bucket_number, they must
    represent the same bucket because the bitmap bytes do not retain bucket metadata. This scalar
    function combines two bitmaps from the same row; use bitmap_*_agg to combine bitmaps across
    rows.
  """,
  since = "4.4.0",
  group = "misc_funcs"
)
case class BitmapXor(left: Expression, right: Expression) extends BitmapBinaryExpression {

  override def prettyName: String = "bitmap_xor"

  override protected def applyOperation(
      bitmap1: Array[Byte], bitmap2: Array[Byte]): Array[Byte] =
    BitmapExpressionUtils.bitmapXor(bitmap1, bitmap2)

  override protected def genCodeOperation(
      bitmapUtils: String, bitmap1: String, bitmap2: String): String =
    s"$bitmapUtils.bitmapXor($bitmap1, $bitmap2)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BitmapXor =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap with the positions of the bits set from all the values from
    the child expression. The child expression will most likely be bitmap_bit_position().
  """,
  arguments = """
    Arguments:
      * child - The expression whose values set the bit positions in the bitmap.
        An expression that evaluates to a long.
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
  arguments = """
    Arguments:
      * child - The expression whose bitmap values are combined with a bitwise OR.
          An expression that evaluates to a binary bitmap created from bitmap_construct_agg().
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

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap that is the bitwise AND of all of the bitmaps from the child
    expression. The input should be bitmaps created from bitmap_construct_agg().
  """,
  arguments = """
    Arguments:
      * child - The expression whose bitmap values are combined with a bitwise AND.
          An expression that evaluates to a binary bitmap created from bitmap_construct_agg().
  """,
  // scalastyle:off line.size.limit
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X 'F0'), (X '70'), (X '30') AS tab(col);
       300000
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X 'FF'), (X 'FF'), (X 'FF') AS tab(col);
       FF0000
  """,
  // scalastyle:on line.size.limit
  since = "4.1.0",
  group = "agg_funcs")
case class BitmapAndAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate
    with UnaryLike[Expression] {

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
          "inputType" -> toSQLType(child.dataType)))
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bitmap_and_agg"

  override protected def withNewChildInternal(newChild: Expression): BitmapAndAgg =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def aggBufferSchema: StructType = DataTypeUtils.fromAttributes(aggBufferAttributes)

  // The aggregation buffer is a fixed size binary.
  private val bitmapAttr = AttributeReference("bitmap", BinaryType, false)()

  override def aggBufferAttributes: Seq[AttributeReference] = bitmapAttr :: Nil

  override def defaultResult: Option[Literal] =
    Option(Literal(Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(-1)))

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(-1))
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val input_bitmap = child.eval(input).asInstanceOf[Array[Byte]]
    if (input_bitmap != null) {
      val bitmap = buffer.getBinary(mutableAggBufferOffset)
      BitmapExpressionUtils.bitmapAndMerge(bitmap, input_bitmap)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapExpressionUtils.bitmapAndMerge(bitmap1, bitmap2)
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getBinary(mutableAggBufferOffset)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns a bitmap that is the bitwise XOR of all of the bitmaps from the child
    expression. The input should be bitmaps created from bitmap_construct_agg().
  """,
  arguments = """
    Arguments:
      * child - The expression whose bitmap values are combined with a bitwise XOR.
          An expression that evaluates to a binary bitmap created from bitmap_construct_agg().
  """,
  // scalastyle:off line.size.limit
  examples = """
    Examples:
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X'10'), (X'30'), (X'40') AS tab(col);
       600000
      > SELECT substring(hex(_FUNC_(col)), 0, 6) FROM VALUES (X'10'), (X'10') AS tab(col);
       000000
  """,
  // scalastyle:on line.size.limit
  since = "4.4.0",
  group = "agg_funcs")
case class BitmapXorAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate
    with UnaryLike[Expression] {

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
          "inputType" -> toSQLType(child.dataType)))
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bitmap_xor_agg"

  override protected def withNewChildInternal(newChild: Expression): BitmapXorAgg =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
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
      BitmapExpressionUtils.bitmapXorMerge(bitmap, input_bitmap)
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val bitmap1 = buffer1.getBinary(mutableAggBufferOffset)
    val bitmap2 = buffer2.getBinary(inputAggBufferOffset)
    BitmapExpressionUtils.bitmapXorMerge(bitmap1, bitmap2)
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getBinary(mutableAggBufferOffset)
  }
}
