package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.apache.spark.sql.types._
import org.roaringbitmap.RoaringBitmap
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes
import org.apache.spark.sql.catalyst.expressions.UnaryExpression


@ExpressionDescription(
 usage = "_FUNC_(expr) - Returns the bitmap from values of a group of the offset.",
 examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES;
      +---+--------------------+
      |build_bitmap(offset)    |
      +---+--------------------+
      |[3A 30 00 00 01 0...    |
      +---+--------------------+
""")
case class BuildBitmap(child: Expression,
                              override val mutableAggBufferOffset: Int = 0,
                              override val inputAggBufferOffset: Int = 0)
    extends NullableSketchAggregation {

  def this(child: Expression) = this(child, 0, 0)
  
  override def merge(buffer: Option[RoaringBitmap],
                     other: Option[RoaringBitmap]): Option[RoaringBitmap] =
    (buffer, other) match {
      case (Some(a), Some(b)) =>
        a.or(b)
        Some(a)
      case (a, None) => a
      case (None, b) => b
      case _         => None
    }
  

  override def update(buffer: Option[RoaringBitmap],
                      inputRow: InternalRow): Option[RoaringBitmap] = {
    val value = child.eval(inputRow)
    if (value != null) {
      val rbm1 = value match {
        case b: Array[Byte] =>
          val rbm = new RoaringBitmap
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(b)))
          rbm
        case _ =>
          throw new IllegalStateException(
            s"$prettyName only supports Array[Byte]")
      }
      buffer
        .map(buf => {
          buf.or(rbm1)
          buf
        })
        .orElse(Option(rbm1))
    } else {
      buffer
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case BinaryType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"$prettyName only supports binary input")
    }
  }

  override def dataType: DataType = BinaryType

  def withNewMutableAggBufferOffset(newOffset: Int): BuildBitmap =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): BuildBitmap =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "build_bitmap"
  
}


@ExpressionDescription(
  usage =
     "_FUNC_(expr) - Returns the cardinality of the bitmap from values of a group of the offset.",
  examples = """
    Examples:
      > SELECT _FUNC_(build_bitmap(col)) FROM VALUES;
    3
  """)
case class BitmapCardinality(override val child: Expression)
    extends UnaryExpression
    with ExpectsInputTypes
    with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    val rbm = new RoaringBitmap
    rbm.deserialize(new DataInputStream(new ByteArrayInputStream(data)))
    rbm.getLongCardinality

  }

  override def prettyName: String = "bitmap_cardinality"
}
