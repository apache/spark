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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

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
    extends TypedImperativeAggregate[Option[RoaringBitmap]]  {

  def this(child: Expression) = this(child, 0, 0)

  override def createAggregationBuffer(): Option[RoaringBitmap] = None

  override def merge(buffer: Option[RoaringBitmap],
                     other: Option[RoaringBitmap]): Option[RoaringBitmap] =
    (buffer, other) match {
      case (Some(a), Some(b)) =>
        a.or(b)
        Some(a)
      case (a, None) => a
      case (None, b) => b
      case _ => None
    }

  override def eval(buffer: Option[RoaringBitmap]): Any = {
    buffer
      .map(rbm => {
        val bos = new ByteArrayOutputStream
        rbm.serialize(new DataOutputStream(bos))
        bos.toByteArray
      })
      .orNull
  }

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = child.nullable

  override def serialize(buffer: Option[RoaringBitmap]): Array[Byte] = {
    buffer
      .map(rbm => {
        val bos = new ByteArrayOutputStream
        rbm.serialize(new DataOutputStream(bos))
        bos.toByteArray
      })
      .orNull
  }

  override def deserialize(bytes: Array[Byte]): Option[RoaringBitmap] = {
    if (bytes == null) {
      return None
    }

    val rbm = new RoaringBitmap
    rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
    Option(rbm)
  }

  override def update(buffer: Option[RoaringBitmap],
                      inputRow: InternalRow): Option[RoaringBitmap] = {
    val value = child.eval(inputRow)
    if (value != null) {
      val bitmap = value match {
        case bytes: Array[Byte] =>
          val rbm = new RoaringBitmap
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
          rbm
        case _ =>
          throw new IllegalStateException(
            s"$prettyName only supports array of bytes")
      }
      buffer.map(buf => {
          buf.or(bitmap)
          buf
        })
        .orElse(Option(bitmap))
      } else {
      buffer
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case BinaryType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"Type error, $prettyName only supports binary type")
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
    val bytes = input.asInstanceOf[Array[Byte]]
    val rbm = new RoaringBitmap
    rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
    rbm.getLongCardinality

  }

  override def prettyName: String = "bitmap_cardinality"
}
