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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.util.sketch.BloomFilter

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the mean calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       2.0
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (NULL) AS tab(col);
       1.5
  """,
  group = "agg_funcs",
  since = "1.0.0")
case class BuildBloomFilter(
       child: Expression,
       expectedNumItems: Long = 100000000,
       override val mutableAggBufferOffset: Int,
       override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter]
    with ExpectsInputTypes
    with UnaryLike[Expression] {

  def this(child: Expression) = this(child, expectedNumItems = 100000000, 0, 0)

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(BuildBloomFilter.buildBloomFilterTypes: _*))
  }

  override def createAggregationBuffer(): BloomFilter = {
    BloomFilter.create(expectedNumItems, expectedNumItems.toDouble / 3000000000L.toDouble)
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      child.dataType match {
        case BooleanType =>
          buffer.putLong(if (value.asInstanceOf[JBoolean]) 1L else 0L)
        case _: IntegralType =>
          buffer.putLong(value.asInstanceOf[Number].longValue())
        case DateType | TimestampType =>
          buffer.putLong(value.asInstanceOf[Number].longValue())
        case FloatType =>
          buffer.putLong(JFloat.floatToIntBits(value.asInstanceOf[Float]).toLong)
        case DoubleType =>
          buffer.putLong(JDouble.doubleToLongBits(value.asInstanceOf[Double]))
        case StringType =>
          buffer.putBinary(value.asInstanceOf[UTF8String].getBytes)
        case BinaryType =>
          buffer.putBinary(value.asInstanceOf[Array[Byte]])
        case _: DecimalType =>
          buffer.putBinary(value.asInstanceOf[Decimal].toJavaBigDecimal.unscaledValue().toByteArray)
      }
    }
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(input)
  }

  override def eval(buffer: BloomFilter): Any = serialize(buffer)

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream) { out =>
      buffer.writeTo(out)
      out.toByteArray
    }
  }

  override def deserialize(storageFormat: Array[Byte]): BloomFilter = {
    Utils.tryWithResource(new ByteArrayInputStream(storageFormat)) { in =>
      BloomFilter.readFrom(in)
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): BuildBloomFilter =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): BuildBloomFilter =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def defaultResult: Option[Literal] =
    Option(Literal.create(eval(createAggregationBuffer()), dataType))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object BuildBloomFilter {
  val buildBloomFilterTypes: Seq[AbstractDataType] =
    Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType,
      DateType, TimestampType, StringType, BinaryType)

  def isSupportBuildBloomFilterType(dataType: DataType): Boolean = {
    dataType match {
      case _: DecimalType => true
      case other => buildBloomFilterTypes.contains(other)
    }
  }

  def isSupportBuildBloomFilter(expression: Expression): Boolean = {
    isSupportBuildBloomFilterType(expression.dataType)
  }
}
