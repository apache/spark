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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

/**
 * An internal aggregate function that creates a Bloom filter from input values.
 *
 * @param child                     Child expression of Long values for creating a Bloom filter.
 * @param estimatedNumItemsExpression The number of estimated distinct items (optional).
 * @param numBitsExpression         The number of bits to use (optional).
 */
case class BloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter] with TernaryLike[Expression] {

  def this(child: Expression, estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression) = {
    this(child, estimatedNumItemsExpression, numBitsExpression, 0, 0)
  }

  def this(child: Expression, estimatedNumItemsExpression: Expression) = {
    this(child, estimatedNumItemsExpression,
      // 1 byte per item.
      Multiply(estimatedNumItemsExpression, Literal(8L)))
  }

  def this(child: Expression) = {
    this(child, Literal(BloomFilterAggregate.DEFAULT_EXPECTED_NUM_ITEMS),
      Literal(BloomFilterAggregate.DEFAULT_NUM_BITS))
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = (first.dataType, second.dataType, third.dataType) match {
      case (_, NullType, _) | (_, _, NullType) =>
        TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as size arguments")
      case (LongType, LongType, LongType) => TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
        s"been a ${LongType.simpleString} value followed with two ${LongType.simpleString} size " +
        s"arguments, but it's [${first.dataType.catalogString}, " +
        s"${second.dataType.catalogString}, ${third.dataType.catalogString}]")
    }
    if (typeCheckResult.isFailure) {
      return typeCheckResult
    }
    if (!estimatedNumItemsExpression.foldable) {
      TypeCheckFailure("The estimated number of items provided must be a constant literal")
    } else if (estimatedNumItems <= 0L) {
      TypeCheckFailure("The estimated number of items must be a positive value " +
        s" (current value = $estimatedNumItems)")
    } else if (!numBitsExpression.foldable) {
      TypeCheckFailure("The number of bits provided must be a constant literal")
    } else if (numBits <= 0L) {
      TypeCheckFailure("The number of bits must be a positive value " +
        s" (current value = $numBits)")
    } else {
      require(estimatedNumItems <= BloomFilterAggregate.MAX_ALLOWED_NUM_ITEMS)
      require(numBits <= BloomFilterAggregate.MAX_NUM_BITS)
      TypeCheckSuccess
    }
  }
  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bloom_filter_agg"

  // Mark as lazy so that `estimatedNumItems` is not evaluated during tree transformation.
  private lazy val estimatedNumItems: Long =
    Math.min(estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
      BloomFilterAggregate.MAX_ALLOWED_NUM_ITEMS)

  // Mark as lazy so that `numBits` is not evaluated during tree transformation.
  private lazy val numBits: Long =
    Math.min(numBitsExpression.eval().asInstanceOf[Number].longValue,
      BloomFilterAggregate.MAX_NUM_BITS)

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override protected def withNewChildrenInternal(newChild: Expression,
      newEstimatedNumItemsExpression: Expression, newNumBitsExpression: Expression)
  : BloomFilterAggregate = {
    copy(child = newChild, estimatedNumItemsExpression = newEstimatedNumItemsExpression,
      numBitsExpression = newNumBitsExpression)
  }

  override def createAggregationBuffer(): BloomFilter = {
    BloomFilter.create(estimatedNumItems, numBits)
  }

  override def update(buffer: BloomFilter, inputRow: InternalRow): BloomFilter = {
    val value = child.eval(inputRow)
    // Ignore null values.
    if (value == null) {
      return buffer
    }
    buffer.putLong(value.asInstanceOf[Long])
    buffer
  }

  override def merge(buffer: BloomFilter, other: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(other)
  }

  override def eval(buffer: BloomFilter): Any = {
    if (buffer.cardinality() == 0) {
      // There's no set bit in the Bloom filter and hence no not-null value is processed.
      return null
    }
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): BloomFilterAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): BloomFilterAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def serialize(obj: BloomFilter): Array[Byte] = {
    BloomFilterAggregate.serde.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): BloomFilter = {
    BloomFilterAggregate.serde.deserialize(bytes)
  }
}

object BloomFilterAggregate {

  val DEFAULT_EXPECTED_NUM_ITEMS: Long = 1000000L // Default 1M distinct items

  val MAX_ALLOWED_NUM_ITEMS: Long = 4000000L // At most 4M distinct items

  val DEFAULT_NUM_BITS: Long = 8388608 // Default 1MB

  val MAX_NUM_BITS: Long = 67108864 // At most 8MB

  /**
   * Serializer/Deserializer for class [[BloomFilter]]
   *
   * This class is thread safe.
   */
  class BloomFilterSerDe {

    final def serialize(obj: BloomFilter): Array[Byte] = {
      val size = obj.bitSize()/8
      require(size <= Integer.MAX_VALUE, s"actual number of bits is too large $size")
      val out = new ByteArrayOutputStream(size.intValue())
      obj.writeTo(out)
      out.close()
      out.toByteArray
    }

    final def deserialize(bytes: Array[Byte]): BloomFilter = {
      val in = new ByteArrayInputStream(bytes)
      val bloomFilter = BloomFilter.readFrom(in)
      in.close()
      bloomFilter
    }
  }

  val serde: BloomFilterSerDe = new BloomFilterSerDe
}
