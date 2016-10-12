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

import java.nio.ByteBuffer

import scala.collection.immutable.TreeMap
import scala.collection.mutable

import com.google.common.primitives.{Ints, Longs}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.aggregate.StringHistogram.StringHistogramInfo
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * The StringHistogram function returns the histogram bins - (distinct value, frequency) pairs
 * for a string column.
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param numBinsExpression The maximum number of bins. When the number of distinct values is
 *                          larger than this, the function will return an empty result.
 */
@ExpressionDescription(
  usage = "_FUNC_(col, numBins) - Returns histogram bins with the maximum number of bins allowed.")
case class StringHistogram(
    child: Expression,
    numBinsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int) extends TypedImperativeAggregate[StringHistogramInfo] {

  def this(child: Expression, numBinsExpression: Expression) = {
    this(child, numBinsExpression, 0, 0)
  }

  // Mark as lazy so that numBinsExpression is not evaluated during tree transformation.
  private lazy val numBins: Int = numBinsExpression.eval().asInstanceOf[Int]

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!numBinsExpression.foldable) {
      TypeCheckFailure(s"The maximum number of bins provided must be a constant literal")
    } else if (numBins <= 0) {
      TypeCheckFailure(
        s"The maximum number of bins provided must be a positive integer literal (current value" +
          s" = $numBins)")
    } else {
      TypeCheckSuccess
    }
  }

  override def update(buffer: StringHistogramInfo, input: InternalRow): Unit = {
    if (buffer.invalid) {
      return
    }
    val evaluated = child.eval(input)
    if (evaluated != null) {
      val str = evaluated.asInstanceOf[UTF8String]
      if (buffer.bins.contains(str)) {
        buffer.bins.update(str, buffer.bins(str) + 1)
      } else {
        if (buffer.bins.size >= numBins) {
          // clear the buffer and mark it as invalid
          buffer.bins.clear()
          buffer.invalid = true
        } else {
          buffer.bins.put(str, 1)
        }
      }
    }
  }

  override def merge(buffer: StringHistogramInfo, other: StringHistogramInfo): Unit = {
    if (buffer.invalid || other.invalid) {
      buffer.bins.clear()
      return
    }
    other.bins.foreach { case (key, value) =>
      if (buffer.bins.contains(key)) {
        buffer.bins.update(key, buffer.bins(key) + value)
      } else {
        if (buffer.bins.size >= numBins) {
          // clear the buffer and mark it as invalid
          buffer.bins.clear()
          buffer.invalid = true
          return
        } else {
          buffer.bins.put(key, value)
        }
      }
    }
  }

  override def eval(buffer: StringHistogramInfo): Any = {
    val sorted = TreeMap[UTF8String, Long](buffer.bins.toSeq: _*)
    ArrayBasedMapData(sorted.keys.toArray, sorted.values.toArray)
  }

  override def serialize(buffer: StringHistogramInfo): Array[Byte] = {
    StringHistogram.serializer.serialize(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): StringHistogramInfo = {
    StringHistogram.serializer.deserialize(storageFormat)
  }

  override def createAggregationBuffer(): StringHistogramInfo = new StringHistogramInfo()

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): StringHistogram = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): StringHistogram = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)

  override def nullable: Boolean = false

  override def dataType: DataType = MapType(StringType, LongType)

  override def children: Seq[Expression] = Seq(child, numBinsExpression)

  override def prettyName: String = "string_histogram"
}

object StringHistogram {

  /**
   * StringHistogramInfo maintains frequency of each distinct value.
   * @param invalid This is to mark the HashMap as invalid when its size (ndv of the column)
   *                exceeds numBins, because we won't generate string histograms in that case.
   * @param bins A HashMap to maintain frequency of each distinct value.
   */
  case class StringHistogramInfo(
      var invalid: Boolean = false,
      bins: mutable.HashMap[UTF8String, Long] = mutable.HashMap.empty[UTF8String, Long])

  class StringHistogramInfoSerializer {

    private final def length(info: StringHistogramInfo): Int = {
      // invalid, size of bins
      var len: Int = Ints.BYTES + Ints.BYTES
      info.bins.foreach { case (key, value) =>
        // length of key, key, value
        len += Ints.BYTES + key.getBytes.length + Longs.BYTES
      }
      len
    }

    final def serialize(obj: StringHistogramInfo): Array[Byte] = {
      val buffer = ByteBuffer.wrap(new Array(length(obj)))
      buffer.putInt(if (obj.invalid) 0 else 1)
      buffer.putInt(obj.bins.size)
      obj.bins.foreach { case (key, value) =>
        val bytes = key.getBytes
        buffer.putInt(bytes.length)
        buffer.put(bytes)
        buffer.putLong(value)
      }
      buffer.array()
    }

    final def deserialize(bytes: Array[Byte]): StringHistogramInfo = {
      val buffer = ByteBuffer.wrap(bytes)
      val invalid = if (buffer.getInt == 0) true else false
      val size = buffer.getInt
      val bins = new mutable.HashMap[UTF8String, Long]
      var i = 0
      while (i < size) {
        val keyLength = buffer.getInt
        var j = 0
        val bytes = new Array[Byte](keyLength)
        while (j < keyLength) {
          bytes(j) = buffer.get()
          j += 1
        }
        val value = buffer.getLong
        bins.put(UTF8String.fromBytes(bytes), value)
        i += 1
      }
      StringHistogramInfo(invalid, bins)
    }
  }

  val serializer: StringHistogramInfoSerializer = new StringHistogramInfoSerializer
}
