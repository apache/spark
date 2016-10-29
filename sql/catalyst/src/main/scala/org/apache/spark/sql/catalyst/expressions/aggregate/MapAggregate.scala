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

import com.google.common.primitives.{Doubles, Ints, Longs}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The MapAggregate function for a column returns bins - (distinct value, frequency) pairs
 * of equi-width histogram when the number of distinct values is less than or equal to the
 * specified maximum number of bins. Otherwise, it returns an empty map.
 *
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param numBinsExpression The maximum number of bins.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(col, numBins) - Returns bins - (distinct value, frequency) pairs of equi-width
      histogram when the number of distinct values is less than or equal to the specified
      maximum number of bins. Otherwise, it returns an empty map.
    """)
case class MapAggregate(
    child: Expression,
    numBinsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int) extends TypedImperativeAggregate[MapDigest] {

  def this(child: Expression, numBinsExpression: Expression) = {
    this(child, numBinsExpression, 0, 0)
  }

  // Mark as lazy so that numBinsExpression is not evaluated during tree transformation.
  private lazy val numBins: Int = numBinsExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(NumericType, TimestampType, DateType, StringType), IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!numBinsExpression.foldable) {
      TypeCheckFailure("The maximum number of bins provided must be a constant literal")
    } else if (numBins < 2) {
      TypeCheckFailure(
        "The maximum number of bins provided must be a positive integer literal >= 2 " +
          s"(current value = $numBins)")
    } else {
      TypeCheckSuccess
    }
  }

  override def update(buffer: MapDigest, input: InternalRow): Unit = {
    if (buffer.invalid) {
      return
    }
    val evaluated = child.eval(input)
    if (evaluated != null) {
      buffer.update(child.dataType, evaluated, numBins)
    }
  }

  override def merge(buffer: MapDigest, other: MapDigest): Unit = {
    if (buffer.invalid) return
    if (other.invalid) {
      buffer.invalid = true
      buffer.clear()
      return
    }
    buffer.merge(other, numBins)
  }

  override def eval(buffer: MapDigest): Any = {
    if (buffer.invalid) {
      // return empty map
      ArrayBasedMapData(Map.empty)
    } else {
      // sort the result to make it more readable
      val sorted = buffer match {
        case stringDigest: StringMapDigest => TreeMap[UTF8String, Long](stringDigest.bins.toSeq: _*)
        case numericDigest: NumericMapDigest => TreeMap[Double, Long](numericDigest.bins.toSeq: _*)
      }
      ArrayBasedMapData(sorted.keys.toArray, sorted.values.toArray)
    }
  }

  override def serialize(buffer: MapDigest): Array[Byte] = {
    buffer match {
      case stringDigest: StringMapDigest => StringMapDigest.serialize(stringDigest)
      case numericDigest: NumericMapDigest => NumericMapDigest.serialize(numericDigest)
    }
  }

  override def deserialize(bytes: Array[Byte]): MapDigest = {
    child.dataType match {
      case StringType => StringMapDigest.deserialize(bytes)
      case _ => NumericMapDigest.deserialize(bytes)
    }
  }

  override def createAggregationBuffer(): MapDigest = {
    child.dataType match {
      case StringType => StringMapDigest()
      case _ => NumericMapDigest()
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MapAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MapAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = {
    child.dataType match {
      case StringType => MapType(StringType, LongType)
      case _ => MapType(DoubleType, LongType)
    }
  }

  override def children: Seq[Expression] = Seq(child, numBinsExpression)

  override def prettyName: String = "map_aggregate"
}

trait MapDigest {
  // Mark this MapDigest invalid when the size of the hashmap (ndv of the column) exceeds numBins
  var invalid: Boolean = false

  def update(dataType: DataType, value: Any, numBins: Int): Unit
  def merge(otherDigest: MapDigest, numBins: Int): Unit
  def clear(): Unit

  // Update baseMap and clear it when its size exceeds numBins.
  def updateMap[T](baseMap: mutable.HashMap[T, Long], value: T, numBins: Int): Unit = {
    mergeMap(baseMap, mutable.HashMap(value -> 1L), numBins)
  }

  // Merge two maps and clear baseMap when its size exceeds numBins.
  def mergeMap[T](
      baseMap: mutable.HashMap[T, Long],
      otherMap: mutable.HashMap[T, Long],
      numBins: Int): Unit = {
    otherMap.foreach { case (key, value) =>
      if (baseMap.contains(key)) {
        baseMap.update(key, baseMap(key) + value)
      } else {
        if (baseMap.size >= numBins) {
          invalid = true
          baseMap.clear()
          return
        } else {
          baseMap.put(key, value)
        }
      }
    }
  }
}

// Digest class for column of string type.
case class StringMapDigest(
    bins: mutable.HashMap[UTF8String, Long] = mutable.HashMap.empty) extends MapDigest {
  def this(bins: mutable.HashMap[UTF8String, Long], invalid: Boolean) = {
    this(bins)
    this.invalid = invalid
  }

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    updateMap(bins, value.asInstanceOf[UTF8String], numBins)
  }

  override def merge(otherDigest: MapDigest, numBins: Int): Unit = {
    mergeMap(baseMap = bins, otherMap = otherDigest.asInstanceOf[StringMapDigest].bins, numBins)
  }

  override def clear(): Unit = bins.clear()
}

object StringMapDigest {

  private final def length(obj: StringMapDigest): Int = {
    // invalid, size of bins
    var len: Int = Ints.BYTES + Ints.BYTES
    obj.bins.foreach { case (key, value) =>
      // length of key, key, value
      len += Ints.BYTES + key.getBytes.length + Longs.BYTES
    }
    len
  }

  final def serialize(obj: StringMapDigest): Array[Byte] = {
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

  final def deserialize(bytes: Array[Byte]): StringMapDigest = {
    val buffer = ByteBuffer.wrap(bytes)
    val invalid = if (buffer.getInt == 0) true else false
    val size = buffer.getInt
    val bins = new mutable.HashMap[UTF8String, Long]
    var i = 0
    while (i < size) {
      val keyLength = buffer.getInt
      var j = 0
      val keyBytes = new Array[Byte](keyLength)
      while (j < keyLength) {
        keyBytes(j) = buffer.get()
        j += 1
      }
      val value = buffer.getLong
      bins.put(UTF8String.fromBytes(keyBytes), value)
      i += 1
    }
    new StringMapDigest(bins, invalid)
  }
}

// Digest class for column of numeric type.
case class NumericMapDigest(
    bins: mutable.HashMap[Double, Long] = mutable.HashMap.empty) extends MapDigest {
  def this(bins: mutable.HashMap[Double, Long], invalid: Boolean) = {
    this(bins)
    this.invalid = invalid
  }

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    // Use Double to represent endpoints (in histograms) for simplicity
    val doubleValue = dataType match {
      case n: NumericType =>
        n.numeric.toDouble(value.asInstanceOf[n.InternalType])
      case d: DateType =>
        value.asInstanceOf[Int].toDouble
      case t: TimestampType =>
        value.asInstanceOf[Long].toDouble
    }
    updateMap(bins, doubleValue, numBins)
  }

  override def merge(otherDigest: MapDigest, numBins: Int): Unit = {
    mergeMap(baseMap = bins, otherMap = otherDigest.asInstanceOf[NumericMapDigest].bins, numBins)
  }

  override def clear(): Unit = bins.clear()
}

object NumericMapDigest {

  private final def length(obj: NumericMapDigest): Int = {
    // invalid, size of bins
    var len: Int = Ints.BYTES + Ints.BYTES
    obj.bins.foreach { case (key, value) =>
      // key, value
      len += Doubles.BYTES + Longs.BYTES
    }
    len
  }

  final def serialize(obj: NumericMapDigest): Array[Byte] = {
    val buffer = ByteBuffer.wrap(new Array(length(obj)))
    buffer.putInt(if (obj.invalid) 0 else 1)
    buffer.putInt(obj.bins.size)
    obj.bins.foreach { case (key, value) =>
      buffer.putDouble(key)
      buffer.putLong(value)
    }
    buffer.array()
  }

  final def deserialize(bytes: Array[Byte]): NumericMapDigest = {
    val buffer = ByteBuffer.wrap(bytes)
    val invalid = if (buffer.getInt == 0) true else false
    val size = buffer.getInt
    val bins = new mutable.HashMap[Double, Long]
    var i = 0
    while (i < size) {
      val key = buffer.getDouble
      val value = buffer.getLong
      bins.put(key, value)
      i += 1
    }
    new NumericMapDigest(bins, invalid)
  }
}
