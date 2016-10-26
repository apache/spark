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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.{PercentileDigest, PercentileDigestSerializer}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, QuantileSummaries}
import org.apache.spark.sql.catalyst.util.QuantileSummaries._
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The HistogramEndpoints function for a column returns bins - (distinct value, frequency) pairs
 * of equi-width histogram when the number of distinct values is less than or equal to the
 * specified maximum number of bins. Otherwise, for column of string type, it returns an empty
 * map; for column of numeric type, it returns endpoints of equi-height histogram - approximate
 * percentiles at percentages 1/numBins, 2/numBins, ..., (numBins-1)/numBins.
 *
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param numBinsExpression The maximum number of bins.
 * @param accuracyExpression Accuracy used in computing approximate percentiles.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(col, numBins [, accuracy]) - Returns bins - (distinct value, frequency) pairs
      of equi-width histogram when the number of distinct values is less than or equal to the
      specified maximum number of bins. Otherwise, for column of string type, it returns an empty
      map; for column of numeric type, it returns endpoints of equi-height histogram - approximate
      percentiles at percentages 1/numBins, 2/numBins, ..., (numBins-1)/numBins. The `accuracy`
      parameter (default: 10000) is a positive integer literal which controls percentiles
      approximation accuracy at the cost of memory. Higher value of `accuracy` yields better
      accuracy, `1.0/accuracy` is the relative error of the approximation.
    """)
case class HistogramEndpoints(
    child: Expression,
    numBinsExpression: Expression,
    accuracyExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int) extends TypedImperativeAggregate[EndpointsDigest] {

  def this(child: Expression, numBinsExpression: Expression, accuracyExpression: Expression) = {
    this(child, numBinsExpression, accuracyExpression, 0, 0)
  }

  def this(child: Expression, numBinsExpression: Expression) = {
    this(child, numBinsExpression, Literal(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  }

  // Mark as lazy so that numBinsExpression is not evaluated during tree transformation.
  private lazy val numBins: Int = numBinsExpression.eval().asInstanceOf[Int]

  private lazy val percentages: Array[Double] = {
    val array = new Array[Double](numBins + 1)
    for (i <- 0 to numBins) {
      array(i) = i / numBins.toDouble
    }
    array
  }

  private lazy val accuracy: Int = accuracyExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(NumericType, TimestampType, DateType, StringType), IntegerType, IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!numBinsExpression.foldable || !accuracyExpression.foldable) {
      TypeCheckFailure("The maximum number of bins or accuracy provided must be a constant literal")
    } else if (numBins < 2) {
      TypeCheckFailure(
        "The maximum number of bins provided must be a positive integer literal >= 2 " +
          s"(current value = $numBins)")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer literal (current value = $accuracy)")
    } else {
      TypeCheckSuccess
    }
  }

  override def update(buffer: EndpointsDigest, input: InternalRow): Unit = {
    if (buffer.invalid) {
      return
    }
    val evaluated = child.eval(input)
    if (evaluated != null) {
      buffer.update(child.dataType, evaluated, numBins)
    }
  }

  override def merge(buffer: EndpointsDigest, other: EndpointsDigest): Unit = {
    if (buffer.invalid) return
    if (other.invalid) {
      buffer.invalid = true
      buffer.clear()
      return
    }
    buffer.merge(other, numBins)
  }

  override def eval(buffer: EndpointsDigest): Any = {
    if (buffer.invalid) {
      // return empty map
      ArrayBasedMapData(Map.empty)
    } else {
      buffer match {
        case stringDigest: StringEndpointsDigest =>
          // sort the result to make it more readable
          val sorted = TreeMap[UTF8String, Long](stringDigest.bins.toSeq: _*)
          ArrayBasedMapData(sorted.keys.toArray, sorted.values.toArray)
        case numericDigest: NumericEndpointsDigest =>
          if (!numericDigest.mapInvalid) {
            val sorted = TreeMap[Double, Long](numericDigest.bins.toSeq: _*)
            ArrayBasedMapData(sorted.keys.toArray, sorted.values.toArray)
          } else {
            val percentiles = numericDigest.percentileDigest.getPercentiles(percentages)
            // we only need percentiles, this is for constructing MapData
            val padding = new Array[Long](percentiles.length)
            ArrayBasedMapData(percentiles.toArray, padding.toArray)
          }
      }
    }
  }

  override def serialize(buffer: EndpointsDigest): Array[Byte] = {
    buffer match {
      case stringDigest: StringEndpointsDigest => StringEndpointsDigest.serialize(stringDigest)
      case numericDigest: NumericEndpointsDigest => NumericEndpointsDigest.serialize(numericDigest)
    }
  }

  override def deserialize(bytes: Array[Byte]): EndpointsDigest = {
    child.dataType match {
      case StringType => StringEndpointsDigest.deserialize(bytes)
      case _ => NumericEndpointsDigest.deserialize(bytes)
    }
  }

  override def createAggregationBuffer(): EndpointsDigest = {
    child.dataType match {
      case StringType =>
        StringEndpointsDigest()
      case _ =>
        NumericEndpointsDigest(new PercentileDigest(1.0D / accuracy))
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): HistogramEndpoints = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): HistogramEndpoints = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = {
    child.dataType match {
      case StringType => MapType(StringType, LongType)
      case _ => MapType(DoubleType, LongType)
    }
  }

  override def children: Seq[Expression] = Seq(child, numBinsExpression, accuracyExpression)

  override def prettyName: String = "histogram_endpoints"
}

trait EndpointsDigest {

  // Mark this EndpointsDigest as invalid when:
  // 1. for string type - the size of the hashmap (ndv of the column) exceeds numBins;
  // 2. for numeric type - Some Decimal value out of the range of Double occurs.
  var invalid: Boolean = false

  def update(dataType: DataType, value: Any, numBins: Int): Unit
  def merge(otherDigest: EndpointsDigest, numBins: Int): Unit
  def clear(): Unit

  // Updates baseMap, and returns success or not.
  def updateMap[T](baseMap: mutable.HashMap[T, Long], value: T, numBins: Int): Boolean = {
    if (baseMap.contains(value)) {
      baseMap.update(value, baseMap(value) + 1)
    } else {
      if (baseMap.size >= numBins) {
        return false
      } else {
        baseMap.put(value, 1)
      }
    }
    true
  }

  // Merges two Maps into baseMap, and returns success or not.
  def mergeMaps[T](
      baseMap: mutable.HashMap[T, Long],
      otherMap: mutable.HashMap[T, Long],
      numBins: Int): Boolean = {
    otherMap.foreach { case (key, value) =>
      if (baseMap.contains(key)) {
        baseMap.update(key, baseMap(key) + value)
      } else {
        if (baseMap.size >= numBins) {
          return false
        } else {
          baseMap.put(key, value)
        }
      }
    }
    true
  }
}

/**
 * Digest class for column of string type.
 * @param bins A HashMap to maintain frequency of each distinct value.
 */
case class StringEndpointsDigest(
    bins: mutable.HashMap[UTF8String, Long] = mutable.HashMap.empty[UTF8String, Long])
  extends EndpointsDigest {

  def this(bins: mutable.HashMap[UTF8String, Long], invalid: Boolean) = {
    this(bins)
    this.invalid = invalid
  }

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    val success = updateMap(baseMap = bins, value.asInstanceOf[UTF8String], numBins)
    if (!success) {
      // clear the map and mark this digest as invalid
      bins.clear()
      invalid = true
    }
  }

  override def merge(otherDigest: EndpointsDigest, numBins: Int): Unit = {
    val other = otherDigest.asInstanceOf[StringEndpointsDigest]
    val success = mergeMaps(baseMap = bins, otherMap = other.bins, numBins)
    if (!success) {
      // clear the map and mark this digest as invalid
      bins.clear()
      invalid = true
    }
  }

  override def clear(): Unit = {
    bins.clear()
  }
}

object StringEndpointsDigest {

  private final def length(obj: StringEndpointsDigest): Int = {
    // invalid, size of bins
    var len: Int = Ints.BYTES + Ints.BYTES
    obj.bins.foreach { case (key, value) =>
      // length of key, key, value
      len += Ints.BYTES + key.getBytes.length + Longs.BYTES
    }
    len
  }

  final def serialize(obj: StringEndpointsDigest): Array[Byte] = {
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

  final def deserialize(bytes: Array[Byte]): StringEndpointsDigest = {
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
    new StringEndpointsDigest(bins, invalid)
  }
}

/**
 * Digest class for column of numeric type.
 * @param bins A HashMap to maintain frequency of each distinct value.
 * @param percentileDigest A helper class to compute approximate percentiles.
 */
case class NumericEndpointsDigest(
    percentileDigest: PercentileDigest,
    bins: mutable.HashMap[Double, Long] = mutable.HashMap.empty[Double, Long])
  extends EndpointsDigest {

  var mapInvalid = false

  def this(
      percentileDigest: PercentileDigest,
      bins: mutable.HashMap[Double, Long],
      invalid: Boolean,
      mapInvalid: Boolean) = {
    this(percentileDigest, bins)
    this.invalid = invalid
    this.mapInvalid = mapInvalid
  }

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    if (dataType.isInstanceOf[DecimalType]) {
      val decimal = value.asInstanceOf[Decimal]
      val double = decimal.toDouble
      if (double == Double.PositiveInfinity || double == Double.NegativeInfinity) {
        // This value has too great a magnitude to represent as a Double.
        invalid = true
        clear()
        return
      }
    }

    // We use Double to represent endpoints (in histograms) for simplicity.
    // Loss of precision is acceptable because we are computing approximate answers anyway.
    val doubleValue = dataType match {
      case n: NumericType =>
        n.numeric.toDouble(value.asInstanceOf[n.InternalType])
      case d: DateType =>
        value.asInstanceOf[Int].toDouble
      case t: TimestampType =>
        value.asInstanceOf[Long].toDouble
    }
    // update percentileDigest
    percentileDigest.add(doubleValue)
    if (!mapInvalid) {
      // update hashmap
      val success = updateMap(baseMap = bins, doubleValue, numBins)
      if (!success) {
        // clear the map and mark it as invalid
        bins.clear()
        mapInvalid = true
      }
    }
  }

  override def merge(otherDigest: EndpointsDigest, numBins: Int): Unit = {
    val other = otherDigest.asInstanceOf[NumericEndpointsDigest]
    // merge percentileDigest
    percentileDigest.merge(other.percentileDigest)
    if (other.mapInvalid) {
      bins.clear()
      mapInvalid = true
    }
    if (!mapInvalid) {
      // merge hashmap
      val success = mergeMaps(baseMap = bins, otherMap = other.bins, numBins)
      if (!success) {
        // clear the map and mark it as invalid
        bins.clear()
        mapInvalid = true
      }
    }
  }

  override def clear(): Unit = {
    bins.clear()
    // reset percentileDigest
    percentileDigest.summaries =
      new QuantileSummaries(defaultCompressThreshold, percentileDigest.summaries.relativeError)
    percentileDigest.isCompressed = true
  }
}

object NumericEndpointsDigest {

  val percentileDigestSerializer: PercentileDigestSerializer = new PercentileDigestSerializer

  final def serialize(obj: NumericEndpointsDigest): Array[Byte] = {
    // invalid, mapInvalid, size of bins
    var length: Int = Ints.BYTES + Ints.BYTES + Ints.BYTES
    obj.bins.foreach { case (key, value) =>
      // key, value
      length += Doubles.BYTES + Longs.BYTES
    }
    val summaries = obj.percentileDigest.quantileSummaries
    val summaryLength = percentileDigestSerializer.length(summaries)
    // length of PercentileDigest, PercentileDigest
    length += Ints.BYTES + summaryLength

    val buffer = ByteBuffer.wrap(new Array(length))
    buffer.putInt(if (obj.invalid) 0 else 1)
    buffer.putInt(if (obj.mapInvalid) 0 else 1)
    buffer.putInt(obj.bins.size)
    obj.bins.foreach { case (key, value) =>
      buffer.putDouble(key)
      buffer.putLong(value)
    }
    buffer.putInt(summaryLength)
    buffer.put(percentileDigestSerializer.serializeSummaries(summaries))
    buffer.array()
  }

  final def deserialize(bytes: Array[Byte]): NumericEndpointsDigest = {
    val buffer = ByteBuffer.wrap(bytes)
    val invalid = if (buffer.getInt == 0) true else false
    val mapInvalid = if (buffer.getInt == 0) true else false
    val size = buffer.getInt
    val bins = new mutable.HashMap[Double, Long]
    var i = 0
    while (i < size) {
      val key = buffer.getDouble
      val value = buffer.getLong
      bins.put(key, value)
      i += 1
    }
    val percentileDigestLength = buffer.getInt
    var j = 0
    val percentileDigestBytes = new Array[Byte](percentileDigestLength)
    while (j < percentileDigestLength) {
      percentileDigestBytes(j) = buffer.get()
      j += 1
    }
    val percentileDigest = percentileDigestSerializer.deserialize(percentileDigestBytes)
    new NumericEndpointsDigest(percentileDigest, bins, invalid = invalid, mapInvalid = mapInvalid)
  }
}
