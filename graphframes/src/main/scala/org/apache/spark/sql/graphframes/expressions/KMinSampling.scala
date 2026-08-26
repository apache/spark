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

package org.apache.spark.sql.graphframes.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType
import org.apache.spark.graphframes.GraphFramesUnsupportedVertexTypeException

import scala.annotation.nowarn
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class KMinAccum[T](values: Array[T], weights: Array[Long], var cnt: Int) extends Serializable

case class KMinSampling[T: ClassTag](size: Int)(implicit
    @nowarn tag: TypeTag[T],
    ord: Ordering[T])
    extends Aggregator[Row, KMinAccum[T], Seq[T]]
    with Serializable {

  override def zero: KMinAccum[T] = KMinAccum(Array.ofDim[T](size), Array.ofDim[Long](size), 0)

  override def reduce(b: KMinAccum[T], a: Row): KMinAccum[T] = {
    val newWeight = a.getLong(1)
    val newValue = a.getAs[T](0)
    // fast-path: buffer is already full of "strong" elements
    // the case of "influencer" vertex
    if (b.cnt == size) {
      val lastWeight = b.weights.last
      if ((lastWeight < newWeight) || ((lastWeight == newWeight) && (ord.compare(
          newValue,
          b.values.last) >= 0))) {
        return b
      }
    }

    // slow-path: custom binary search for (Weight, Value)
    // We want to find the first index where (b.w, b.v) > (newWeight, newValue)
    var low = 0
    var high = b.cnt - 1
    var idx = b.cnt // Default insertion point is at the end

    while (low <= high) {
      val mid = (low + high) / 2
      val midWeight = b.weights(mid)

      // Compare (midWeight, midValue) vs (newWeight, newValue)
      val res =
        if (midWeight < newWeight) -1
        else if (midWeight > newWeight) 1
        else ord.compare(b.values(mid), newValue)

      if (res <= 0) {
        // mid is smaller or equal: we must insert after mid
        low = mid + 1
      } else {
        // mid is larger: potential insertion point here
        idx = mid
        high = mid - 1
      }
    }

    if (idx < size) {
      val newCount = math.min(b.cnt + 1, size)
      if (idx < newCount - 1) {
        // shift to the right if needed
        System.arraycopy(b.weights, idx, b.weights, idx + 1, newCount - idx - 1)
        System.arraycopy(b.values, idx, b.values, idx + 1, newCount - idx - 1)
      }

      b.weights(idx) = newWeight
      b.values(idx) = newValue
      b.cnt = newCount
    }

    b
  }

  override def merge(b1: KMinAccum[T], b2: KMinAccum[T]): KMinAccum[T] = {

    if (b1.cnt == 0) {
      return b2
    }

    if (b2.cnt == 0) {
      return b1
    }

    val resultSize = math.min(b1.cnt + b2.cnt, size)
    val newValues = Array.ofDim[T](resultSize)
    val newWeights = Array.ofDim[Long](resultSize)

    var i = 0
    var j = 0
    var r = 0

    while (r < resultSize) {
      val useLeft = if (i >= b1.cnt) {
        false
      } else if (j >= b2.cnt) {
        true
      } else {
        val wLeft = b1.weights(i)
        val wRight = b2.weights(j)

        if (wLeft < wRight) {
          true
        } else if (wLeft > wRight) {
          false
        } else {
          ord.compare(b1.values(i), b2.values(j)) <= 0
        }
      }

      if (useLeft) {
        newWeights(r) = b1.weights(i)
        newValues(r) = b1.values(i)
        i += 1
      } else {
        newWeights(r) = b2.weights(j)
        newValues(r) = b2.values(j)
        j += 1
      }

      r += 1
    }

    KMinAccum(newValues, newWeights, resultSize)
  }

  override def finish(reduction: KMinAccum[T]): Seq[T] =
    reduction.values.slice(0, reduction.cnt).toSeq
  // TODO: replace by Kryo after 4.0.2 is released, see SPARK-52819
  override def bufferEncoder: Encoder[KMinAccum[T]] = Encoders.product
  override def outputEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]()
}

object KMinSampling extends Serializable {
  def getEncoder(spark: SparkSession, dataType: DataType, colNames: Seq[String]): Encoder[Row] = {
    // That is very stupid way actually. But it is the only way with public API
    spark
      .createDataFrame(
        java.util.Collections.emptyList[Row](),
        StructType(
          StructField(colNames(0), dataType) :: StructField(colNames(1), LongType) :: Nil))
      .encoder
  }

  def fromSparkType(dataType: DataType, size: Int, encoder: Encoder[Row]): UserDefinedFunction = {
    dataType match {
      case StringType => udaf(KMinSampling[java.lang.String](size), encoder)
      case ShortType => udaf(KMinSampling[java.lang.Short](size), encoder)
      case ByteType => udaf(KMinSampling[java.lang.Byte](size), encoder)
      case IntegerType => udaf(KMinSampling[java.lang.Integer](size), encoder)
      case LongType => udaf(KMinSampling[java.lang.Long](size), encoder)
      case _ => throw new GraphFramesUnsupportedVertexTypeException("unsupported vertex type")
    }
  }
}
