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

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, HyperLogLogPlusPlusHelper}
import org.apache.spark.sql.types._

/**
 * This function counts the approximate number of distinct values (ndv) in
 * intervals constructed from endpoints specified in `endpointsExpression`. The endpoints should be
 * sorted into ascending order. E.g., given an array of endpoints
 * (endpoint_1, endpoint_2, ... endpoint_N), returns the approximate ndv's for intervals
 * [endpoint_1, endpoint_2], (endpoint_2, endpoint_3], ... (endpoint_N-1, endpoint_N].
 * To count ndv's in these intervals, apply the HyperLogLogPlusPlus algorithm in each of them.
 * @param child to estimate the ndv's of.
 * @param endpointsExpression An array expression to construct the intervals. It must be foldable,
 *                            and its elements should be sorted into ascending order.
 *                            Duplicate endpoints are allowed, e.g. (1, 5, 5, 10), and ndv for
 *                            interval (5, 5] would be 1.
 * @param relativeSD The maximum estimation error allowed in the HyperLogLogPlusPlus algorithm.
 */
case class ApproxCountDistinctForIntervals(
    child: Expression,
    endpointsExpression: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate with ExpectsInputTypes {

  def this(child: Expression, endpointsExpression: Expression) = {
    this(
      child = child,
      endpointsExpression = endpointsExpression,
      relativeSD = 0.05,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  def this(child: Expression, endpointsExpression: Expression, relativeSD: Expression) = {
    this(
      child = child,
      endpointsExpression = endpointsExpression,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(NumericType, TimestampType, DateType), ArrayType)
  }

  // Mark as lazy so that endpointsExpression is not evaluated during tree transformation.
  lazy val endpoints: Array[Double] =
    (endpointsExpression.dataType, endpointsExpression.eval()) match {
      case (ArrayType(elementType, _), arrayData: ArrayData) =>
        arrayData.toObjectArray(elementType).map(_.toString.toDouble)
    }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!endpointsExpression.foldable) {
      TypeCheckFailure("The endpoints provided must be constant literals")
    } else {
      endpointsExpression.dataType match {
        case ArrayType(_: NumericType | DateType | TimestampType, _) =>
          if (endpoints.length < 2) {
            TypeCheckFailure("The number of endpoints must be >= 2 to construct intervals")
          } else {
            TypeCheckSuccess
          }
        case _ =>
          TypeCheckFailure("Endpoints require (numeric or timestamp or date) type")
      }
    }
  }

  // N endpoints construct N-1 intervals, creating a HLLPP for each interval
  private lazy val hllppArray = {
    val array = new Array[HyperLogLogPlusPlusHelper](endpoints.length - 1)
    for (i <- array.indices) {
      array(i) = new HyperLogLogPlusPlusHelper(relativeSD)
    }
    // `numWords` in each HLLPPHelper should be the same because it is determined by `relativeSD`
    // which is shared among all HLLPPHelpers.
    assert(array.map(_.numWords).distinct.length == 1)
    array
  }

  private lazy val numWordsPerHllpp = hllppArray.head.numWords

  private lazy val totalNumWords = numWordsPerHllpp * hllppArray.length

  /** Allocate enough words to store all registers. */
  override lazy val aggBufferAttributes: Seq[AttributeReference] = {
    Seq.tabulate(totalNumWords) { i =>
      AttributeReference(s"MS[$i]", LongType)()
    }
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  /** Fill all words with zeros. */
  override def initialize(buffer: InternalRow): Unit = {
    var word = 0
    while (word < totalNumWords) {
      buffer.setLong(mutableAggBufferOffset + word, 0)
      word += 1
    }
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      // convert the value into a double value for searching in the double array
      val doubleValue = child.dataType match {
        case n: NumericType =>
          n.numeric.toDouble(value.asInstanceOf[n.InternalType])
        case _: DateType =>
          value.asInstanceOf[Int].toDouble
        case _: TimestampType =>
          value.asInstanceOf[Long].toDouble
      }

      // endpoints are sorted into ascending order already
      if (endpoints.head > doubleValue || endpoints.last < doubleValue) {
        // ignore if the value is out of the whole range
        return
      }

      val hllppIndex = findHllppIndex(doubleValue)
      val offset = mutableAggBufferOffset + hllppIndex * numWordsPerHllpp
      hllppArray(hllppIndex).update(buffer, offset, value, child.dataType)
    }
  }

  // Find which interval (HyperLogLogPlusPlusHelper) should receive the given value.
  def findHllppIndex(value: Double): Int = {
    var index = util.Arrays.binarySearch(endpoints, value)
    if (index >= 0) {
      // The value is found.
      if (index == 0) {
        0
      } else {
        // If the endpoints contains multiple elements with the specified value, there is no
        // guarantee which one binarySearch will return. We remove this uncertainty by moving the
        // index to the first position of these elements.
        var first = index - 1
        while (first >= 0 && endpoints(first) == value) {
          first -= 1
        }
        index = first + 1

        if (index == 0) {
          // reach the first endpoint
          0
        } else {
          // send values in (endpoints(index-1), endpoints(index)] to hllpps(index-1)
          index - 1
        }
      }
    } else {
      // The value is not found, binarySearch returns (-(<i>insertion point</i>) - 1).
      // The <i>insertion point</i> is defined as the point at which the key would be inserted
      // into the array: the index of the first element greater than the key.
      val insertionPoint = - (index + 1)
      if (insertionPoint == 0) 0 else insertionPoint - 1
    }
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    for (i <- hllppArray.indices) {
      hllppArray(i).merge(
        buffer1 = buffer1,
        buffer2 = buffer2,
        offset1 = mutableAggBufferOffset + i * numWordsPerHllpp,
        offset2 = inputAggBufferOffset + i * numWordsPerHllpp)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val ndvArray = hllppResults(buffer)
    // If the endpoints contains multiple elements with the same value,
    // we set ndv=1 for intervals between these elements.
    // E.g. given four endpoints (1, 2, 2, 4) and input sequence (0.5, 2),
    // the ndv's for the three intervals should be (2, 1, 0)
    for (i <- ndvArray.indices) {
      if (endpoints(i) == endpoints(i + 1)) ndvArray(i) = 1
    }
    new GenericArrayData(ndvArray)
  }

  def hllppResults(buffer: InternalRow): Array[Long] = {
    val ndvArray = new Array[Long](hllppArray.length)
    for (i <- ndvArray.indices) {
      ndvArray(i) = hllppArray(i).query(buffer, mutableAggBufferOffset + i * numWordsPerHllpp)
    }
    ndvArray
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child, endpointsExpression)

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(LongType)

  override def prettyName: String = "approx_count_distinct_for_intervals"
}
