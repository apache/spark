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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, HyperLogLogPlusPlusHelper}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

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
 * @param relativeSD The maximum relative standard deviation allowed
 *                   in the HyperLogLogPlusPlus algorithm.
 */
case class ApproxCountDistinctForIntervals(
    child: Expression,
    endpointsExpression: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Array[Long]]
  with ExpectsInputTypes
  with BinaryLike[Expression]
  with QueryErrorsBase {

  def this(child: Expression, endpointsExpression: Expression, relativeSD: Expression) = {
    this(
      child = child,
      endpointsExpression = endpointsExpression,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(NumericType, TimestampType, DateType, TimestampNTZType,
      YearMonthIntervalType, DayTimeIntervalType), ArrayType)
  }

  // Mark as lazy so that endpointsExpression is not evaluated during tree transformation.
  lazy val endpoints: Array[Double] = {
    val endpointsType = endpointsExpression.dataType.asInstanceOf[ArrayType]
    val endpoints = endpointsExpression.eval().asInstanceOf[ArrayData]
    endpoints.toObjectArray(endpointsType.elementType).map(_.toString.toDouble)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!endpointsExpression.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "endpointsExpression",
          "inputType" -> toSQLType(endpointsExpression.dataType)))
    } else {
      endpointsExpression.dataType match {
        case ArrayType(_: NumericType | DateType | TimestampType | TimestampNTZType |
           _: AnsiIntervalType, _) =>
          if (endpoints.length < 2) {
            DataTypeMismatch(
              errorSubClass = "WRONG_NUM_ENDPOINTS",
              messageParameters = Map("actualNumber" -> endpoints.length.toString))
          } else {
            TypeCheckSuccess
          }
        case inputType =>
          val requiredElemTypes = toSQLType(TypeCollection(
            NumericType, DateType, TimestampType, TimestampNTZType, AnsiIntervalType))
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> "2",
              "requiredType" -> s"ARRAY OF $requiredElemTypes",
              "inputSql" -> toSQLExpr(endpointsExpression),
              "inputType" -> toSQLType(inputType)))
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
  override def createAggregationBuffer(): Array[Long] = {
    Array.fill(totalNumWords)(0L)
  }

  override def update(buffer: Array[Long], input: InternalRow): Array[Long] = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      // convert the value into a double value for searching in the double array
      val doubleValue = child.dataType match {
        case n: NumericType =>
          n.numeric.toDouble(value.asInstanceOf[n.InternalType])
        case _: DateType | _: YearMonthIntervalType =>
          value.asInstanceOf[Int].toDouble
        case TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
          value.asInstanceOf[Long].toDouble
      }

      // endpoints are sorted into ascending order already
      if (endpoints.head > doubleValue || endpoints.last < doubleValue) {
        // ignore if the value is out of the whole range
        return buffer
      }

      val hllppIndex = findHllppIndex(doubleValue)
      val offset = hllppIndex * numWordsPerHllpp
      hllppArray(hllppIndex).update(LongArrayInternalRow(buffer), offset, value, child.dataType)
    }
    buffer
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

  override def merge(buffer1: Array[Long], buffer2: Array[Long]): Array[Long] = {
    for (i <- hllppArray.indices) {
      hllppArray(i).merge(
        buffer1 = LongArrayInternalRow(buffer1),
        buffer2 = LongArrayInternalRow(buffer2),
        offset1 = i * numWordsPerHllpp,
        offset2 = i * numWordsPerHllpp)
    }
    buffer1
  }

  override def eval(buffer: Array[Long]): Any = {
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

  def hllppResults(buffer: Array[Long]): Array[Long] = {
    val ndvArray = new Array[Long](hllppArray.length)
    for (i <- ndvArray.indices) {
      ndvArray(i) = hllppArray(i).query(LongArrayInternalRow(buffer), i * numWordsPerHllpp)
    }
    ndvArray
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
    : ApproxCountDistinctForIntervals = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int)
    : ApproxCountDistinctForIntervals = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def left: Expression = child
  override def right: Expression = endpointsExpression

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(LongType)

  override def prettyName: String = "approx_count_distinct_for_intervals"

  override def serialize(obj: Array[Long]): Array[Byte] = {
    val byteArray = new Array[Byte](obj.length * 8)
    var i = 0
    while (i < obj.length) {
      Platform.putLong(byteArray, Platform.BYTE_ARRAY_OFFSET + i * 8, obj(i))
      i += 1
    }
    byteArray
  }

  override def deserialize(bytes: Array[Byte]): Array[Long] = {
    assert(bytes.length % 8 == 0)
    val length = bytes.length / 8
    val longArray = new Array[Long](length)
    var i = 0
    while (i < length) {
      longArray(i) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + i * 8)
      i += 1
    }
    longArray
  }

  private case class LongArrayInternalRow(array: Array[Long]) extends GenericInternalRow {
    override def getLong(offset: Int): Long = array(offset)
    override def setLong(offset: Int, value: Long): Unit = { array(offset) = value }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ApproxCountDistinctForIntervals =
    copy(child = newLeft, endpointsExpression = newRight)
}
