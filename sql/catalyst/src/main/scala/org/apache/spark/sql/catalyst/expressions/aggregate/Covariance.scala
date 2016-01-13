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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

/**
 * Compute the covariance between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 */
abstract class Covariance(left: Expression, right: Expression) extends ImperativeAggregate
    with Serializable {
  override def children: Seq[Expression] = Seq(left, right)

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType.isInstanceOf[DoubleType] && right.dataType.isInstanceOf[DoubleType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"covariance requires that both arguments are double type, " +
          s"not (${left.dataType}, ${right.dataType}).")
    }
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map(_.newInstance())
  }

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(
    AttributeReference("xAvg", DoubleType)(),
    AttributeReference("yAvg", DoubleType)(),
    AttributeReference("Ck", DoubleType)(),
    AttributeReference("count", LongType)())

  // Local cache of mutableAggBufferOffset(s) that will be used in update and merge
  val xAvgOffset = mutableAggBufferOffset
  val yAvgOffset = mutableAggBufferOffset + 1
  val CkOffset = mutableAggBufferOffset + 2
  val countOffset = mutableAggBufferOffset + 3

  // Local cache of inputAggBufferOffset(s) that will be used in update and merge
  val inputXAvgOffset = inputAggBufferOffset
  val inputYAvgOffset = inputAggBufferOffset + 1
  val inputCkOffset = inputAggBufferOffset + 2
  val inputCountOffset = inputAggBufferOffset + 3

  override def initialize(buffer: MutableRow): Unit = {
    buffer.setDouble(xAvgOffset, 0.0)
    buffer.setDouble(yAvgOffset, 0.0)
    buffer.setDouble(CkOffset, 0.0)
    buffer.setLong(countOffset, 0L)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val leftEval = left.eval(input)
    val rightEval = right.eval(input)

    if (leftEval != null && rightEval != null) {
      val x = leftEval.asInstanceOf[Double]
      val y = rightEval.asInstanceOf[Double]

      var xAvg = buffer.getDouble(xAvgOffset)
      var yAvg = buffer.getDouble(yAvgOffset)
      var Ck = buffer.getDouble(CkOffset)
      var count = buffer.getLong(countOffset)

      val deltaX = x - xAvg
      val deltaY = y - yAvg
      count += 1
      xAvg += deltaX / count
      yAvg += deltaY / count
      Ck += deltaX * (y - yAvg)

      buffer.setDouble(xAvgOffset, xAvg)
      buffer.setDouble(yAvgOffset, yAvg)
      buffer.setDouble(CkOffset, Ck)
      buffer.setLong(countOffset, count)
    }
  }

  // Merge counters from other partitions. Formula can be found at:
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val count2 = buffer2.getLong(inputCountOffset)

    // We only go to merge two buffers if there is at least one record aggregated in buffer2.
    // We don't need to check count in buffer1 because if count2 is more than zero, totalCount
    // is more than zero too, then we won't get a divide by zero exception.
    if (count2 > 0) {
      var xAvg = buffer1.getDouble(xAvgOffset)
      var yAvg = buffer1.getDouble(yAvgOffset)
      var Ck = buffer1.getDouble(CkOffset)
      var count = buffer1.getLong(countOffset)

      val xAvg2 = buffer2.getDouble(inputXAvgOffset)
      val yAvg2 = buffer2.getDouble(inputYAvgOffset)
      val Ck2 = buffer2.getDouble(inputCkOffset)

      val totalCount = count + count2
      val deltaX = xAvg - xAvg2
      val deltaY = yAvg - yAvg2
      Ck += Ck2 + deltaX * deltaY * count / totalCount * count2
      xAvg = (xAvg * count + xAvg2 * count2) / totalCount
      yAvg = (yAvg * count + yAvg2 * count2) / totalCount
      count = totalCount

      buffer1.setDouble(xAvgOffset, xAvg)
      buffer1.setDouble(yAvgOffset, yAvg)
      buffer1.setDouble(CkOffset, Ck)
      buffer1.setLong(countOffset, count)
    }
  }
}

case class CovSample(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Covariance(left, right) {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def eval(buffer: InternalRow): Any = {
    val count = buffer.getLong(countOffset)
    if (count > 1) {
      val Ck = buffer.getDouble(CkOffset)
      val cov = Ck / (count - 1)
      if (cov.isNaN) {
        null
      } else {
        cov
      }
    } else {
      null
    }
  }
}

case class CovPopulation(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Covariance(left, right) {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def eval(buffer: InternalRow): Any = {
    val count = buffer.getLong(countOffset)
    if (count > 0) {
      val Ck = buffer.getDouble(CkOffset)
      if (Ck.isNaN) {
        null
      } else {
        Ck / count
      }
    } else {
      null
    }
  }
}
