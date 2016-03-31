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
 * Compute Pearson correlation between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
case class Corr(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate {

  def this(left: Expression, right: Expression) =
    this(left, right, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = Seq(left, right)

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType.isInstanceOf[DoubleType] && right.dataType.isInstanceOf[DoubleType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"corr requires that both arguments are double type, " +
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
    AttributeReference("MkX", DoubleType)(),
    AttributeReference("MkY", DoubleType)(),
    AttributeReference("count", LongType)())

  // Local cache of mutableAggBufferOffset(s) that will be used in update and merge
  private[this] val mutableAggBufferOffsetPlus1 = mutableAggBufferOffset + 1
  private[this] val mutableAggBufferOffsetPlus2 = mutableAggBufferOffset + 2
  private[this] val mutableAggBufferOffsetPlus3 = mutableAggBufferOffset + 3
  private[this] val mutableAggBufferOffsetPlus4 = mutableAggBufferOffset + 4
  private[this] val mutableAggBufferOffsetPlus5 = mutableAggBufferOffset + 5

  // Local cache of inputAggBufferOffset(s) that will be used in update and merge
  private[this] val inputAggBufferOffsetPlus1 = inputAggBufferOffset + 1
  private[this] val inputAggBufferOffsetPlus2 = inputAggBufferOffset + 2
  private[this] val inputAggBufferOffsetPlus3 = inputAggBufferOffset + 3
  private[this] val inputAggBufferOffsetPlus4 = inputAggBufferOffset + 4
  private[this] val inputAggBufferOffsetPlus5 = inputAggBufferOffset + 5

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(buffer: MutableRow): Unit = {
    buffer.setDouble(mutableAggBufferOffset, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus1, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus2, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus3, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus4, 0.0)
    buffer.setLong(mutableAggBufferOffsetPlus5, 0L)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val leftEval = left.eval(input)
    val rightEval = right.eval(input)

    if (leftEval != null && rightEval != null) {
      val x = leftEval.asInstanceOf[Double]
      val y = rightEval.asInstanceOf[Double]

      var xAvg = buffer.getDouble(mutableAggBufferOffset)
      var yAvg = buffer.getDouble(mutableAggBufferOffsetPlus1)
      var Ck = buffer.getDouble(mutableAggBufferOffsetPlus2)
      var MkX = buffer.getDouble(mutableAggBufferOffsetPlus3)
      var MkY = buffer.getDouble(mutableAggBufferOffsetPlus4)
      var count = buffer.getLong(mutableAggBufferOffsetPlus5)

      val deltaX = x - xAvg
      val deltaY = y - yAvg
      count += 1
      xAvg += deltaX / count
      yAvg += deltaY / count
      Ck += deltaX * (y - yAvg)
      MkX += deltaX * (x - xAvg)
      MkY += deltaY * (y - yAvg)

      buffer.setDouble(mutableAggBufferOffset, xAvg)
      buffer.setDouble(mutableAggBufferOffsetPlus1, yAvg)
      buffer.setDouble(mutableAggBufferOffsetPlus2, Ck)
      buffer.setDouble(mutableAggBufferOffsetPlus3, MkX)
      buffer.setDouble(mutableAggBufferOffsetPlus4, MkY)
      buffer.setLong(mutableAggBufferOffsetPlus5, count)
    }
  }

  // Merge counters from other partitions. Formula can be found at:
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val count2 = buffer2.getLong(inputAggBufferOffsetPlus5)

    // We only go to merge two buffers if there is at least one record aggregated in buffer2.
    // We don't need to check count in buffer1 because if count2 is more than zero, totalCount
    // is more than zero too, then we won't get a divide by zero exception.
    if (count2 > 0) {
      var xAvg = buffer1.getDouble(mutableAggBufferOffset)
      var yAvg = buffer1.getDouble(mutableAggBufferOffsetPlus1)
      var Ck = buffer1.getDouble(mutableAggBufferOffsetPlus2)
      var MkX = buffer1.getDouble(mutableAggBufferOffsetPlus3)
      var MkY = buffer1.getDouble(mutableAggBufferOffsetPlus4)
      var count = buffer1.getLong(mutableAggBufferOffsetPlus5)

      val xAvg2 = buffer2.getDouble(inputAggBufferOffset)
      val yAvg2 = buffer2.getDouble(inputAggBufferOffsetPlus1)
      val Ck2 = buffer2.getDouble(inputAggBufferOffsetPlus2)
      val MkX2 = buffer2.getDouble(inputAggBufferOffsetPlus3)
      val MkY2 = buffer2.getDouble(inputAggBufferOffsetPlus4)

      val totalCount = count + count2
      val deltaX = xAvg - xAvg2
      val deltaY = yAvg - yAvg2
      Ck += Ck2 + deltaX * deltaY * count / totalCount * count2
      xAvg = (xAvg * count + xAvg2 * count2) / totalCount
      yAvg = (yAvg * count + yAvg2 * count2) / totalCount
      MkX += MkX2 + deltaX * deltaX * count / totalCount * count2
      MkY += MkY2 + deltaY * deltaY * count / totalCount * count2
      count = totalCount

      buffer1.setDouble(mutableAggBufferOffset, xAvg)
      buffer1.setDouble(mutableAggBufferOffsetPlus1, yAvg)
      buffer1.setDouble(mutableAggBufferOffsetPlus2, Ck)
      buffer1.setDouble(mutableAggBufferOffsetPlus3, MkX)
      buffer1.setDouble(mutableAggBufferOffsetPlus4, MkY)
      buffer1.setLong(mutableAggBufferOffsetPlus5, count)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val count = buffer.getLong(mutableAggBufferOffsetPlus5)
    if (count > 0) {
      val Ck = buffer.getDouble(mutableAggBufferOffsetPlus2)
      val MkX = buffer.getDouble(mutableAggBufferOffsetPlus3)
      val MkY = buffer.getDouble(mutableAggBufferOffsetPlus4)
      val corr = Ck / math.sqrt(MkX * MkY)
      if (corr.isNaN) {
        null
      } else {
        corr
      }
    } else {
      null
    }
  }
}
