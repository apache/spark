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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * Calculate the approximate quantile.
 * @param child
 * @param quantile
 * @param epsilon
 *
 * This computes approximate quantile based on the paper
 * Greenwald, Michael and Khanna, Sanjeev, "Space-efficient Online Computation of
 * Quantile Summaries," SIGMOD '01.
 */
case class ApproxQuantile(
    child: Expression,
    quantile: Double,
    epsilon: Double = 0.05,
    compressThreshold: Int = 1000,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  def this(child: Expression, quantile: Double, epsilon: Double, compressThreshold: Int) =
    this(child, quantile, epsilon, compressThreshold, mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)

  def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[DoubleType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"approxQuantile requires the argument is double type, " +
          s"not ${child.dataType}.")
    }
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map(_.newInstance())
  }

  private[this] val innerStruct =
    StructType(
      StructField("f1", DoubleType, false) ::
      StructField("f2", IntegerType, false) ::
      StructField("f3", IntegerType, false) :: Nil)

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(
    AttributeReference("sampled", ArrayType(innerStruct, false))(),
    AttributeReference("count", LongType)())

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private[this] def getConstant(count: Long): Double = 2 * epsilon * count

  private[this] def compress(sampled: mutable.Buffer[InternalRow], count: Long): Unit = {
    var i = 0
    while (i < sampled.size - 1) {
      val sample1 = sampled(i)
      val sample2 = sampled(i + 1)
      if (sample1.getInt(1) + sample2.getInt(1) +
        sample2.getInt(2) < math.floor(getConstant(count))) {
        sampled.update(i + 1,
          InternalRow(sample2.getDouble(0), sample1.getInt(1) + sample2.getInt(1),
            sample2.getInt(2)))
        sampled.remove(i)
      }
      i += 1
    }
  }

  private[this] def query(sampled: Array[InternalRow], count: Long): Double = {
    val rank = (quantile * count).toInt
    var minRank = 0
    var i = 1
    while (i < sampled.size) {
      val curSample = sampled(i)
      val prevSample = sampled(i - 1)
      minRank += prevSample.getInt(1)
      if (minRank + curSample.getInt(1) + curSample.getInt(1) > rank + getConstant(count)) {
        return prevSample.getDouble(0)
      }
      i += 1
    }
    return sampled.last.getDouble(0)
  }

  override def initialize(buffer: MutableRow): Unit = {
    buffer.update(mutableAggBufferOffset,
      new GenericArrayData(Array[InternalRow]().asInstanceOf[Array[Any]]))
    buffer.setLong(mutableAggBufferOffset + 1, 0L)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      val inputValue = v.asInstanceOf[Double]

      val sampled: mutable.Buffer[InternalRow] = new mutable.ArrayBuffer[InternalRow]()
      buffer.getArray(mutableAggBufferOffset).toArray[InternalRow](innerStruct)
        .copyToBuffer(sampled)

      var count: Long = buffer.getLong(mutableAggBufferOffset + 1)

      var idx: Int = sampled.indexWhere(_.getDouble(0) > inputValue)
      if (idx == -1) {
        idx = sampled.size
      }

      val delta: Int = if (idx == 0 || idx == sampled.size) {
        0
      } else {
        math.floor(getConstant(count)).toInt
      }

      val tuple = InternalRow(inputValue, 1, delta)
      sampled.insert(idx, tuple)
      count += 1

      if (sampled.size > compressThreshold) {
        compress(sampled, count)
      }

      buffer.update(mutableAggBufferOffset, new GenericArrayData(sampled.toArray))
      buffer.setLong(mutableAggBufferOffset + 1, count)
    }
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val otherCount = buffer2.getLong(inputAggBufferOffset + 1)
    var count: Long = buffer1.getLong(mutableAggBufferOffset + 1)

    val otherSampled = new mutable.ArrayBuffer[InternalRow]()
    buffer2.getArray(inputAggBufferOffset).toArray[InternalRow](innerStruct)
      .copyToBuffer(otherSampled)
    val sampled: mutable.Buffer[InternalRow] = new mutable.ArrayBuffer[InternalRow]()
    buffer1.getArray(mutableAggBufferOffset).toArray[InternalRow](innerStruct).copyToBuffer(sampled)

    if (otherCount > 0 && count > 0) {
      otherSampled.foreach { sample =>
        val idx = sampled.indexWhere(s => s.getDouble(0) > sample.getDouble(0))
        if (idx == 0) {
          val new_sampled =
            InternalRow(sampled(0).getDouble(0), sampled(0).getInt(1), sampled(1).getInt(2) / 2)
          sampled.update(0, new_sampled)
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1), 0)
          sampled.insert(0, new_sample)
        } else if (idx == -1) {
          val new_sampled = InternalRow(sampled(sampled.size - 1).getDouble(0),
            sampled(sampled.size - 1).getInt(1),
            (sampled(sampled.size - 2).getInt(2) * 2 * epsilon).toInt)
          sampled.update(sampled.size - 1, new_sampled)
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1), 0)
          sampled.insert(sampled.size, new_sample)
        } else {
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1),
            (sampled(idx - 1).getInt(2) + sampled(idx).getInt(2)) / 2)
          sampled.insert(idx, new_sample)
        }
      }

      count = count + otherCount
      compress(sampled, count)
      buffer1.update(mutableAggBufferOffset, new GenericArrayData(sampled.toArray))
      buffer1.setLong(mutableAggBufferOffset + 1, count)
    } else if (otherCount > 0) {
      buffer1.update(mutableAggBufferOffset, new GenericArrayData(otherSampled.toArray))
      buffer1.setLong(mutableAggBufferOffset + 1, otherCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val count: Long = buffer.getLong(mutableAggBufferOffset + 1)
    val sampled: Array[InternalRow] =
      buffer.getArray(mutableAggBufferOffset).toArray[InternalRow](innerStruct)
    query(sampled, count)
  }
}
