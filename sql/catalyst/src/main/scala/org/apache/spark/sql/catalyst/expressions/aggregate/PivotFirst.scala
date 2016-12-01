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

import scala.collection.immutable.HashMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

object PivotFirst {

  def supportsDataType(dataType: DataType): Boolean = updateFunction.isDefinedAt(dataType)

  // Currently UnsafeRow does not support the generic update method (throws
  // UnsupportedOperationException), so we need to explicitly support each DataType.
  private val updateFunction: PartialFunction[DataType, (InternalRow, Int, Any) => Unit] = {
    case DoubleType =>
      (row, offset, value) => row.setDouble(offset, value.asInstanceOf[Double])
    case IntegerType =>
      (row, offset, value) => row.setInt(offset, value.asInstanceOf[Int])
    case LongType =>
      (row, offset, value) => row.setLong(offset, value.asInstanceOf[Long])
    case FloatType =>
      (row, offset, value) => row.setFloat(offset, value.asInstanceOf[Float])
    case BooleanType =>
      (row, offset, value) => row.setBoolean(offset, value.asInstanceOf[Boolean])
    case ShortType =>
      (row, offset, value) => row.setShort(offset, value.asInstanceOf[Short])
    case ByteType =>
      (row, offset, value) => row.setByte(offset, value.asInstanceOf[Byte])
    case d: DecimalType =>
      (row, offset, value) => row.setDecimal(offset, value.asInstanceOf[Decimal], d.precision)
  }
}

/**
 * PivotFirst is an aggregate function used in the second phase of a two phase pivot to do the
 * required rearrangement of values into pivoted form.
 *
 * For example on an input of
 * A | B
 * --+--
 * x | 1
 * y | 2
 * z | 3
 *
 * with pivotColumn=A, valueColumn=B, and pivotColumnValues=[z,y] the output is [3,2].
 *
 * @param pivotColumn column that determines which output position to put valueColumn in.
 * @param valueColumn the column that is being rearranged.
 * @param pivotColumnValues the list of pivotColumn values in the order of desired output. Values
 *                          not listed here will be ignored.
 */
case class PivotFirst(
  pivotColumn: Expression,
  valueColumn: Expression,
  pivotColumnValues: Seq[Any],
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  override val children: Seq[Expression] = pivotColumn :: valueColumn :: Nil

  override val nullable: Boolean = false

  val valueDataType = valueColumn.dataType

  override val dataType: DataType = ArrayType(valueDataType)

  val pivotIndex = HashMap(pivotColumnValues.zipWithIndex: _*)

  val indexSize = pivotIndex.size

  private val updateRow: (InternalRow, Int, Any) => Unit = PivotFirst.updateFunction(valueDataType)

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val pivotColValue = pivotColumn.eval(inputRow)
    if (pivotColValue != null) {
      // We ignore rows whose pivot column value is not in the list of pivot column values.
      val index = pivotIndex.getOrElse(pivotColValue, -1)
      if (index >= 0) {
        val value = valueColumn.eval(inputRow)
        if (value != null) {
          updateRow(mutableAggBuffer, mutableAggBufferOffset + index, value)
        }
      }
    }
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    for (i <- 0 until indexSize) {
      if (!inputAggBuffer.isNullAt(inputAggBufferOffset + i)) {
        val value = inputAggBuffer.get(inputAggBufferOffset + i, valueDataType)
        updateRow(mutableAggBuffer, mutableAggBufferOffset + i, value)
      }
    }
  }

  override def initialize(mutableAggBuffer: InternalRow): Unit = valueDataType match {
    case d: DecimalType =>
      // Per doc of setDecimal we need to do this instead of setNullAt for DecimalType.
      for (i <- 0 until indexSize) {
        mutableAggBuffer.setDecimal(mutableAggBufferOffset + i, null, d.precision)
      }
    case _ =>
      for (i <- 0 until indexSize) {
        mutableAggBuffer.setNullAt(mutableAggBufferOffset + i)
      }
  }

  override def eval(input: InternalRow): Any = {
    val result = new Array[Any](indexSize)
    for (i <- 0 until indexSize) {
      result(i) = input.get(mutableAggBufferOffset + i, valueDataType)
    }
    new GenericArrayData(result)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)


  override val aggBufferAttributes: Seq[AttributeReference] =
    pivotIndex.toList.sortBy(_._2).map(kv => AttributeReference(kv._1.toString, valueDataType)())

  override val aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())
}

