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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap


object PivotFirst {
  def apply(pivotColumn: Expression,
            valueColumn: Expression,
            pivotValues: Seq[Literal]): PivotFirst = {
    val pivotIndex = HashMap(pivotValues.map(_.value).zipWithIndex: _*)
    PivotFirst(pivotColumn, valueColumn, pivotIndex)
  }
}

case class PivotFirst(pivotColumn: Expression,
                      valueColumn: Expression,
                      pivotIndex: Map[Any, Int],
                      mutableAggBufferOffset: Int = 0,
                      inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  val valueDataType = valueColumn.dataType
  val indexSize = pivotIndex.size

  override def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    val index = mutableAggBufferOffset + pivotIndex(pivotColumn.eval(inputRow))
    val value = valueColumn.eval(inputRow)
    mutableAggBuffer.update(index, value)
  }

  override def merge(mutableAggBuffer: MutableRow, inputAggBuffer: InternalRow): Unit = {
    for ( i <- 0 until indexSize) {
      val value = inputAggBuffer.get(inputAggBufferOffset + i, valueDataType)
      mutableAggBuffer.update(mutableAggBufferOffset + i, value)
    }
  }

  override def initialize(mutableAggBuffer: MutableRow): Unit = {
    for ( i <- 0 until indexSize) {
      mutableAggBuffer.setNullAt(mutableAggBufferOffset + i)
    }
  }

  override def eval(input: InternalRow): Any = {
    val result = new Array[Any](indexSize)
    for ( i <- 0 until indexSize) {
      result(i) = input.get(mutableAggBufferOffset + i, valueDataType)
    }
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)


  override def aggBufferAttributes: Seq[AttributeReference] =
    (0 until indexSize).map(i => AttributeReference(i.toString, valueColumn.dataType)())

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def inputTypes: Seq[AbstractDataType] = children.map(_.dataType)

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(valueColumn.dataType)

  override def children: Seq[Expression] = pivotColumn :: valueColumn :: Nil
}
