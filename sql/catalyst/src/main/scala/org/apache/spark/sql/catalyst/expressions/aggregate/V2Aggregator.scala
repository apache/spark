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
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, UnsafeProjection}
import org.apache.spark.sql.connector.catalog.functions.{AggregateFunction => V2AggregateFunction}
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

case class V2Aggregator[BUF <: java.io.Serializable, OUT](
    aggrFunc: V2AggregateFunction[BUF, OUT],
    children: Seq[Expression],
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[BUF] with ImplicitCastInputTypes {

  private[this] lazy val inputProjection = UnsafeProjection.create(children)

  override def nullable: Boolean = aggrFunc.isResultNullable
  override def dataType: DataType = aggrFunc.resultType()
  override def inputTypes: Seq[AbstractDataType] = aggrFunc.inputTypes().toImmutableArraySeq
  override def createAggregationBuffer(): BUF = aggrFunc.newAggregationState()

  override def update(buffer: BUF, input: InternalRow): BUF = {
    aggrFunc.update(buffer, inputProjection(input))
  }

  override def merge(buffer: BUF, input: BUF): BUF = aggrFunc.merge(buffer, input)

  override def eval(buffer: BUF): Any = {
    aggrFunc.produceResult(buffer)
  }

  override def serialize(buffer: BUF): Array[Byte] = {
    Utils.serialize(buffer)
  }

  override def deserialize(bytes: Array[Byte]): BUF = {
    Utils.deserialize(bytes)
  }

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): V2Aggregator[BUF, OUT] =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): V2Aggregator[BUF, OUT] =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

