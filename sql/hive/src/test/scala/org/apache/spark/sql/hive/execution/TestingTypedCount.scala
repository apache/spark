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

package org.apache.spark.sql.hive.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.hive.execution.TestingTypedCount.State
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - A testing aggregate function resembles COUNT " +
          "but implements ObjectAggregateFunction.")
case class TestingTypedCount(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[TestingTypedCount.State] {

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): State = TestingTypedCount.State(0L)

  override def update(buffer: State, input: InternalRow): State = {
    if (child.eval(input) != null) {
      buffer.count += 1
    }
    buffer
  }

  override def merge(buffer: State, input: State): State = {
    buffer.count += input.count
    buffer
  }

  override def eval(buffer: State): Any = buffer.count

  override def serialize(buffer: State): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)
    dataStream.writeLong(buffer.count)
    byteStream.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): State = {
    val byteStream = new ByteArrayInputStream(storageFormat)
    val dataStream = new DataInputStream(byteStream)
    TestingTypedCount.State(dataStream.readLong())
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "typed_count"
}

object TestingTypedCount {
  case class State(var count: Long)
}
