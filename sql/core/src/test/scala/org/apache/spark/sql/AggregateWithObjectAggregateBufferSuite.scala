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

package org.apache.spark.sql

import org.apache.spark.sql.AggregateWithObjectAggregateBufferSuite.MaxWithObjectAggregateBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GenericMutableRow, MutableRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, WithObjectAggregateBuffer}
import org.apache.spark.sql.execution.aggregate.{SortAggregateExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StructType}

class AggregateWithObjectAggregateBufferSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  private val data = Seq((1, 0), (3, 1), (2, 0), (6, 3), (3, 1), (4, 1), (5, 0))


  test("aggregate with object aggregate buffer, should not use HashAggregate") {
    val df = data.toDF("a", "b")
    val max = new MaxWithObjectAggregateBuffer($"a".expr)

    // Always use SortAggregateExec instead of HashAggregateExec for planning even if the aggregate
    //  buffer attributes are mutable fields (every field can be mutated inline like int, long...)
    val allFieldsMutable = max.aggBufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)
    val sparkPlan = df.select(Column(max.toAggregateExpression())).queryExecution.sparkPlan
    assert(allFieldsMutable == true && sparkPlan.isInstanceOf[SortAggregateExec])
  }

  test("aggregate with object aggregate buffer, no group by") {
    val df = data.toDF("a", "b").coalesce(2)
    checkAnswer(
      df.select(objectAggregateMax($"a"), count($"a"), objectAggregateMax($"b"), count($"b")),
      Seq(Row(6, 7, 3, 7))
    )
  }

  test("aggregate with object aggregate buffer, with group by") {
    val df = data.toDF("a", "b").coalesce(2)
    checkAnswer(
      df.groupBy($"b").agg(objectAggregateMax($"a"), count($"a"), objectAggregateMax($"a")),
      Seq(
        Row(0, 5, 3, 5),
        Row(1, 4, 3, 4),
        Row(3, 6, 1, 6)
      )
    )
  }

  test("aggregate with object aggregate buffer, empty inputs, no group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.select(objectAggregateMax($"a"), count($"a"), objectAggregateMax($"b"), count($"b")),
      Seq(Row(Int.MinValue, 0, Int.MinValue, 0)))
  }

  test("aggregate with object aggregate buffer, empty inputs, with group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.groupBy($"b").agg(objectAggregateMax($"a"), count($"a"), objectAggregateMax($"a")),
      Seq.empty[Row])
  }

  private def objectAggregateMax(column: Column): Column = {
    val max = MaxWithObjectAggregateBuffer(column.expr)
    Column(max.toAggregateExpression())
  }
}

object AggregateWithObjectAggregateBufferSuite {

  /**
   * Calculate the max value with object aggregation buffer. This stores object of class MaxValue
   * in aggregation buffer.
   */
  private case class MaxWithObjectAggregateBuffer(
      child: Expression,
      mutableAggBufferOffset: Int = 0,
      inputAggBufferOffset: Int = 0) extends ImperativeAggregate with WithObjectAggregateBuffer {

    override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
      copy(mutableAggBufferOffset = newOffset)

    override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
      copy(inputAggBufferOffset = newOffset)

    // Stores a generic object MaxValue in aggregation buffer.
    override def initialize(buffer: MutableRow): Unit = {
      // Makes sure we are using an unsafe row for aggregation buffer.
      assert(buffer.isInstanceOf[GenericMutableRow])
      buffer.update(mutableAggBufferOffset, new MaxValue(Int.MinValue))
    }

    override def update(buffer: MutableRow, input: InternalRow): Unit = {
      val inputValue = child.eval(input).asInstanceOf[Int]
      val maxValue = buffer.get(mutableAggBufferOffset, null).asInstanceOf[MaxValue]
      if (inputValue > maxValue.value) {
        maxValue.value = inputValue
      }
    }

    override def merge(buffer: MutableRow, inputBuffer: InternalRow): Unit = {
      val bufferMax = buffer.get(mutableAggBufferOffset, null).asInstanceOf[MaxValue]
      val inputMax = deserialize(inputBuffer, inputAggBufferOffset)
      if (inputMax.value > bufferMax.value) {
        bufferMax.value = inputMax.value
      }
    }

    private def deserialize(buffer: InternalRow, offset: Int): MaxValue = {
      new MaxValue((buffer.getInt(offset)))
    }

    override def serializeObjectAggregateBuffer(buffer: InternalRow, target: MutableRow): Unit = {
      val bufferMax = buffer.get(mutableAggBufferOffset, null).asInstanceOf[MaxValue]
      target(mutableAggBufferOffset) = bufferMax.value
    }

    override def eval(buffer: InternalRow): Any = {
      val max = deserialize(buffer, mutableAggBufferOffset)
      max.value
    }

    override val aggBufferAttributes: Seq[AttributeReference] =
      Seq(AttributeReference("buf", IntegerType)())

    override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
      aggBufferAttributes.map(_.newInstance())

    override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)
    override def dataType: DataType = IntegerType
    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)
    override def nullable: Boolean = true
    override def deterministic: Boolean = false
    override def children: Seq[Expression] = Seq(child)
  }

  private class MaxValue(var value: Int)
}
