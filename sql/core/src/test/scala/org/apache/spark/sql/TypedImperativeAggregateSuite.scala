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

import org.apache.spark.sql.TypedImperativeAggregateSuite.TypedMax
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, UserDefinedType}

class TypedImperativeAggregateSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  private val data = Seq((1, 0), (3, 1), (2, 0), (6, 3), (3, 1), (4, 1), (5, 0))

  test("aggregate with object aggregate buffer") {
    val agg = new TypedMax(BoundReference(0, IntegerType, nullable = false))

    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    assert(mergeBuffer.value == data.map(_._1).max)
    assert(agg.eval(mergeBuffer) == data.map(_._1).max)
  }

  test("dataframe aggregate with object aggregate buffer, should not use HashAggregate") {
    val df = data.toDF("a", "b")
    val max = new TypedMax($"a".expr)

    // Always use SortAggregateExec instead of HashAggregateExec for planning even if the aggregate
    //  buffer attributes are mutable fields (every field can be mutated inline like int, long...)
    val allFieldsMutable = max.aggBufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)
    val sparkPlan = df.select(Column(max.toAggregateExpression())).queryExecution.sparkPlan
    assert(allFieldsMutable == true && sparkPlan.isInstanceOf[SortAggregateExec])
  }

  test("dataframe aggregate with object aggregate buffer, no group by") {
    val df = data.toDF("a", "b").coalesce(2)
    checkAnswer(
      df.select(typedMax($"a"), count($"a"), typedMax($"b"), count($"b")),
      Seq(Row(6, 7, 3, 7))
    )
  }

  test("dataframe aggregate with object aggregate buffer, with group by") {
    val df = data.toDF("a", "b").coalesce(2)
    checkAnswer(
      df.groupBy($"b").agg(typedMax($"a"), count($"a"), typedMax($"a")),
      Seq(
        Row(0, 5, 3, 5),
        Row(1, 4, 3, 4),
        Row(3, 6, 1, 6)
      )
    )
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, no group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.select(typedMax($"a"), count($"a"), typedMax($"b"), count($"b")),
      Seq(Row(Int.MinValue, 0, Int.MinValue, 0)))
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, with group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.groupBy($"b").agg(typedMax($"a"), count($"a"), typedMax($"a")),
      Seq.empty[Row])
  }

  private def typedMax(column: Column): Column = {
    val max = TypedMax(column.expr)
    Column(max.toAggregateExpression())
  }
}

object TypedImperativeAggregateSuite {

  /**
   * Calculate the max value with object aggregation buffer. This stores class MaxValue
   * in aggregation buffer.
   */
  private case class TypedMax(
      child: Expression,
      mutableAggBufferOffset: Int = 0,
      inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[MaxValue] {

    override lazy val aggregationBufferType: UserDefinedType[MaxValue] = new MaxValueUDT()

    override def createAggregationBuffer(): MaxValue = {
      new MaxValue(Int.MinValue)
    }

    override def update(buffer: MaxValue, input: InternalRow): Unit = {
      val inputValue = child.eval(input).asInstanceOf[Int]
      if (inputValue > buffer.value) {
        buffer.value = inputValue
      }
    }

    override def merge(bufferMax: MaxValue, inputMax: MaxValue): Unit = {
      if (inputMax.value > bufferMax.value) {
        bufferMax.value = inputMax.value
      }
    }

    override def eval(bufferMax: MaxValue): Any = {
      bufferMax.value
    }

    override def nullable: Boolean = true

    override def deterministic: Boolean = false

    override def children: Seq[Expression] = Seq(child)

    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

    override def dataType: DataType = IntegerType

    override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
      copy(mutableAggBufferOffset = newOffset)

    override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
      copy(inputAggBufferOffset = newOffset)

  }

  private class MaxValue(var value: Int)

  private class MaxValueUDT extends UserDefinedType[MaxValue] {
    override def sqlType: DataType = IntegerType

    override def serialize(obj: MaxValue): Any = obj.value

    override def userClass: Class[MaxValue] = classOf[MaxValue]

    override def deserialize(datum: Any): MaxValue = {
      datum match {
        case i: Int => new MaxValue(i)
      }
    }
  }
}
