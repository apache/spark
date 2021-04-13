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

package org.apache.spark.sql.connector.catalog.functions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}

class AggregateFunctionSuite extends SparkFunSuite {
  test("Test simple iavg(int)") {
    val rows = Seq(InternalRow(2), InternalRow(2), InternalRow(2))

    val bound = IntegralAverage.bind(new StructType().add("foo", IntegerType, nullable = false))
    assert(bound.isInstanceOf[AggregateFunction[_, _]])
    val udaf = bound.asInstanceOf[AggregateFunction[Serializable, _]]

    val finalState = rows.foldLeft(udaf.newAggregationState()) { (state, row) =>
      udaf.update(state, row)
    }

    assert(udaf.produceResult(finalState) == 2)
  }

  test("Test simple iavg(long)") {
    val bigValue = 9762097370951020L
    val rows = Seq(InternalRow(bigValue + 2), InternalRow(bigValue), InternalRow(bigValue - 2))

    val bound = IntegralAverage.bind(new StructType().add("foo", LongType, nullable = false))
    assert(bound.isInstanceOf[AggregateFunction[_, _]])
    val udaf = bound.asInstanceOf[AggregateFunction[Serializable, _]]

    val finalState = rows.foldLeft(udaf.newAggregationState()) { (state, row) =>
      udaf.update(state, row)
    }

    assert(udaf.produceResult(finalState) == bigValue)
  }

  test("Test associative iavg(long)") {
    val bigValue = 7620099737951020L
    val rows = Seq(InternalRow(bigValue + 2), InternalRow(bigValue), InternalRow(bigValue - 2))

    val bound = IntegralAverage.bind(new StructType().add("foo", LongType, nullable = false))
    assert(bound.isInstanceOf[AggregateFunction[_, _]])
    val udaf = bound.asInstanceOf[AggregateFunction[Serializable, _]]

    val state1 = rows.foldLeft(udaf.newAggregationState()) { (state, row) =>
      udaf.update(state, row)
    }
    val state2 = rows.foldLeft(udaf.newAggregationState()) { (state, row) =>
      udaf.update(state, row)
    }
    val finalState = udaf.merge(state1, state2)

    assert(udaf.produceResult(finalState) == bigValue)
  }
}

object IntegralAverage extends UnboundFunction {
  override def name(): String = "iavg"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length > 1) {
      throw new UnsupportedOperationException("Too many arguments")
    }

    if (inputType.fields(0).nullable) {
      throw new UnsupportedOperationException("Nullable values are not supported")
    }

    inputType.fields(0).dataType match {
      case _: IntegerType => IntAverage
      case _: LongType => LongAverage
      case dataType =>
        throw new UnsupportedOperationException(s"Unsupported non-integral type: $dataType")
    }
  }

  override def description(): String =
    """iavg: produces an average using integer division
      |  iavg(int not null) -> int
      |  iavg(bigint not null) -> bigint""".stripMargin
}

object IntAverage extends AggregateFunction[(Int, Int), Int] {

  override def inputTypes(): Array[DataType] = Array(IntegerType)

  override def name(): String = "iavg"

  override def newAggregationState(): (Int, Int) = (0, 0)

  override def update(state: (Int, Int), input: InternalRow): (Int, Int) = {
    val i = input.getInt(0)
    val (total, count) = state
    (total + i, count + 1)
  }

  override def merge(leftState: (Int, Int), rightState: (Int, Int)): (Int, Int) = {
    (leftState._1 + rightState._1, leftState._2 + rightState._2)
  }

  override def produceResult(state: (Int, Int)): Int = state._1 / state._2

  override def resultType(): DataType = IntegerType
}

object LongAverage extends AggregateFunction[(Long, Long), Long] {

  override def inputTypes(): Array[DataType] = Array(LongType)

  override def name(): String = "iavg"

  override def newAggregationState(): (Long, Long) = (0L, 0L)

  override def update(state: (Long, Long), input: InternalRow): (Long, Long) = {
    val l = input.getLong(0)
    state match {
      case (_, 0L) =>
        (l, 1)
      case (total, count) =>
        (total + l, count + 1L)
    }
  }

  override def merge(leftState: (Long, Long), rightState: (Long, Long)): (Long, Long) = {
    (leftState._1 + rightState._1, leftState._2 + rightState._2)
  }

  override def produceResult(state: (Long, Long)): Long = state._1 / state._2

  override def resultType(): DataType = IntegerType
}
