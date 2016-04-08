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

package org.apache.spark.sql.catalyst.optimizer

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules.RuleExecutor

case class OtherTuple(_1: Int, _2: Int)

class EliminateSerializationSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Serialization", FixedPoint(100),
        EliminateSerialization) :: Nil
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()
  implicit private def intEncoder = ExpressionEncoder[Int]()
  private val func = identity[Iterator[(Int, Int)]] _
  private val func2 = identity[Iterator[OtherTuple]] _

  def assertNumSerializations(count: Int, plan: LogicalPlan): Unit = {
    val serializations = plan collect {
      case s: SerializeFromObject => s
    }

    val deserializations = plan collect {
      case d: DeserializeToObject => d
      // `MapGroups` takes input row and outputs domain object, it's also kind if deserialization.
      case m: MapGroups => m
    }

    assert(serializations.length == deserializations.length,
      "serializations and deserializations must appear in pair")

    if (serializations.size != count) {
      fail(
        s"""
           |Wrong number of serializations in plan: ${serializations.size} != $count
           |$plan
         """.stripMargin)
    }
  }

  test("back to back MapPartitions") {
    val input = LocalRelation('_1.int, '_2.int)
    val plan =
      MapPartitions(func,
        MapPartitions(func, input))

    val optimized = Optimize.execute(plan.analyze)
    assertNumSerializations(1, optimized)
  }

  test("back to back with object change") {
    val input = LocalRelation('_1.int, '_2.int)
    val plan =
      MapPartitions(func,
        MapPartitions(func2, input))

    val optimized = Optimize.execute(plan.analyze)
    assertNumSerializations(2, optimized)
  }

  test("Filter under MapPartition") {
    val input = LocalRelation('_1.int, '_2.int)
    val filter = input.filter((tuple: (Int, Int)) => tuple._1 > 1)
    val map = MapPartitions(func, filter)

    val optimized = Optimize.execute(map.analyze)
    assertNumSerializations(1, optimized)
  }

  test("Filter under MapPartition with object change") {
    val input = LocalRelation('_1.int, '_2.int)
    val filter = input.filter((tuple: OtherTuple) => tuple._1 > 1)
    val map = MapPartitions(func, filter)

    val optimized = Optimize.execute(map.analyze)
    assertNumSerializations(2, optimized)
  }

  test("MapGroups under MapPartition") {
    val input = LocalRelation('_1.int, '_2.int)
    val append = AppendColumns((tuple: OtherTuple) => tuple._1, input)
    val aggFunc: (Int, Iterator[OtherTuple]) => Iterator[OtherTuple] =
      (key, values) => Iterator(OtherTuple(1, 1))
    val agg = MapGroups(aggFunc, append.newColumns, input.output, append)
    val map = MapPartitions(func2, agg)

    val optimized = Optimize.execute(map.analyze)
    assertNumSerializations(1, optimized)
  }

  test("MapGroups under MapPartition with object change") {
    val input = LocalRelation('_1.int, '_2.int)
    val append = AppendColumns((tuple: OtherTuple) => tuple._1, input)
    val aggFunc: (Int, Iterator[OtherTuple]) => Iterator[OtherTuple] =
      (key, values) => Iterator(OtherTuple(1, 1))
    val agg = MapGroups(aggFunc, append.newColumns, input.output, append)
    val map = MapPartitions(func, agg)

    val optimized = Optimize.execute(map.analyze)
    assertNumSerializations(2, optimized)
  }

  test("MapPartitions under AppendColumns") {
    val input = LocalRelation('_1.int, '_2.int)
    val map = MapPartitions(func2, input)
    val appendFunc = (tuple: OtherTuple) => tuple._1
    val append = AppendColumns(appendFunc, map)

    val optimized = Optimize.execute(append.analyze)

    val mapWithoutSer = map.asInstanceOf[SerializeFromObject].child
    val expected = AppendColumnsWithObject(
      appendFunc.asInstanceOf[Any => Any],
      productEncoder[OtherTuple].namedExpressions,
      intEncoder.namedExpressions,
      mapWithoutSer).analyze

    comparePlans(optimized, expected)
  }
}
