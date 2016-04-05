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
import org.apache.spark.sql.catalyst.expressions.NewInstance
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, MapPartitions}
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
  private val func = identity[Iterator[(Int, Int)]] _
  private val func2 = identity[Iterator[OtherTuple]] _

  def assertObjectCreations(count: Int, plan: LogicalPlan): Unit = {
    val newInstances = plan.flatMap(_.expressions.collect {
      case n: NewInstance => n
    })

    if (newInstances.size != count) {
      fail(
        s"""
           |Wrong number of object creations in plan: ${newInstances.size} != $count
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
    assertObjectCreations(1, optimized)
  }

  test("back to back with object change") {
    val input = LocalRelation('_1.int, '_2.int)
    val plan =
      MapPartitions(func,
        MapPartitions(func2, input))

    val optimized = Optimize.execute(plan.analyze)
    assertObjectCreations(2, optimized)
  }
}
