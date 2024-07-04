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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

case class OtherTuple(_1: Int, _2: Int)

class EliminateSerializationSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Serialization", FixedPoint(100),
        EliminateSerialization) :: Nil
  }

  implicit private def productEncoder[T <: Product : TypeTag]: ExpressionEncoder[T] =
    ExpressionEncoder[T]()
  implicit private def intEncoder: ExpressionEncoder[Int] = ExpressionEncoder[Int]()

  test("back to back serialization") {
    val input = LocalRelation($"obj".obj(classOf[(Int, Int)]))
    val plan = input.serialize[(Int, Int)].deserialize[(Int, Int)].analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select($"obj".as("obj")).analyze
    comparePlans(optimized, expected)
  }

  test("back to back serialization with object change") {
    val input = LocalRelation($"obj".obj(classOf[OtherTuple]))
    val plan = input.serialize[OtherTuple].deserialize[(Int, Int)].analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("back to back serialization in AppendColumns") {
    val input = LocalRelation($"obj".obj(classOf[(Int, Int)]))
    val func = (item: (Int, Int)) => item._1
    val plan = AppendColumns(func, input.serialize[(Int, Int)]).analyze

    val optimized = Optimize.execute(plan)

    val expected = AppendColumnsWithObject(
      func.asInstanceOf[Any => Any],
      productEncoder[(Int, Int)].namedExpressions,
      intEncoder.namedExpressions,
      input).analyze

    comparePlans(optimized, expected)
  }

  test("back to back serialization in AppendColumns with object change") {
    val input = LocalRelation($"obj".obj(classOf[OtherTuple]))
    val func = (item: (Int, Int)) => item._1
    val plan = AppendColumns(func, input.serialize[OtherTuple]).analyze

    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }
}
