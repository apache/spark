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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.StringType

class CollapseGetJsonObjectSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseGetJsonObject", FixedPoint(10),
        CollapseGetJsonObject) :: Nil
  }

  val testRelation = LocalRelation($"json".string)
  val attr = testRelation.output.head

  test("collapse adjacent get_json_object to one") {
    val query = testRelation.select(
      GetJsonObject(GetJsonObject(attr, Literal("$.a")), Literal("$.b")).as("b"))
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(GetJsonObject(attr, Literal("$.a.b")).as("b"))

    comparePlans(optimized, correctAnswer)
  }

  test("collapse multiple adjacent get_json_object to one") {
    val query = testRelation.select(
      GetJsonObject(
        GetJsonObject(GetJsonObject(attr, Literal("$.a")), Literal("$.b")), Literal("$.c")).as("c"))
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(GetJsonObject(attr, Literal("$.a.b.c")).as("c"))

    comparePlans(optimized, correctAnswer)
  }

  test("collapse invalid path to null") {
    val query = testRelation.select(
      GetJsonObject(GetJsonObject(attr, Literal("$.a")), Literal(".b")).as("b"))
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      GetJsonObject(attr, Literal.create(null, StringType)).as("b"))

    comparePlans(optimized, correctAnswer)
  }
}
