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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class NullFilteringSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NullFiltering", Once, NullFiltering) ::
      Batch("CombineFilters", Once, CombineFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("filter: filter out nulls in condition") {
    val originalQuery = testRelation.where('a === 1)
    val correctAnswer = testRelation.where(IsNotNull('a) && 'a === 1).analyze
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer)
  }

  test("join: filter out nulls on either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y,
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === 1 && "y.c".attr > 5))
    val left = x.where(IsNotNull('a) && IsNotNull('b))
    val right = y.where(IsNotNull('a) && IsNotNull('c))
    val correctAnswer = left.join(right,
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === 1 && "y.c".attr > 5)).analyze
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer)
  }

  test("join with pre-existing filters: filter out nulls on either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.where('b > 5).join(y.where('c === 10),
      condition = Some("x.a".attr === "y.a".attr))
    val left = x.where(IsNotNull('a) && IsNotNull('b) && 'b > 5)
    val right = y.where(IsNotNull('a) && IsNotNull('c) && 'c === 10)
    val correctAnswer = left.join(right,
      condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer)
  }
}
