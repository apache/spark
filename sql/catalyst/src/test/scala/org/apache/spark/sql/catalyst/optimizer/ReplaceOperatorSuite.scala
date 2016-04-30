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
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin) :: Nil
  }

  test("replace Intersect with Left-semi Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = Intersect(table1, table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Left-anti Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = Except(table1, table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftAnti, Option('a <=> 'c && 'b <=> 'd))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Distinct with Aggregate") {
    val input = LocalRelation('a.int, 'b.int)

    val query = Distinct(input)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = Aggregate(input.output, input.output, input)

    comparePlans(optimized, correctAnswer)
  }
}
