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
import org.apache.spark.sql.catalyst.plans.{LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        ReplaceIntersectWithSemiJoin,
        ReplaceDistinctWithAggregate,
        EliminateDistinct) :: Nil
  }

  test("replace Intersect with Left-semi Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = Intersect(table1, table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      table1.join(table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd)).groupBy('a, 'b)('a, 'b).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Intersect with Left-semi Join whose left is Distinct") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = table1.distinct().intersect(table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      table1.groupBy('a, 'b)('a, 'b).join(table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("continuous Intersect whose children containing Distinct") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)
    val table3 = LocalRelation('e.int, 'f.int)

    // DISTINCT (actually, it is AGGREGATE) is the direct child
    val query1 = table1.distinct().intersect(table2).intersect(table3)
    val correctAnswer1 =
      table1.groupBy('a, 'b)('a, 'b)
        .join(table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd))
        .join(table3, LeftSemi, Option('a <=> 'e && 'b <=> 'f)).analyze
    comparePlans(Optimize.execute(query1.analyze), correctAnswer1)
  }

  test("replace Intersect with Left-semi Join whose left is inferred to have distinct values") {
    val table1 = LocalRelation('a.int)
    val table2 = LocalRelation('c.int, 'd.int)
    val table3 = LocalRelation('e.int, 'f.int)

    // DISTINCT is inferred from the child's child
    val query2 = table1.distinct()
      .where('a > 3).limit(5)
      .select('a.attr, ('a + 1).as("b")).orderBy('a.asc, 'b.desc)
      .intersect(table2).intersect(table3)
    val correctAnswer2 =
      table1.groupBy('a)('a).where('a > 3).limit(5)
        .select('a.attr, ('a + 1).as("b")).orderBy('a.asc, 'b.desc)
        .join(table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd))
        .join(table3, LeftSemi, Option('a <=> 'e && 'b <=> 'f)).analyze
    comparePlans(Optimize.execute(query2.analyze), correctAnswer2)
  }

  test("replace Intersect with Left-semi Join whose left is the Distinct") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = table1.groupBy('a, 'b)('a, 'b).intersect(table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      table1.groupBy('a, 'b)('a, 'b).join(table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Distinct with Aggregate") {
    val input = LocalRelation('a.int, 'b.int)

    val query = input.distinct()
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = input.groupBy('a, 'b)('a, 'b).analyze

    comparePlans(optimized, correctAnswer)
  }
}
