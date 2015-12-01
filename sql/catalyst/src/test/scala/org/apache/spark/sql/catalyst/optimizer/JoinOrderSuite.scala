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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class JoinOrderSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
        Batch("Filter Pushdown", Once,
          CombineFilters,
          PushPredicateThroughProject,
          BooleanSimplification,
          ReorderJoin,
          PushPredicateThroughJoin,
          PushPredicateThroughGenerate,
          PushPredicateThroughAggregate,
          ColumnPruning,
          ProjectCollapsing) :: Nil

  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)

  test("joins: reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val originalQuery = {
      x.join(y).join(z)
        .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.join(z, condition = Some("x.b".attr === "z.b".attr))
        .join(y, condition = Some("y.d".attr === "z.a".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateSubQueries(correctAnswer))
  }
}
