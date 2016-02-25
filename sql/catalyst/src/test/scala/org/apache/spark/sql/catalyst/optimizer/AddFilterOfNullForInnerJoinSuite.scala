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
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.IntegerType

class AddFilterOfNullForInnerJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Add Filter", FixedPoint(100),
        AddFilterOfNullForInnerJoin) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  test("add filters to left and right of inner join") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, condition = Some("d".attr === "b".attr && "d".attr === "c".attr && "a".attr > 10))
    }

    val nullLiteral = Literal.create(null, IntegerType)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(IsNotNull('b) && IsNotNull('c))
    val right = testRelation1.where(IsNotNull('d))
    val correctAnswer =
      left.join(right,
        condition = Some(("d".attr === "b".attr && "d".attr === "c".attr) && "a".attr > 10)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("don't need to add filters to left and right of inner join if constraints are ready") {
    // left and right of join node already have not null constraints
    val x = testRelation.subquery('x).where(IsNotNull('b) && IsNotNull('c)).select('a, 'b, 'c)
    val y = testRelation1.subquery('y).where(IsNotNull('d)).select('d)

    val originalQuery = {
      x.join(y, condition = Some("d".attr === "b".attr && "d".attr === "c".attr && "a".attr > 10))
    }

    val nullLiteral = Literal.create(null, IntegerType)

    val optimized = Optimize.execute(originalQuery.analyze)
    assert(optimized.collect {
      case j @ Join(left, right, _, _)
        if !left.isInstanceOf[Filter] && !right.isInstanceOf[Filter] => 1
    }.nonEmpty)
  }
}
