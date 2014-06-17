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
import org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class FilterPushdownSuite extends OptimizerTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateAnalysisOperators) ::
      Batch("Filter Pushdown", Once,
        CombineFilters,
        PushPredicateThroughProject,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery('y)
        .select('a)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a.attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  // After this line is unimplemented.
  test("simple push down") {
    val originalQuery =
      testRelation
        .select('a)
        .where('a === 1)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a === 1)
        .select('a)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("can't push without rewrite") {
    val originalQuery =
      testRelation
        .select('a + 'b as 'e)
        .where('e === 1)
        .analyze

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a + 'b === 1)
        .select('a + 'b as 'e)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filters: combines filters") {
    val originalQuery = testRelation
      .select('a)
      .where('a === 1)
      .where('a === 2)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a === 1 && 'a === 2)
        .select('a).analyze


    comparePlans(optimized, correctAnswer)
  }


  test("joins: push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
        .where("y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation.where('b === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to one side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: rewrite filter to push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation.where('b === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val correctAnswer =
      left.join(y, LeftOuter).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val right = testRelation.where('b === 2).subquery('d)
    val correctAnswer =
      x.join(right, RightOuter).where("x.b".attr === 1).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("x.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 2).subquery('d)
    val correctAnswer =
      left.join(y, LeftOuter, Some("d.b".attr === 1)).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val right = testRelation.where('b === 2).subquery('d)
    val correctAnswer =
      x.join(right, RightOuter, Some("d.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 2).subquery('l)
    val right = testRelation.where('b === 1).subquery('r)
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val right = testRelation.where('b === 2).subquery('r)
    val correctAnswer =
      x.join(right, RightOuter, Some("r.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #4") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 2).subquery('l)
    val right = testRelation.where('b === 1).subquery('r)
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #4") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.subquery('l)
    val right = testRelation.where('b === 2).subquery('r)
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #5") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 2).subquery('l)
    val right = testRelation.where('b === 1).subquery('r)
    val correctAnswer =
      left.join(right, LeftOuter, Some("l.a".attr===3)).
        where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #5") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('a === 3).subquery('l)
    val right = testRelation.where('b === 2).subquery('r)
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: can't push down") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, condition = Some("x.b".attr === "y.b".attr))
    }
    val optimized = Optimize(originalQuery.analyze)

    comparePlans(analysis.EliminateAnalysisOperators(originalQuery.analyze), optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.where('a === 1).subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateAnalysisOperators(correctAnswer))
  }

  test("joins: conjunctive predicates #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1))
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateAnalysisOperators(correctAnswer))
  }

  test("joins: conjunctive predicates #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val z = testRelation.subquery('z)

    val originalQuery = {
      z.join(x.join(y))
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("z.a".attr >= 3) && ("z.a".attr === "x.b".attr))
    }

    val optimized = Optimize(originalQuery.analyze)
    val lleft = testRelation.where('a >= 3).subquery('z)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateAnalysisOperators(correctAnswer))
  }
}
