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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{LeftSemi, PlanTest, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.IntegerType

class FilterPushdownSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("Filter Pushdown", Once,
        SamplePushDown,
        CombineFilters,
        PushPredicateThroughProject,
        BooleanSimplification,
        PushPredicateThroughJoin,
        PushPredicateThroughGenerate,
        ColumnPruning,
        ProjectCollapsing) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery('y)
        .select('a)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a.attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for group") {
    val originalQuery =
      testRelation
        .groupBy('a)('a, Count('b))
        .select('a)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .groupBy('a)('a)
        .select('a).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for group with alias") {
    val originalQuery =
      testRelation
        .groupBy('a)('a as 'c, Count('b))
        .select('c)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .groupBy('a)('a as 'c)
        .select('c).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for Project(ne, Limit)") {
    val originalQuery =
      testRelation
        .select('a, 'b)
        .limit(2)
        .select('a)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  // After this line is unimplemented.
  test("simple push down") {
    val originalQuery =
      testRelation
        .select('a)
        .where('a === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a + 'b === 1)
        .select('a + 'b as 'e)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: can't push down filter through project") {
    val originalQuery = testRelation
      .select(Rand(10).as('rand), 'a)
      .where('rand > 5 || 'a > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("nondeterministic: push down part of filter through project") {
    val originalQuery = testRelation
      .select(Rand(10).as('rand), 'a)
      .where('rand > 5 && 'a > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    val correctAnswer = testRelation
      .where('a > 5)
      .select(Rand(10).as('rand), 'a)
      .where('rand > 5)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: push down filter through project") {
    val originalQuery = testRelation
      .select(Rand(10).as('rand), 'a)
      .where('a > 5 && 'a < 10)
      .analyze

    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = testRelation
      .where('a > 5 && 'a < 10)
      .select(Rand(10).as('rand), 'a)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filters: combines filters") {
    val originalQuery = testRelation
      .select('a)
      .where('a === 1)
      .where('a === 2)

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to one side after transformCondition") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y)
       .where(("x.a".attr === 1 && "y.d".attr === "x.b".attr) ||
              ("x.a".attr === 1 && "y.d".attr === "x.c".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1)
    val right = testRelation1
    val correctAnswer =
      left.join(right, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: rewrite filter to push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation.where('b === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left semi join") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, LeftSemi, Option("x.a".attr === "y.d".attr && "x.b".attr >= 1 && "y.d".attr >= 2))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('b >= 1)
    val right = testRelation1.where('d >= 2)
    val correctAnswer =
      left.join(right, LeftSemi, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #1") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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

    val optimized = Optimize.execute(originalQuery.analyze)
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
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(analysis.EliminateSubQueries(originalQuery.analyze), optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.where('a === 1).subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateSubQueries(correctAnswer))
  }

  test("joins: conjunctive predicates #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateSubQueries(correctAnswer))
  }

  test("joins: conjunctive predicates #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val z = testRelation.subquery('z)

    val originalQuery = {
      z.join(x.join(y))
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) &&
          ("z.a".attr >= 3) && ("z.a".attr === "x.b".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val lleft = testRelation.where('a >= 3).subquery('z)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze

    comparePlans(optimized, analysis.EliminateSubQueries(correctAnswer))
  }

  val testRelationWithArrayType = LocalRelation('a.int, 'b.int, 'c_arr.array(IntegerType))

  test("generate: predicate referenced no generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode('c_arr), true, false, Some("arr"))
        .where(('b >= 5) && ('a > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = {
      testRelationWithArrayType
        .where(('b >= 5) && ('a > 6))
        .generate(Explode('c_arr), true, false, Some("arr")).analyze
    }

    comparePlans(optimized, correctAnswer)
  }

  test("generate: part of conjuncts referenced generated column") {
    val generator = Explode('c_arr)
    val originalQuery = {
      testRelationWithArrayType
        .generate(generator, true, false, Some("arr"))
        .where(('b >= 5) && ('c > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val referenceResult = {
      testRelationWithArrayType
        .where('b >= 5)
        .generate(generator, true, false, Some("arr"))
        .where('c > 6).analyze
    }

    // Since newly generated columns get different ids every time being analyzed
    // e.g. comparePlans(originalQuery.analyze, originalQuery.analyze) fails.
    // So we check operators manually here.
    // Filter("c" > 6)
    assertResult(classOf[Filter])(optimized.getClass)
    assertResult(1)(optimized.asInstanceOf[Filter].condition.references.size)
    assertResult("c"){
      optimized.asInstanceOf[Filter].condition.references.toSeq(0).name
    }

    // the rest part
    comparePlans(optimized.children(0), referenceResult.children(0))
  }

  test("generate: all conjuncts referenced generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode('c_arr), true, false, Some("arr"))
        .where(('c > 6) || ('b > 5)).analyze
    }
    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("push down project past sort") {
    val x = testRelation.subquery('x)

    // push down valid
    val originalQuery = {
      x.select('a, 'b)
       .sortBy(SortOrder('a, Ascending))
       .select('a)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.select('a)
       .sortBy(SortOrder('a, Ascending)).analyze

    comparePlans(optimized, analysis.EliminateSubQueries(correctAnswer))

    // push down invalid
    val originalQuery1 = {
      x.select('a, 'b)
       .sortBy(SortOrder('a, Ascending))
       .select('b)
    }

    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 =
      x.select('a, 'b)
       .sortBy(SortOrder('a, Ascending))
       .select('b).analyze

    comparePlans(optimized1, analysis.EliminateSubQueries(correctAnswer1))
  }

  test("push project and filter down into sample") {
    val x = testRelation.subquery('x)
    val originalQuery =
      Sample(0.0, 0.6, false, 11L, x).select('a)

    val originalQueryAnalyzed = EliminateSubQueries(analysis.SimpleAnalyzer.execute(originalQuery))

    val optimized = Optimize.execute(originalQueryAnalyzed)

    val correctAnswer =
      Sample(0.0, 0.6, false, 11L, x.select('a))

    comparePlans(optimized, correctAnswer.analyze)
  }
}
