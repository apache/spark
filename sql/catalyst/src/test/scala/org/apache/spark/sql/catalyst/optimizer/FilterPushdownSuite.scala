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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class FilterPushdownSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {

    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(10),
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        PushPredicateThroughJoin,
        CollapseProject) ::
      Batch("Push extra predicate through join", FixedPoint(10),
        PushExtraPredicateThroughJoin,
        PushDownPredicates) :: Nil
  }

  val attrA = $"a".int
  val attrB = $"b".int
  val attrC = $"c".int
  val attrD = $"d".int

  val testRelation = LocalRelation(attrA, attrB, attrC)

  val testRelation1 = LocalRelation(attrD)

  val simpleDisjunctivePredicate =
    ("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11)
  val expectedPredicatePushDownResult = {
    val left = testRelation.where(($"a" > 3 || $"a" > 1)).subquery("x")
    val right = testRelation.where($"a" > 13 || $"a" > 11).subquery("y")
    left.join(right, condition = Some("x.b".attr === "y.b".attr
      && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11)))).analyze
  }

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery("y")
        .select($"a")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a".attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  // After this line is unimplemented.
  test("simple push down") {
    val originalQuery =
      testRelation
        .select($"a")
        .where($"a" === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where($"a" === 1)
        .select($"a")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combine redundant filters") {
    val originalQuery =
      testRelation
        .where($"a" === 1 && $"b" === 1)
        .where($"a" === 1 && $"c" === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where($"a" === 1 && $"b" === 1 && $"c" === 1)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not combine non-deterministic filters even if they are identical") {
    val originalQuery =
      testRelation
        .where(Rand(0) > 0.1 && $"a" === 1)
        .where(Rand(0) > 0.1 && $"a" === 1).analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("SPARK-16164: Filter pushdown should keep the ordering in the logical plan") {
    val originalQuery =
      testRelation
        .where($"a" === 1)
        .select($"a", $"b")
        .where($"b" === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where($"a" === 1 && $"b" === 1)
        .select($"a", $"b")
        .analyze

    // We can not use comparePlans here because it normalized the plan.
    assert(optimized == correctAnswer)
  }

  test("SPARK-16994: filter should not be pushed through limit") {
    val originalQuery = testRelation.limit(10).where($"a" === 1).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("can't push without rewrite") {
    val originalQuery =
      testRelation
        .select($"a" + $"b" as "e")
        .where($"e" === 1)
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where($"a" + $"b" === 1)
        .select($"a" + $"b" as "e")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: can always push down filter through project with deterministic field") {
    val originalQuery = testRelation
      .select($"a")
      .where(Rand(10) > 5 || $"a" > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    val correctAnswer = testRelation
      .where(Rand(10) > 5 || $"a" > 5)
      .select($"a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: can't push down filter through project with nondeterministic field") {
    val originalQuery = testRelation
      .select(Rand(10).as("rand"), $"a")
      .where($"a" > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("nondeterministic: can't push down filter through aggregate with nondeterministic field") {
    val originalQuery = testRelation
      .groupBy($"a")($"a", Rand(10).as("rand"))
      .where($"a" > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("nondeterministic: push down part of filter through aggregate with deterministic field") {
    val originalQuery = testRelation
      .groupBy($"a")($"a")
      .where($"a" > 5 && Rand(10) > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where($"a" > 5)
      .groupBy($"a")($"a")
      .where(Rand(10) > 5)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Can't push down nondeterministic filter through aggregate") {
    val originalQuery = testRelation
      .groupBy($"a")($"a", count($"b") as "c")
      .where(Rand(10) > $"a")
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("filters: combines filters") {
    val originalQuery = testRelation
      .select($"a")
      .where($"a" === 1)
      .where($"a" === 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where($"a" === 1 && $"a" === 2)
        .select($"a").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to either side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
        .where("y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 1)
    val right = testRelation.where($"b" === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to one side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 1)
    val right = testRelation
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: do not push down non-deterministic filters into join condition") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery = x.join(y).where(Rand(10) > 5.0).analyze
    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("joins: push to one side after transformCondition") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery = {
      x.join(y)
       .where(("x.a".attr === 1 && "y.d".attr === "x.b".attr) ||
              ("x.a".attr === 1 && "y.d".attr === "x.c".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" === 1)
    val right = testRelation1
    val correctAnswer =
      left.join(right, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: rewrite filter to push to either side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 1)
    val right = testRelation.where($"b" === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left semi join") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery = {
      x.join(y, LeftSemi, Option("x.a".attr === "y.d".attr && "x.b".attr >= 1 && "y.d".attr >= 2))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" >= 1)
    val right = testRelation1.where($"d" >= 2)
    val correctAnswer =
      left.join(right, LeftSemi, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #1") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 1)
    val correctAnswer =
      left.join(y, LeftOuter).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #1") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, RightOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where($"b" === 2).subquery("d")
    val correctAnswer =
      x.join(right, RightOuter).where("x.b".attr === 1).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #2") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, LeftOuter, Some("x.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 2).subquery("d")
    val correctAnswer =
      left.join(y, LeftOuter, Some("d.b".attr === 1)).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #2") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where($"b" === 2).subquery("d")
    val correctAnswer =
      x.join(right, RightOuter, Some("d.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #3") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 2).subquery("l")
    val right = testRelation.where($"b" === 1).subquery("r")
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #3") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where($"b" === 2).subquery("r")
    val correctAnswer =
      x.join(right, RightOuter, Some("r.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #4") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 2).subquery("l")
    val right = testRelation.where($"b" === 1).subquery("r")
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #4") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery("l")
    val right = testRelation.where($"b" === 2).subquery("r")
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #5") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" === 2).subquery("l")
    val right = testRelation.where($"b" === 1).subquery("r")
    val correctAnswer =
      left.join(right, LeftOuter, Some("l.a".attr===3)).
        where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #5") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" === 3).subquery("l")
    val right = testRelation.where($"b" === 2).subquery("r")
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: can't push down") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y, condition = Some("x.b".attr === "y.b".attr))
    }
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(originalQuery.analyze, optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" === 1).subquery("x")
    val right = testRelation.where($"a" === 1).subquery("y")
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: conjunctive predicates #2") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" === 1).subquery("x")
    val right = testRelation.subquery("y")
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: conjunctive predicates #3") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val z = testRelation.subquery("z")

    val originalQuery = {
      z.join(x.join(y))
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) &&
          ("z.a".attr >= 3) && ("z.a".attr === "x.b".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val lleft = testRelation.where($"a" >= 3).subquery("z")
    val left = testRelation.where($"a" === 1).subquery("x")
    val right = testRelation.subquery("y")
    val correctAnswer =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down where clause into left anti join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery =
      x.join(y, LeftAnti, Some("x.b".attr === "y.b".attr))
        .where("x.a".attr > 10)
        .analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      x.where("x.a".attr > 10)
        .join(y, LeftAnti, Some("x.b".attr === "y.b".attr))
        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("joins: only push down join conditions to the right of a left anti join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery =
      x.join(y,
        LeftAnti,
        Some("x.b".attr === "y.b".attr && "y.a".attr > 10 && "x.a".attr > 10)).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      x.join(
        y.where("y.a".attr > 10),
        LeftAnti,
        Some("x.b".attr === "y.b".attr && "x.a".attr > 10))
        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("joins: only push down join conditions to the right of an existence join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val fillerVal = $"val".boolean
    val originalQuery =
      x.join(y,
        ExistenceJoin(fillerVal),
        Some("x.a".attr > 1 && "y.b".attr > 2)).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      x.join(
        y.where("y.b".attr > 2),
        ExistenceJoin(fillerVal),
        Some("x.a".attr > 1))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  val testRelationWithArrayType = LocalRelation($"a".int, $"b".int, $"c_arr".array(IntegerType))

  test("generate: predicate referenced no generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode($"c_arr"), alias = Some("arr"))
        .where(($"b" >= 5) && ($"a" > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = {
      testRelationWithArrayType
        .where(($"b" >= 5) && ($"a" > 6))
        .generate(Explode($"c_arr"), alias = Some("arr")).analyze
    }

    comparePlans(optimized, correctAnswer)
  }

  test("generate: non-deterministic predicate referenced no generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode($"c_arr"), alias = Some("arr"))
        .where(($"b" >= 5) && ($"a" + Rand(10).as("rnd") > 6) && ($"col" > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = {
      testRelationWithArrayType
        .where($"b" >= 5)
        .generate(Explode($"c_arr"), alias = Some("arr"))
        .where($"a" + Rand(10).as("rnd") > 6 && $"col" > 6)
        .analyze
    }

    comparePlans(optimized, correctAnswer)
  }

  test("generate: part of conjuncts referenced generated column") {
    val generator = Explode($"c_arr")
    val originalQuery = {
      testRelationWithArrayType
        .generate(generator, alias = Some("arr"), outputNames = Seq("c"))
        .where(($"b" >= 5) && ($"c" > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val referenceResult = {
      testRelationWithArrayType
        .where($"b" >= 5)
        .generate(generator, alias = Some("arr"), outputNames = Seq("c"))
        .where($"c" > 6).analyze
    }

    // Since newly generated columns get different ids every time being analyzed
    // e.g. comparePlans(originalQuery.analyze, originalQuery.analyze) fails.
    // So we check operators manually here.
    // Filter("c" > 6)
    assertResult(classOf[Filter])(optimized.getClass)
    assertResult(1)(optimized.asInstanceOf[Filter].condition.references.size)
    assertResult("c") {
      optimized.asInstanceOf[Filter].condition.references.toSeq(0).name
    }

    // the rest part
    comparePlans(optimized.children(0), referenceResult.children(0))
  }

  test("generate: all conjuncts referenced generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode($"c_arr"), alias = Some("arr"))
        .where(($"col" > 6) || ($"b" > 5)).analyze
    }
    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("aggregate: push down filter when filter on group by expression") {
    val originalQuery = testRelation
                        .groupBy($"a")($"a", count($"b") as "c")
                        .select($"a", $"c")
                        .where($"a" === 2)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
                        .where($"a" === 2)
                        .groupBy($"a")($"a", count($"b") as "c")
                        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push down filter when filter not on group by expression") {
    val originalQuery = testRelation
                        .select($"a", $"b")
                        .groupBy($"a")($"a", count($"b") as "c")
                        .where($"c" === 2L)

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, originalQuery.analyze)
  }

  test("aggregate: push down filters partially which are subset of group by expressions") {
    val originalQuery = testRelation
                        .select($"a", $"b")
                        .groupBy($"a")($"a", count($"b") as "c")
                        .where($"c" === 2L && $"a" === 3)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
                        .where($"a" === 3)
                        .select($"a", $"b")
                        .groupBy($"a")($"a", count($"b") as "c")
                        .where($"c" === 2L)
                        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: push down filters with alias") {
    val originalQuery = testRelation
      .select($"a", $"b")
      .groupBy($"a")(($"a" + 1) as "aa", count($"b") as "c")
      .where(($"c" === 2L || $"aa" > 4) && $"aa" < 3)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where($"a" + 1 < 3)
      .select($"a", $"b")
      .groupBy($"a")(($"a" + 1) as "aa", count($"b") as "c")
      .where($"c" === 2L || $"aa" > 4)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: push down filters with literal") {
    val originalQuery = testRelation
      .select($"a", $"b")
      .groupBy($"a")($"a", count($"b") as "c", "s" as "d")
      .where($"c" === 2L && $"d" === "s")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where("s" === "s")
      .select($"a", $"b")
      .groupBy($"a")($"a", count($"b") as "c", "s" as "d")
      .where($"c" === 2L)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push down filters that are nondeterministic") {
    val originalQuery = testRelation
      .select($"a", $"b")
      .groupBy($"a")($"a" + Rand(10) as "aa", count($"b") as "c",
        Rand(11).as("rnd"))
      .where($"c" === 2L && $"aa" + Rand(10).as("rnd") === 3 && $"rnd" === 5)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select($"a", $"b")
      .groupBy($"a")($"a" + Rand(10) as "aa", count($"b") as "c",
        Rand(11).as("rnd"))
      .where($"c" === 2L && $"aa" + Rand(10).as("rnd") === 3 && $"rnd" === 5)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-17712: aggregate: don't push down filters that are data-independent") {
    val originalQuery = LocalRelation.apply(testRelation.output, Seq.empty)
      .select($"a", $"b")
      .groupBy($"a")(count($"a"))
      .where(false)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select($"a", $"b")
      .groupBy($"a")(count($"a"))
      .where(false)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push filters if the aggregate has no grouping expressions") {
    val originalQuery = LocalRelation.apply(testRelation.output, Seq.empty)
      .select($"a", $"b")
      .groupBy()(count(1))
      .where(false)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32940: aggregate: push filters through first, last and collect") {
    Seq(
      first(_: Expression),
      last(_: Expression),
      collectList(_: Expression),
      collectSet(_: Expression)
    ).foreach { agg =>
      val originalQuery = testRelation
        .groupBy($"a")(agg($"b"))
        .where($"a" > 42)
        .analyze

      val optimized = Optimize.execute(originalQuery)

      val correctAnswer = testRelation
        .where($"a" > 42)
        .groupBy($"a")(agg($"b"))
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("union") {
    val testRelation2 = LocalRelation($"d".int, $"e".int, $"f".int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .where($"a" === 2L && $"b" + Rand(10).as("rnd") === 3 && $"c" > 5L)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelation.where($"a" === 2L && $"c" > 5L),
      testRelation2.where($"d" === 2L && $"f" > 5L)))
      .where($"b" + Rand(10).as("rnd") === 3)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("union filter pushdown w/reference to grand-child field") {
    val nonNullableArray = StructField("a", ArrayType(IntegerType, false))
    val bField = StructField("b", IntegerType)
    val testRelationNonNull = LocalRelation(nonNullableArray, bField)
    val testRelationNull = LocalRelation($"c".array(IntegerType), $"d".int)

    val nonNullArrayRef = AttributeReference("a", ArrayType(IntegerType, false))(
      testRelationNonNull.output(0).exprId, List())


    val originalQuery = Union(Seq(testRelationNonNull, testRelationNull))
      .where(IsNotNull(nonNullArrayRef))


    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelationNonNull.where(IsNotNull($"a")),
      testRelationNull.where(IsNotNull($"c"))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("expand") {
    val agg = testRelation
      .groupBy(Cube(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", sum($"c"))
      .analyze
      .asInstanceOf[Aggregate]

    val a = agg.output(0)
    val b = agg.output(1)

    val query = agg.where(a > 1 && b > 2)
    val optimized = Optimize.execute(query)
    val correctedAnswer = agg.copy(child = agg.child.where(a > 1 && b > 2)).analyze
    comparePlans(optimized, correctedAnswer)
  }

  test("predicate subquery: push down simple") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val z = LocalRelation($"a".int, $"b".int, $"c".int).subquery("z")

    val query = x
      .join(y, Inner, Option("x.a".attr === "y.a".attr))
      .where(Exists(z.where("x.a".attr === "z.a".attr)))
      .analyze
    val answer = x
      .where(Exists(z.where("x.a".attr === "z.a".attr)))
      .join(y, Inner, Option("x.a".attr === "y.a".attr))
      .analyze
    val optimized = Optimize.execute(Optimize.execute(query))
    comparePlans(optimized, answer)
  }

  test("predicate subquery: push down complex") {
    val w = testRelation.subquery("w")
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val z = LocalRelation($"a".int, $"b".int, $"c".int).subquery("z")

    val query = w
      .join(x, Inner, Option("w.a".attr === "x.a".attr))
      .join(y, LeftOuter, Option("x.a".attr === "y.a".attr))
      .where(Exists(z.where("w.a".attr === "z.a".attr)))
      .analyze
    val answer = w
      .where(Exists(z.where("w.a".attr === "z.a".attr)))
      .join(x, Inner, Option("w.a".attr === "x.a".attr))
      .join(y, LeftOuter, Option("x.a".attr === "y.a".attr))
      .analyze
    val optimized = Optimize.execute(Optimize.execute(query))
    comparePlans(optimized, answer)
  }

  test("SPARK-20094: don't push predicate with IN subquery into join condition") {
    val x = testRelation.subquery("x")
    val z = testRelation.subquery("z")
    val w = testRelation1.subquery("w")

    val queryPlan = x
      .join(z)
      .where(("x.b".attr === "z.b".attr) &&
        ("x.a".attr > 1 || "z.c".attr.in(ListQuery(w.select("w.d".attr)))))
      .analyze

    val expectedPlan = x
      .join(z, Inner, Some("x.b".attr === "z.b".attr))
      .where("x.a".attr > 1 || "z.c".attr.in(ListQuery(w.select("w.d".attr))))
      .analyze

    val optimized = Optimize.execute(queryPlan)
    comparePlans(optimized, expectedPlan)
  }

  test("Window: predicate push down -- basic") {
    val winExpr = windowExpr(count($"b"),
      windowSpec($"a" :: Nil, $"b".asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select($"a", $"b", $"c",
      winExpr.as("window")).where($"a" > 1)
    val correctAnswer = testRelation
      .where($"a" > 1).select($"a", $"b", $"c")
      .window(winExpr.as("window") :: Nil, $"a" :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- predicates with compound predicate using only one column") {
    val winExpr =
      windowExpr(count($"b"),
        windowSpec($"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select($"a", $"b", $"c",
      winExpr.as("window")).where($"a" * 3 > 15)
    val correctAnswer = testRelation
      .where($"a" * 3 > 15).select($"a", $"b", $"c")
      .window(winExpr.as("window") :: Nil, $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- multi window expressions with the same window spec") {
    val winSpec = windowSpec($"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count($"b"), winSpec)
    val winExpr2 = windowExpr(sum($"b"), winSpec)
    val originalQuery = testRelation
      .select($"a", $"b", $"c", winExpr1.as("window1"), winExpr2.as("window2"))
      .where($"a" > 1)

    val correctAnswer = testRelation
      .where($"a" > 1).select($"a", $"b", $"c")
      .window(winExpr1.as("window1") :: winExpr2.as("window2") :: Nil,
        $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window1", $"window2").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- multi window specification - 1") {
    // order by clauses are different between winSpec1 and winSpec2
    val winSpec1 = windowSpec($"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count($"b"), winSpec1)
    val winSpec2 = windowSpec($"a".attr :: $"b".attr :: Nil, $"a".asc :: Nil, UnspecifiedFrame)
    val winExpr2 = windowExpr(count($"b"), winSpec2)
    val originalQuery = testRelation
      .select($"a", $"b", $"c", winExpr1.as("window1"), winExpr2.as("window2"))
      .where($"a" > 1)

    val correctAnswer1 = testRelation
      .where($"a" > 1).select($"a", $"b", $"c")
      .window(winExpr1.as("window1") :: Nil, $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .window(winExpr2.as("window2") :: Nil, $"a".attr :: $"b".attr :: Nil, $"a".asc :: Nil)
      .select($"a", $"b", $"c", $"window1", $"window2").analyze

    val correctAnswer2 = testRelation
      .where($"a" > 1).select($"a", $"b", $"c")
      .window(winExpr2.as("window2") :: Nil, $"a".attr :: $"b".attr :: Nil, $"a".asc :: Nil)
      .window(winExpr1.as("window1") :: Nil, $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window1", $"window2").analyze

    // When Analyzer adding Window operators after grouping the extracted Window Expressions
    // based on their Partition and Order Specs, the order of Window operators is
    // non-deterministic. Thus, we have two correct plans
    val optimizedQuery = Optimize.execute(originalQuery.analyze)
    try {
      comparePlans(optimizedQuery, correctAnswer1)
    } catch {
      case ae: Throwable => comparePlans(optimizedQuery, correctAnswer2)
    }
  }

  test("Window: predicate push down -- multi window specification - 2") {
    // partitioning clauses are different between winSpec1 and winSpec2
    val winSpec1 = windowSpec($"a".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count($"b"), winSpec1)
    val winSpec2 = windowSpec($"b".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr2 = windowExpr(count($"a"), winSpec2)
    val originalQuery = testRelation
      .select($"a", winExpr1.as("window1"), $"b", $"c", winExpr2.as("window2"))
      .where($"b" > 1)

    val correctAnswer1 = testRelation.select($"a", $"b", $"c")
      .window(winExpr1.as("window1") :: Nil, $"a".attr :: Nil, $"b".asc :: Nil)
      .where($"b" > 1)
      .window(winExpr2.as("window2") :: Nil, $"b".attr :: Nil, $"b".asc :: Nil)
      .select($"a", $"window1", $"b", $"c", $"window2").analyze

    val correctAnswer2 = testRelation.select($"a", $"b", $"c")
      .window(winExpr2.as("window2") :: Nil, $"b".attr :: Nil, $"b".asc :: Nil)
      .window(winExpr1.as("window1") :: Nil, $"a".attr :: Nil, $"b".asc :: Nil)
      .where($"b" > 1)
      .select($"a", $"window1", $"b", $"c", $"window2").analyze

    val optimizedQuery = Optimize.execute(originalQuery.analyze)
    // When Analyzer adding Window operators after grouping the extracted Window Expressions
    // based on their Partition and Order Specs, the order of Window operators is
    // non-deterministic. Thus, we have two correct plans
    try {
      comparePlans(optimizedQuery, correctAnswer1)
    } catch {
      case ae: Throwable => comparePlans(optimizedQuery, correctAnswer2)
    }
  }

  test("Window: predicate push down -- predicates with multiple partitioning columns") {
    val winExpr =
      windowExpr(count($"b"),
        windowSpec($"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"a" + $"b" > 1)
    val correctAnswer = testRelation
      .where($"a" + $"b" > 1).select($"a", $"b", $"c")
      .window(winExpr.as("window") :: Nil, $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  // complex predicates with the same references but the same expressions
  // Todo: in Analyzer, to enable it, we need to convert the expression in conditions
  // to the alias that is defined as the same expression
  ignore("Window: predicate push down -- complex predicate with the same expressions") {
    val winSpec = windowSpec(
      partitionSpec = $"a".attr + $"b".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = $"_w0".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count($"b"), winSpecAnalyzed)

    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"a" + $"b" > 1)
    val correctAnswer = testRelation
      .where($"a" + $"b" > 1).select($"a", $"b", $"c", ($"a" + $"b").as("_w0"))
      .window(winExprAnalyzed.as("window") :: Nil, $"_w0" :: Nil, $"b".asc :: Nil)
      .select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- predicates are not from partitioning keys") {
    val winSpec = windowSpec(
      partitionSpec = $"a".attr :: $"b".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    // No push down: the predicate is c > 1, but the partitioning key is (a, b).
    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"c" > 1)
    val correctAnswer = testRelation.select($"a", $"b", $"c")
      .window(winExpr.as("window") :: Nil, $"a".attr :: $"b".attr :: Nil, $"b".asc :: Nil)
      .where($"c" > 1).select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- partial compound partition key") {
    val winSpec = windowSpec(
      partitionSpec = $"a".attr + $"b".attr :: $"b".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    // No push down: the predicate is a > 1, but the partitioning key is (a + b, b)
    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"a" > 1)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = $"_w0".attr :: $"b".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count($"b"), winSpecAnalyzed)
    val correctAnswer = testRelation.select($"a", $"b", $"c", ($"a" + $"b").as("_w0"))
      .window(
        winExprAnalyzed.as("window") :: Nil, $"_w0" :: $"b".attr :: Nil, $"b".asc :: Nil)
      .where($"a" > 1).select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- complex predicates containing non partitioning columns") {
    val winSpec =
      windowSpec(partitionSpec = $"b".attr :: Nil, orderSpec = $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    // No push down: the predicate is a + b > 1, but the partitioning key is b.
    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"a" + $"b" > 1)
    val correctAnswer = testRelation
      .select($"a", $"b", $"c")
      .window(winExpr.as("window") :: Nil, $"b".attr :: Nil, $"b".asc :: Nil)
      .where($"a" + $"b" > 1).select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  // complex predicates with the same references but different expressions
  test("Window: no predicate push down -- complex predicate with different expressions") {
    val winSpec = windowSpec(
      partitionSpec = $"a".attr + $"b".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = $"_w0".attr :: Nil,
      orderSpec = $"b".asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count($"b"), winSpecAnalyzed)

    // No push down: the predicate is a + b > 1, but the partitioning key is a + b.
    val originalQuery = testRelation.select($"a", $"b", $"c", winExpr.as("window"))
      .where($"a" - $"b" > 1)
    val correctAnswer = testRelation.select($"a", $"b", $"c", ($"a" + $"b").as("_w0"))
      .window(winExprAnalyzed.as("window") :: Nil, $"_w0" :: Nil, $"b".asc :: Nil)
      .where($"a" - $"b" > 1).select($"a", $"b", $"c", $"window").analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("watermark pushdown: no pushdown on watermark attribute #1") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(Seq(attrA, $"b".timestamp, attrC), Nil, isStreaming = true)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark($"b", interval, relation)
      .where($"a" === 5 && $"b" === new java.sql.Timestamp(0) && $"c" === 5)
    val correctAnswer = EventTimeWatermark(
      $"b", interval, relation.where($"a" === 5 && $"c" === 5))
      .where($"b" === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("watermark pushdown: no pushdown for nondeterministic filter") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(Seq(attrA, attrB, $"c".timestamp), Nil, isStreaming = true)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark($"c", interval, relation)
      .where($"a" === 5 && $"b" === Rand(10) && $"c" === new java.sql.Timestamp(0))
    val correctAnswer = EventTimeWatermark(
      $"c", interval, relation.where($"a" === 5))
      .where($"b" === Rand(10) && $"c" === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("watermark pushdown: full pushdown") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(Seq(attrA, attrB, $"c".timestamp), Nil, isStreaming = true)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark($"c", interval, relation)
      .where($"a" === 5 && $"b" === 10)
    val correctAnswer = EventTimeWatermark(
      $"c", interval, relation.where($"a" === 5 && $"b" === 10))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("watermark pushdown: no pushdown on watermark attribute #2") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(Seq($"a".timestamp, attrB, attrC), Nil, isStreaming = true)

    val originalQuery = EventTimeWatermark($"a", interval, relation)
      .where($"a" === new java.sql.Timestamp(0) && $"b" === 10)
    val correctAnswer = EventTimeWatermark(
      $"a", interval, relation.where($"b" === 10)).where($"a" === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("push down predicate through expand") {
    val query =
        Filter($"a" > 1,
          Expand(
            Seq(
              Seq($"a", $"b", $"c", Literal.create(null, StringType), 1),
              Seq($"a", $"b", $"c", $"a", 2)),
            Seq($"a", $"b", $"c"),
            testRelation)).analyze
    val optimized = Optimize.execute(query)

    val expected =
        Expand(
          Seq(
            Seq($"a", $"b", $"c", Literal.create(null, StringType), 1),
            Seq($"a", $"b", $"c", $"a", 2)),
          Seq($"a", $"b", $"c"),
          Filter($"a" > 1, testRelation)).analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-28345: PythonUDF predicate should be able to pushdown to join") {
    val pythonUDFJoinCond = {
      val pythonUDF = PythonUDF("pythonUDF", null,
        IntegerType,
        Seq(attrA),
        PythonEvalType.SQL_BATCHED_UDF,
        udfDeterministic = true)
      pythonUDF === attrD
    }

    val query = testRelation.join(
      testRelation1,
      joinType = Cross).where(pythonUDFJoinCond)

    val expected = testRelation.join(
      testRelation1,
      joinType = Cross,
      condition = Some(pythonUDFJoinCond)).analyze

    comparePlans(Optimize.execute(query.analyze), expected)
  }

  test("push down filter predicates through inner join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery = x.join(y).where(("x.b".attr === "y.b".attr) && (simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, expectedPredicatePushDownResult)
  }

  test("push down join predicates through inner join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr) && (simpleDisjunctivePredicate)))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, expectedPredicatePushDownResult)
  }

  test("push down complex predicates through inner join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val joinCondition = (("x.b".attr === "y.b".attr)
      && ((("x.a".attr === 5) && ("y.a".attr >= 2) && ("y.a".attr <= 3))
      || (("x.a".attr === 2) && ("y.a".attr >= 1) && ("y.a".attr <= 14))
      || (("x.a".attr === 1) && ("y.a".attr >= 9) && ("y.a".attr <= 27))))

    val originalQuery = x.join(y, condition = Some(joinCondition))
    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(
      ($"a" === 5 || $"a" === 2 || $"a" === 1)).subquery("x")
    val right = testRelation.where(
      ($"a" >= 2 && $"a" <= 3) || ($"a" >= 1 && $"a" <= 14) || ($"a" >= 9 && $"a" <= 27))
      .subquery("y")
    val correctAnswer = left.join(right, condition = Some(joinCondition)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates(with NOT predicate) through inner join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr)
        && Not(("x.a".attr > 3)
        && ("x.a".attr < 2 || ("y.a".attr > 13)) || ("x.a".attr > 1) && ("y.a".attr > 11))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" <= 3 || $"a" >= 2).subquery("x")
    val right = testRelation.subquery("y")
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr <= 3) || (("x.a".attr >= 2) && ("y.a".attr <= 13)))
        && (("x.a".attr <= 1) || ("y.a".attr <= 11))))
        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates through left join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, joinType = LeftOuter, condition = Some(("x.b".attr === "y.b".attr)
        && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery("x")
    val right = testRelation.where($"a" > 13 || $"a" > 11).subquery("y")
    val correctAnswer =
      left.join(right, joinType = LeftOuter, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates through right join") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, joinType = RightOuter, condition = Some(("x.b".attr === "y.b".attr)
        && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" > 3 || $"a" > 1).subquery("x")
    val right = testRelation.subquery("y")
    val correctAnswer =
      left.join(right, joinType = RightOuter, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32302: avoid generating too many predicates") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr) && ((("x.a".attr > 3) &&
        ("x.a".attr < 13) && ("y.c".attr <= 5)) || (("y.a".attr > 2) && ("y.c".attr < 1)))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery("x")
    val right = testRelation.where($"c" <= 5 || ($"a" > 2 && $"c" < 1)).subquery("y")
    val correctAnswer = left.join(right, condition = Some("x.b".attr === "y.b".attr &&
      ((("x.a".attr > 3) && ("x.a".attr < 13) && ("y.c".attr <= 5)) ||
        (("y.a".attr > 2) && ("y.c".attr < 1))))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicate through multiple joins") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val z = testRelation.subquery("z")
    val xJoinY = x.join(y, condition = Some("x.b".attr === "y.b".attr))
    val originalQuery = z.join(xJoinY,
      condition = Some("x.a".attr === "z.a".attr && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = x.where($"a" > 3 || $"a" > 1)
    val right = y.where($"a" > 13 || $"a" > 11)
    val correctAnswer = z.join(left.join(right,
      condition = Some("x.b".attr === "y.b".attr && simpleDisjunctivePredicate)),
      condition = Some("x.a".attr === "z.a".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-37828: Push down filters through RebalancePartitions") {
    val originalQuery = RebalancePartitions(Seq.empty, testRelation).where($"a" > 3)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = RebalancePartitions(Seq.empty, testRelation.where($"a" > 3)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-46707: push down predicate with sequence (without step) through joins") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    // do not push down when sequence has step param
    val queryWithStep = x.join(y, joinType = Inner, condition = Some($"x.c" === $"y.d"))
      .where(IsNotNull(Sequence($"x.a", $"x.b", Some(Literal(1)))))
      .analyze
    val optimizedQueryWithStep = Optimize.execute(queryWithStep)
    comparePlans(optimizedQueryWithStep, queryWithStep)

    // push down when sequence does not have step param
    val queryWithoutStep = x.join(y, joinType = Inner, condition = Some($"x.c" === $"y.d"))
      .where(IsNotNull(Sequence($"x.a", $"x.b", None)))
      .analyze
    val optimizedQueryWithoutStep = Optimize.execute(queryWithoutStep)
    val correctAnswer = x.where(IsNotNull(Sequence($"x.a", $"x.b", None)))
      .join(y, joinType = Inner, condition = Some($"x.c" === $"y.d"))
      .analyze
    comparePlans(optimizedQueryWithoutStep, correctAnswer)
  }

  test("SPARK-46707: push down predicate with sequence (without step) through aggregates") {
    val x = testRelation.subquery("x")

    // Always push down sequence as it's deterministic
    val queryWithStep = x.groupBy($"x.a", $"x.b")($"x.a", $"x.b")
      .where(IsNotNull(Sequence($"x.a", $"x.b", Some(Literal(1)))))
      .analyze
    val optimizedQueryWithStep = Optimize.execute(queryWithStep)
    val correctAnswerWithStep = x.where(IsNotNull(Sequence($"x.a", $"x.b", Some(Literal(1)))))
      .groupBy($"x.a", $"x.b")($"x.a", $"x.b")
      .analyze
    comparePlans(optimizedQueryWithStep, correctAnswerWithStep)

    val queryWithoutStep = x.groupBy($"x.a", $"x.b")($"x.a", $"x.b")
      .where(IsNotNull(Sequence($"x.a", $"x.b", None)))
      .analyze
    val optimizedQueryWithoutStep = Optimize.execute(queryWithoutStep)
    val correctAnswer = x.where(IsNotNull(Sequence($"x.a", $"x.b", None)))
      .groupBy($"x.a", $"x.b")($"x.a", $"x.b")
      .analyze
    comparePlans(optimizedQueryWithoutStep, correctAnswer)
  }

  test("SPARK-46707: combine predicate with sequence (without step) with other filters") {
    val x = testRelation.subquery("x")

    // do not combine when sequence has step param
    val queryWithStep = x.where($"x.c" > 1)
      .where(IsNotNull(Sequence($"x.a", $"x.b", Some(Literal(1)))))
      .analyze
    val optimizedQueryWithStep = Optimize.execute(queryWithStep)
    comparePlans(optimizedQueryWithStep, queryWithStep)

    // combine when sequence does not have step param
    val queryWithoutStep = x.where($"x.c" > 1)
      .where(IsNotNull(Sequence($"x.a", $"x.b", None)))
      .analyze
    val optimizedQueryWithoutStep = Optimize.execute(queryWithoutStep)
    val correctAnswer = x.where(IsNotNull(Sequence($"x.a", $"x.b", None)) && $"x.c" > 1)
      .analyze
    comparePlans(optimizedQueryWithoutStep, correctAnswer)
  }
}
