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
import org.apache.spark.sql.types.{IntegerType, StringType}
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

  val attrA = Symbol("a").int
  val attrB = Symbol("b").int
  val attrC = Symbol("c").int
  val attrD = Symbol("d").int

  val testRelation = LocalRelation(attrA, attrB, attrC)

  val testRelation1 = LocalRelation(attrD)

  val simpleDisjunctivePredicate =
    ("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11)
  val expectedPredicatePushDownResult = {
    val left = testRelation.where((Symbol("a") > 3 || Symbol("a") > 1)).subquery(Symbol("x"))
    val right = testRelation.where(Symbol("a") > 13 || Symbol("a") > 11).subquery(Symbol("y"))
    left.join(right, condition = Some("x.b".attr === "y.b".attr
      && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11)))).analyze
  }

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery(Symbol("y"))
        .select(Symbol("a"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Symbol("a").attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  // After this line is unimplemented.
  test("simple push down") {
    val originalQuery =
      testRelation
        .select(Symbol("a"))
        .where(Symbol("a") === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Symbol("a") === 1)
        .select(Symbol("a"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combine redundant filters") {
    val originalQuery =
      testRelation
        .where(Symbol("a") === 1 && Symbol("b") === 1)
        .where(Symbol("a") === 1 && Symbol("c") === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Symbol("a") === 1 && Symbol("b") === 1 && Symbol("c") === 1)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not combine non-deterministic filters even if they are identical") {
    val originalQuery =
      testRelation
        .where(Rand(0) > 0.1 && Symbol("a") === 1)
        .where(Rand(0) > 0.1 && Symbol("a") === 1).analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("SPARK-16164: Filter pushdown should keep the ordering in the logical plan") {
    val originalQuery =
      testRelation
        .where(Symbol("a") === 1)
        .select(Symbol("a"), Symbol("b"))
        .where(Symbol("b") === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Symbol("a") === 1 && Symbol("b") === 1)
        .select(Symbol("a"), Symbol("b"))
        .analyze

    // We can not use comparePlans here because it normalized the plan.
    assert(optimized == correctAnswer)
  }

  test("SPARK-16994: filter should not be pushed through limit") {
    val originalQuery = testRelation.limit(10).where(Symbol("a") === 1).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("can't push without rewrite") {
    val originalQuery =
      testRelation
        .select(Symbol("a") + Symbol("b") as Symbol("e"))
        .where(Symbol("e") === 1)
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Symbol("a") + Symbol("b") === 1)
        .select(Symbol("a") + Symbol("b") as Symbol("e"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: can always push down filter through project with deterministic field") {
    val originalQuery = testRelation
      .select(Symbol("a"))
      .where(Rand(10) > 5 || Symbol("a") > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    val correctAnswer = testRelation
      .where(Rand(10) > 5 || Symbol("a") > 5)
      .select(Symbol("a"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nondeterministic: can't push down filter through project with nondeterministic field") {
    val originalQuery = testRelation
      .select(Rand(10).as(Symbol("rand")), Symbol("a"))
      .where(Symbol("a") > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("nondeterministic: can't push down filter through aggregate with nondeterministic field") {
    val originalQuery = testRelation
      .groupBy(Symbol("a"))(Symbol("a"), Rand(10).as(Symbol("rand")))
      .where(Symbol("a") > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("nondeterministic: push down part of filter through aggregate with deterministic field") {
    val originalQuery = testRelation
      .groupBy(Symbol("a"))(Symbol("a"))
      .where(Symbol("a") > 5 && Rand(10) > 5)
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where(Symbol("a") > 5)
      .groupBy(Symbol("a"))(Symbol("a"))
      .where(Rand(10) > 5)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filters: combines filters") {
    val originalQuery = testRelation
      .select(Symbol("a"))
      .where(Symbol("a") === 1)
      .where(Symbol("a") === 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Symbol("a") === 1 && Symbol("a") === 2)
        .select(Symbol("a")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to either side") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
        .where("y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 1)
    val right = testRelation.where(Symbol("b") === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to one side") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 1)
    val right = testRelation
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: do not push down non-deterministic filters into join condition") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation1.subquery(Symbol("y"))

    val originalQuery = x.join(y).where(Rand(10) > 5.0).analyze
    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("joins: push to one side after transformCondition") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation1.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
       .where(("x.a".attr === 1 && "y.d".attr === "x.b".attr) ||
              ("x.a".attr === 1 && "y.d".attr === "x.c".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") === 1)
    val right = testRelation1
    val correctAnswer =
      left.join(right, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: rewrite filter to push to either side") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 1)
    val right = testRelation.where(Symbol("b") === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left semi join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation1.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftSemi, Option("x.a".attr === "y.d".attr && "x.b".attr >= 1 && "y.d".attr >= 2))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") >= 1)
    val right = testRelation1.where(Symbol("d") >= 2)
    val correctAnswer =
      left.join(right, LeftSemi, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #1") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 1)
    val correctAnswer =
      left.join(y, LeftOuter).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #1") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, RightOuter)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where(Symbol("b") === 2).subquery(Symbol("d"))
    val correctAnswer =
      x.join(right, RightOuter).where("x.b".attr === 1).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #2") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftOuter, Some("x.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 2).subquery(Symbol("d"))
    val correctAnswer =
      left.join(y, LeftOuter, Some("d.b".attr === 1)).where("y.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #2") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where(Symbol("b") === 2).subquery(Symbol("d"))
    val correctAnswer =
      x.join(right, RightOuter, Some("d.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #3") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 2).subquery(Symbol("l"))
    val right = testRelation.where(Symbol("b") === 1).subquery(Symbol("r"))
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #3") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val right = testRelation.where(Symbol("b") === 2).subquery(Symbol("r"))
    val correctAnswer =
      x.join(right, RightOuter, Some("r.b".attr === 1)).where("x.b".attr === 2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #4") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 2).subquery(Symbol("l"))
    val right = testRelation.where(Symbol("b") === 1).subquery(Symbol("r"))
    val correctAnswer =
      left.join(right, LeftOuter).where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #4") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery(Symbol("l"))
    val right = testRelation.where(Symbol("b") === 2).subquery(Symbol("r"))
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down left outer join #5") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, LeftOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("b") === 2).subquery(Symbol("l"))
    val right = testRelation.where(Symbol("b") === 1).subquery(Symbol("r"))
    val correctAnswer =
      left.join(right, LeftOuter, Some("l.a".attr===3)).
        where("r.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down right outer join #5") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, RightOuter, Some("y.b".attr === 1 && "x.a".attr === 3))
        .where("x.b".attr === 2 && "y.b".attr === 2 && "x.c".attr === "y.c".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") === 3).subquery(Symbol("l"))
    val right = testRelation.where(Symbol("b") === 2).subquery(Symbol("r"))
    val correctAnswer =
      left.join(right, RightOuter, Some("r.b".attr === 1)).
        where("l.b".attr === 2 && "l.c".attr === "r.c".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: can't push down") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y, condition = Some("x.b".attr === "y.b".attr))
    }
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(originalQuery.analyze, optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") === 1).subquery(Symbol("x"))
    val right = testRelation.where(Symbol("a") === 1).subquery(Symbol("y"))
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: conjunctive predicates #2") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") === 1).subquery(Symbol("x"))
    val right = testRelation.subquery(Symbol("y"))
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: conjunctive predicates #3") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
    val z = testRelation.subquery(Symbol("z"))

    val originalQuery = {
      z.join(x.join(y))
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) &&
          ("z.a".attr >= 3) && ("z.a".attr === "x.b".attr))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val lleft = testRelation.where(Symbol("a") >= 3).subquery(Symbol("z"))
    val left = testRelation.where(Symbol("a") === 1).subquery(Symbol("x"))
    val right = testRelation.subquery(Symbol("y"))
    val correctAnswer =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push down where clause into left anti join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
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
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
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
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
    val fillerVal = Symbol("val").boolean
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

  val testRelationWithArrayType =
    LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c_arr").array(IntegerType))

  test("generate: predicate referenced no generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode(Symbol("c_arr")), alias = Some("arr"))
        .where((Symbol("b") >= 5) && (Symbol("a") > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = {
      testRelationWithArrayType
        .where((Symbol("b") >= 5) && (Symbol("a") > 6))
        .generate(Explode(Symbol("c_arr")), alias = Some("arr")).analyze
    }

    comparePlans(optimized, correctAnswer)
  }

  test("generate: non-deterministic predicate referenced no generated column") {
    val originalQuery = {
      testRelationWithArrayType
        .generate(Explode(Symbol("c_arr")), alias = Some("arr"))
        .where((Symbol("b") >= 5) && (Symbol("a") + Rand(10).as("rnd") > 6) && (Symbol("col") > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = {
      testRelationWithArrayType
        .where(Symbol("b") >= 5)
        .generate(Explode(Symbol("c_arr")), alias = Some("arr"))
        .where(Symbol("a") + Rand(10).as("rnd") > 6 && Symbol("col") > 6)
        .analyze
    }

    comparePlans(optimized, correctAnswer)
  }

  test("generate: part of conjuncts referenced generated column") {
    val generator = Explode(Symbol("c_arr"))
    val originalQuery = {
      testRelationWithArrayType
        .generate(generator, alias = Some("arr"), outputNames = Seq("c"))
        .where((Symbol("b") >= 5) && (Symbol("c") > 6))
    }
    val optimized = Optimize.execute(originalQuery.analyze)
    val referenceResult = {
      testRelationWithArrayType
        .where(Symbol("b") >= 5)
        .generate(generator, alias = Some("arr"), outputNames = Seq("c"))
        .where(Symbol("c") > 6).analyze
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
        .generate(Explode(Symbol("c_arr")), alias = Some("arr"))
        .where((Symbol("col") > 6) || (Symbol("b") > 5)).analyze
    }
    val optimized = Optimize.execute(originalQuery)

    comparePlans(optimized, originalQuery)
  }

  test("aggregate: push down filter when filter on group by expression") {
    val originalQuery = testRelation
                        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"))
                        .select(Symbol("a"), Symbol("c"))
                        .where(Symbol("a") === 2)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
                        .where(Symbol("a") === 2)
                        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"))
                        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push down filter when filter not on group by expression") {
    val originalQuery = testRelation
                        .select(Symbol("a"), Symbol("b"))
                        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"))
                        .where(Symbol("c") === 2L)

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, originalQuery.analyze)
  }

  test("aggregate: push down filters partially which are subset of group by expressions") {
    val originalQuery = testRelation
                        .select(Symbol("a"), Symbol("b"))
                        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"))
                        .where(Symbol("c") === 2L && Symbol("a") === 3)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
                        .where(Symbol("a") === 3)
                        .select(Symbol("a"), Symbol("b"))
                        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"))
                        .where(Symbol("c") === 2L)
                        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: push down filters with alias") {
    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))((Symbol("a") + 1) as Symbol("aa"), count(Symbol("b")) as Symbol("c"))
      .where((Symbol("c") === 2L || Symbol("aa") > 4) && Symbol("aa") < 3)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where(Symbol("a") + 1 < 3)
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))((Symbol("a") + 1) as Symbol("aa"), count(Symbol("b")) as Symbol("c"))
      .where(Symbol("c") === 2L || Symbol("aa") > 4)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: push down filters with literal") {
    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"), "s" as Symbol("d"))
      .where(Symbol("c") === 2L && Symbol("d") === "s")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .where("s" === "s")
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")) as Symbol("c"), "s" as Symbol("d"))
      .where(Symbol("c") === 2L)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push down filters that are nondeterministic") {
    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(Symbol("a") + Rand(10) as Symbol("aa"),
        count(Symbol("b")) as Symbol("c"), Rand(11).as("rnd"))
      .where(Symbol("c") === 2L && Symbol("aa") + Rand(10).as("rnd") === 3 && Symbol("rnd") === 5)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(Symbol("a") + Rand(10) as Symbol("aa"),
        count(Symbol("b")) as Symbol("c"), Rand(11).as("rnd"))
      .where(Symbol("c") === 2L && Symbol("aa") + Rand(10).as("rnd") === 3 && Symbol("rnd") === 5)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-17712: aggregate: don't push down filters that are data-independent") {
    val originalQuery = LocalRelation.apply(testRelation.output, Seq.empty)
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(count(Symbol("a")))
      .where(false)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select(Symbol("a"), Symbol("b"))
      .groupBy(Symbol("a"))(count(Symbol("a")))
      .where(false)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("aggregate: don't push filters if the aggregate has no grouping expressions") {
    val originalQuery = LocalRelation.apply(testRelation.output, Seq.empty)
      .select(Symbol("a"), Symbol("b"))
      .groupBy()(count(1))
      .where(false)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("union") {
    val testRelation2 = LocalRelation(Symbol("d").int, Symbol("e").int, Symbol("f").int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .where(Symbol("a") === 2L && Symbol("b") + Rand(10).as("rnd") === 3 && Symbol("c") > 5L)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelation.where(Symbol("a") === 2L && Symbol("c") > 5L),
      testRelation2.where(Symbol("d") === 2L && Symbol("f") > 5L)))
      .where(Symbol("b") + Rand(10).as("rnd") === 3)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("expand") {
    val agg = testRelation
      .groupBy(Cube(Seq(Symbol("a"), Symbol("b"))))(Symbol("a"), Symbol("b"), sum(Symbol("c")))
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
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
    val z = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int).subquery(Symbol("z"))

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
    val w = testRelation.subquery(Symbol("w"))
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
    val z = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int).subquery(Symbol("z"))

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
    val x = testRelation.subquery(Symbol("x"))
    val z = testRelation.subquery(Symbol("z"))
    val w = testRelation1.subquery(Symbol("w"))

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
    val winExpr = windowExpr(count(Symbol("b")),
      windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") > 1)
    val correctAnswer = testRelation
      .where(Symbol("a") > 1).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil, Symbol("a") :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- predicates with compound predicate using only one column") {
    val winExpr =
      windowExpr(count(Symbol("b")), windowSpec(Symbol("a").attr :: Symbol("b").attr :: Nil,
        Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") * 3 > 15)
    val correctAnswer = testRelation
      .where(Symbol("a") * 3 > 15).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- multi window expressions with the same window spec") {
    val winSpec = windowSpec(Symbol("a").attr :: Symbol("b").attr :: Nil,
      Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count(Symbol("b")), winSpec)
    val winExpr2 = windowExpr(sum(Symbol("b")), winSpec)
    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"), winExpr1.as(Symbol("window1")),
        winExpr2.as(Symbol("window2"))).where(Symbol("a") > 1)

    val correctAnswer = testRelation
      .where(Symbol("a") > 1).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr1.as(Symbol("window1")) :: winExpr2.as(Symbol("window2")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window1"), Symbol("window2")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: predicate push down -- multi window specification - 1") {
    // order by clauses are different between winSpec1 and winSpec2
    val winSpec1 = windowSpec(Symbol("a").attr :: Symbol("b").attr :: Nil,
      Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count(Symbol("b")), winSpec1)
    val winSpec2 = windowSpec(Symbol("a").attr :: Symbol("b").attr :: Nil,
      Symbol("a").asc :: Nil, UnspecifiedFrame)
    val winExpr2 = windowExpr(count(Symbol("b")), winSpec2)
    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"), winExpr1.as(Symbol("window1")),
        winExpr2.as(Symbol("window2"))).where(Symbol("a") > 1)

    val correctAnswer1 = testRelation
      .where(Symbol("a") > 1).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr1.as(Symbol("window1")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .window(winExpr2.as(Symbol("window2")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("a").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window1"), Symbol("window2")).analyze

    val correctAnswer2 = testRelation
      .where(Symbol("a") > 1).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr2.as(Symbol("window2")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("a").asc :: Nil)
      .window(winExpr1.as(Symbol("window1")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window1"), Symbol("window2")).analyze

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
    val winSpec1 = windowSpec(Symbol("a").attr :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr1 = windowExpr(count(Symbol("b")), winSpec1)
    val winSpec2 = windowSpec(Symbol("b").attr :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr2 = windowExpr(count(Symbol("a")), winSpec2)
    val originalQuery = testRelation
      .select(Symbol("a"), winExpr1.as(Symbol("window1")),
        Symbol("b"), Symbol("c"), winExpr2.as(Symbol("window2"))).where(Symbol("b") > 1)

    val correctAnswer1 = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr1.as(Symbol("window1")) :: Nil,
        Symbol("a").attr :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("b") > 1)
      .window(winExpr2.as(Symbol("window2")) :: Nil,
        Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("window1"), Symbol("b"), Symbol("c"), Symbol("window2")).analyze

    val correctAnswer2 = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr2.as(Symbol("window2")) :: Nil,
        Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .window(winExpr1.as(Symbol("window1")) :: Nil,
        Symbol("a").attr :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("b") > 1)
      .select(Symbol("a"), Symbol("window1"), Symbol("b"), Symbol("c"), Symbol("window2")).analyze

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
      windowExpr(count(Symbol("b")), windowSpec(Symbol("a").attr :: Symbol("b").attr :: Nil,
        Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") + Symbol("b") > 1)
    val correctAnswer = testRelation
      .where(Symbol("a") + Symbol("b") > 1).select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  // complex predicates with the same references but the same expressions
  // Todo: in Analyzer, to enable it, we need to convert the expression in conditions
  // to the alias that is defined as the same expression
  ignore("Window: predicate push down -- complex predicate with the same expressions") {
    val winSpec = windowSpec(
      partitionSpec = Symbol("a").attr + Symbol("b").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = Symbol("_w0").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count(Symbol("b")), winSpecAnalyzed)

    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") + Symbol("b") > 1)
    val correctAnswer = testRelation
      .where(Symbol("a") + Symbol("b") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), (Symbol("a") + Symbol("b")).as("_w0"))
      .window(winExprAnalyzed.as(Symbol("window")) :: Nil,
        Symbol("_w0") :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- predicates are not from partitioning keys") {
    val winSpec = windowSpec(
      partitionSpec = Symbol("a").attr :: Symbol("b").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    // No push down: the predicate is c > 1, but the partitioning key is (a, b).
    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("c") > 1)
    val correctAnswer = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil,
        Symbol("a").attr :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("c") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- partial compound partition key") {
    val winSpec = windowSpec(
      partitionSpec = Symbol("a").attr + Symbol("b").attr :: Symbol("b").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    // No push down: the predicate is a > 1, but the partitioning key is (a + b, b)
    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") > 1)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = Symbol("_w0").attr :: Symbol("b").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count(Symbol("b")), winSpecAnalyzed)
    val correctAnswer = testRelation.select(
      Symbol("a"), Symbol("b"), Symbol("c"), (Symbol("a") + Symbol("b")).as("_w0"))
      .window(winExprAnalyzed.as(Symbol("window")) :: Nil,
        Symbol("_w0") :: Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("a") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Window: no predicate push down -- complex predicates containing non partitioning columns") {
    val winSpec =
      windowSpec(partitionSpec = Symbol("b").attr :: Nil,
        orderSpec = Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    // No push down: the predicate is a + b > 1, but the partitioning key is b.
    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"),
      Symbol("c"), winExpr.as(Symbol("window"))).where(Symbol("a") + Symbol("b") > 1)
    val correctAnswer = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil, Symbol("b").attr :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("a") + Symbol("b") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  // complex predicates with the same references but different expressions
  test("Window: no predicate push down -- complex predicate with different expressions") {
    val winSpec = windowSpec(
      partitionSpec = Symbol("a").attr + Symbol("b").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    val winSpecAnalyzed = windowSpec(
      partitionSpec = Symbol("_w0").attr :: Nil,
      orderSpec = Symbol("b").asc :: Nil,
      UnspecifiedFrame)
    val winExprAnalyzed = windowExpr(count(Symbol("b")), winSpecAnalyzed)

    // No push down: the predicate is a + b > 1, but the partitioning key is a + b.
    val originalQuery = testRelation.select(Symbol("a"), Symbol("b"), Symbol("c"),
      winExpr.as(Symbol("window"))).where(Symbol("a") - Symbol("b") > 1)
    val correctAnswer = testRelation.select(Symbol("a"), Symbol("b"),
      Symbol("c"), (Symbol("a") + Symbol("b")).as("_w0"))
      .window(winExprAnalyzed.as(Symbol("window")) :: Nil,
        Symbol("_w0") :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("a") - Symbol("b") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("watermark pushdown: no pushdown on watermark attribute #1") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(attrA, Symbol("b").timestamp, attrC)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark(Symbol("b"), interval, relation)
      .where(Symbol("a") === 5 && Symbol("b") === new java.sql.Timestamp(0) && Symbol("c") === 5)
    val correctAnswer = EventTimeWatermark(
      Symbol("b"), interval, relation.where(Symbol("a") === 5 && Symbol("c") === 5))
      .where(Symbol("b") === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("watermark pushdown: no pushdown for nondeterministic filter") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(attrA, attrB, Symbol("c").timestamp)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark(Symbol("c"), interval, relation)
      .where(Symbol("a") === 5 && Symbol("b") === Rand(10) &&
        Symbol("c") === new java.sql.Timestamp(0))
    val correctAnswer = EventTimeWatermark(
      Symbol("c"), interval, relation.where(Symbol("a") === 5))
      .where(Symbol("b") === Rand(10) && Symbol("c") === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("watermark pushdown: full pushdown") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(attrA, attrB, Symbol("c").timestamp)

    // Verify that all conditions except the watermark touching condition are pushed down
    // by the optimizer and others are not.
    val originalQuery = EventTimeWatermark(Symbol("c"), interval, relation)
      .where(Symbol("a") === 5 && Symbol("b") === 10)
    val correctAnswer = EventTimeWatermark(
      Symbol("c"), interval, relation.where(Symbol("a") === 5 && Symbol("b") === 10))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("watermark pushdown: no pushdown on watermark attribute #2") {
    val interval = new CalendarInterval(2, 2, 2000L)
    val relation = LocalRelation(Symbol("a").timestamp, attrB, attrC)

    val originalQuery = EventTimeWatermark(Symbol("a"), interval, relation)
      .where(Symbol("a") === new java.sql.Timestamp(0) && Symbol("b") === 10)
    val correctAnswer = EventTimeWatermark(
      Symbol("a"), interval, relation.where(Symbol("b") === 10))
      .where(Symbol("a") === new java.sql.Timestamp(0))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze,
      checkAnalysis = false)
  }

  test("push down predicate through expand") {
    val query =
        Filter(Symbol("a") > 1,
          Expand(
            Seq(
              Seq(Symbol("a"), Symbol("b"), Symbol("c"), Literal.create(null, StringType), 1),
              Seq(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("a"), 2)),
            Seq(Symbol("a"), Symbol("b"), Symbol("c")),
            testRelation)).analyze
    val optimized = Optimize.execute(query)

    val expected =
        Expand(
          Seq(
            Seq(Symbol("a"), Symbol("b"), Symbol("c"), Literal.create(null, StringType), 1),
            Seq(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("a"), 2)),
          Seq(Symbol("a"), Symbol("b"), Symbol("c")),
          Filter(Symbol("a") > 1, testRelation)).analyze

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
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y).where(("x.b".attr === "y.b".attr) && (simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, expectedPredicatePushDownResult)
  }

  test("push down join predicates through inner join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr) && (simpleDisjunctivePredicate)))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, expectedPredicatePushDownResult)
  }

  test("push down complex predicates through inner join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val joinCondition = (("x.b".attr === "y.b".attr)
      && ((("x.a".attr === 5) && ("y.a".attr >= 2) && ("y.a".attr <= 3))
      || (("x.a".attr === 2) && ("y.a".attr >= 1) && ("y.a".attr <= 14))
      || (("x.a".attr === 1) && ("y.a".attr >= 9) && ("y.a".attr <= 27))))

    val originalQuery = x.join(y, condition = Some(joinCondition))
    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(
      (Symbol("a") === 5 || Symbol("a") === 2 || Symbol("a") === 1)).subquery(Symbol("x"))
    val right = testRelation.where(
      (Symbol("a") >= 2 && Symbol("a") <= 3) || (Symbol("a") >= 1 &&
        Symbol("a") <= 14) || (Symbol("a") >= 9 && Symbol("a") <= 27)).subquery(Symbol("y"))
    val correctAnswer = left.join(right, condition = Some(joinCondition)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates(with NOT predicate) through inner join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr)
        && Not(("x.a".attr > 3)
        && ("x.a".attr < 2 || ("y.a".attr > 13)) || ("x.a".attr > 1) && ("y.a".attr > 11))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") <= 3 || Symbol("a") >= 2).subquery(Symbol("x"))
    val right = testRelation.subquery(Symbol("y"))
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr <= 3) || (("x.a".attr >= 2) && ("y.a".attr <= 13)))
        && (("x.a".attr <= 1) || ("y.a".attr <= 11))))
        .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates through left join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y, joinType = LeftOuter, condition = Some(("x.b".attr === "y.b".attr)
        && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery(Symbol("x"))
    val right = testRelation.where(Symbol("a") > 13 || Symbol("a") > 11).subquery(Symbol("y"))
    val correctAnswer =
      left.join(right, joinType = LeftOuter, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates through right join") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y, joinType = RightOuter, condition = Some(("x.b".attr === "y.b".attr)
        && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where(Symbol("a") > 3 || Symbol("a") > 1).subquery(Symbol("x"))
    val right = testRelation.subquery(Symbol("y"))
    val correctAnswer =
      left.join(right, joinType = RightOuter, condition = Some("x.b".attr === "y.b".attr
        && (("x.a".attr > 3) && ("y.a".attr > 13) || ("x.a".attr > 1) && ("y.a".attr > 11))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32302: avoid generating too many predicates") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))

    val originalQuery =
      x.join(y, condition = Some(("x.b".attr === "y.b".attr) && ((("x.a".attr > 3) &&
        ("x.a".attr < 13) && ("y.c".attr <= 5)) || (("y.a".attr > 2) && ("y.c".attr < 1)))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.subquery(Symbol("x"))
    val right = testRelation.where(Symbol("c") <= 5 || (Symbol("a") > 2 &&
      Symbol("c") < 1)).subquery(Symbol("y"))
    val correctAnswer = left.join(right, condition = Some("x.b".attr === "y.b".attr &&
      ((("x.a".attr > 3) && ("x.a".attr < 13) && ("y.c".attr <= 5)) ||
        (("y.a".attr > 2) && ("y.c".attr < 1))))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicate through multiple joins") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation.subquery(Symbol("y"))
    val z = testRelation.subquery(Symbol("z"))
    val xJoinY = x.join(y, condition = Some("x.b".attr === "y.b".attr))
    val originalQuery = z.join(xJoinY,
      condition = Some("x.a".attr === "z.a".attr && simpleDisjunctivePredicate))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = x.where(Symbol("a") > 3 || Symbol("a") > 1)
    val right = y.where(Symbol("a") > 13 || Symbol("a") > 11)
    val correctAnswer = z.join(left.join(right,
      condition = Some("x.b".attr === "y.b".attr && simpleDisjunctivePredicate)),
      condition = Some("x.a".attr === "z.a".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }
}
