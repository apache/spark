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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThan, GreaterThanOrEqual, If, Literal, Rand, ReplicateRows}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.BooleanType

class SetOperationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Union Pushdown", FixedPoint(5),
        CombineUnions,
        PushProjectionThroughUnion,
        PushPredicateThroughNonJoin,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)
  val testRelation2 = LocalRelation(Symbol("d").int, Symbol("e").int, Symbol("f").int)
  val testRelation3 = LocalRelation(Symbol("g").int, Symbol("h").int, Symbol("i").int)
  val testUnion = Union(testRelation :: testRelation2 :: testRelation3 :: Nil)

  test("union: combine unions into one unions") {
    val unionQuery1 = Union(Union(testRelation, testRelation2), testRelation3)
    val unionQuery2 = Union(testRelation, Union(testRelation2, testRelation3))
    val unionOptimized1 = Optimize.execute(unionQuery1.analyze)
    val unionOptimized2 = Optimize.execute(unionQuery2.analyze)

    comparePlans(unionOptimized1, unionOptimized2)

    val combinedUnions = Union(unionOptimized1 :: unionOptimized2 :: Nil)
    val combinedUnionsOptimized = Optimize.execute(combinedUnions.analyze)
    val unionQuery3 = Union(unionQuery1, unionQuery2)
    val unionOptimized3 = Optimize.execute(unionQuery3.analyze)
    comparePlans(combinedUnionsOptimized, unionOptimized3)
  }

  test("union: filter to each side") {
    val unionQuery = testUnion.where(Symbol("a") === 1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.where(Symbol("a") === 1) ::
        testRelation2.where(Symbol("d") === 1) ::
        testRelation3.where(Symbol("g") === 1) :: Nil).analyze

    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("union: project to each side") {
    val unionQuery = testUnion.select(Symbol("a"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select(Symbol("a")) ::
        testRelation2.select(Symbol("d")) ::
        testRelation3.select(Symbol("g")) :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Remove unnecessary distincts in multiple unions") {
    val query1 = OneRowRelation()
      .select(Literal(1).as(Symbol("a")))
    val query2 = OneRowRelation()
      .select(Literal(2).as(Symbol("b")))
    val query3 = OneRowRelation()
      .select(Literal(3).as(Symbol("c")))

    // D - U - D - U - query1
    //     |       |
    //     query3  query2
    val unionQuery1 = Distinct(Union(Distinct(Union(query1, query2)), query3)).analyze
    val optimized1 = Optimize.execute(unionQuery1)
    val distinctUnionCorrectAnswer1 =
      Distinct(Union(query1 :: query2 :: query3 :: Nil))
    comparePlans(distinctUnionCorrectAnswer1, optimized1)

    //         query1
    //         |
    // D - U - U - query2
    //     |
    //     D - U - query2
    //         |
    //         query3
    val unionQuery2 = Distinct(Union(Union(query1, query2),
      Distinct(Union(query2, query3)))).analyze
    val optimized2 = Optimize.execute(unionQuery2)
    val distinctUnionCorrectAnswer2 =
      Distinct(Union(query1 :: query2 :: query2 :: query3 :: Nil))
    comparePlans(distinctUnionCorrectAnswer2, optimized2)
  }

  test("Keep necessary distincts in multiple unions") {
    val query1 = OneRowRelation()
      .select(Literal(1).as(Symbol("a")))
    val query2 = OneRowRelation()
      .select(Literal(2).as(Symbol("b")))
    val query3 = OneRowRelation()
      .select(Literal(3).as(Symbol("c")))
    val query4 = OneRowRelation()
      .select(Literal(4).as(Symbol("d")))

    // U - D - U - query1
    // |       |
    // query3  query2
    val unionQuery1 = Union(Distinct(Union(query1, query2)), query3).analyze
    val optimized1 = Optimize.execute(unionQuery1)
    val distinctUnionCorrectAnswer1 =
      Union(Distinct(Union(query1 :: query2 :: Nil)) :: query3 :: Nil).analyze
    comparePlans(distinctUnionCorrectAnswer1, optimized1)

    //         query1
    //         |
    // U - D - U - query2
    // |
    // D - U - query3
    //     |
    //     query4
    val unionQuery2 =
      Union(Distinct(Union(query1, query2)), Distinct(Union(query3, query4))).analyze
    val optimized2 = Optimize.execute(unionQuery2)
    val distinctUnionCorrectAnswer2 =
      Union(Distinct(Union(query1 :: query2 :: Nil)),
            Distinct(Union(query3 :: query4 :: Nil))).analyze
    comparePlans(distinctUnionCorrectAnswer2, optimized2)
  }

  test("EXCEPT ALL rewrite") {
    val input = Except(testRelation, testRelation2, isAll = true)
    val rewrittenPlan = RewriteExceptAll(input)

    val planFragment = testRelation
      .select(Literal(1L).as("vcol"), Symbol("a"), Symbol("b"), Symbol("c"))
      .union(testRelation2.select(Literal(-1L).as("vcol"), Symbol("d"), Symbol("e"), Symbol("f")))
      .groupBy(Symbol("a"), Symbol("b"), Symbol("c"))(
        Symbol("a"), Symbol("b"), Symbol("c"), sum(Symbol("vcol")).as("sum"))
      .where(GreaterThan(Symbol("sum"), Literal(0L))).analyze
    val multiplierAttr = planFragment.output.last
    val output = planFragment.output.dropRight(1)
    val expectedPlan = Project(output,
      Generate(
        ReplicateRows(Seq(multiplierAttr) ++ output),
        Nil,
        false,
        None,
        output,
        planFragment
      ))
    comparePlans(expectedPlan, rewrittenPlan)
  }

  test("INTERSECT ALL rewrite") {
    val input = Intersect(testRelation, testRelation2, isAll = true)
    val rewrittenPlan = RewriteIntersectAll(input)
    val leftRelation = testRelation
      .select(Literal(true).as("vcol1"),
        Literal(null, BooleanType).as("vcol2"), Symbol("a"), Symbol("b"), Symbol("c"))
    val rightRelation = testRelation2
      .select(Literal(null, BooleanType).as("vcol1"),
        Literal(true).as("vcol2"), Symbol("d"), Symbol("e"), Symbol("f"))
    val planFragment = leftRelation.union(rightRelation)
      .groupBy(Symbol("a"), Symbol("b"), Symbol("c"))(count(Symbol("vcol1")).as("vcol1_count"),
        count(Symbol("vcol2")).as("vcol2_count"), Symbol("a"), Symbol("b"), Symbol("c"))
      .where(And(GreaterThanOrEqual(Symbol("vcol1_count"), Literal(1L)),
        GreaterThanOrEqual(Symbol("vcol2_count"), Literal(1L))))
      .select(Symbol("a"), Symbol("b"), Symbol("c"),
        If(GreaterThan(Symbol("vcol1_count"), Symbol("vcol2_count")),
          Symbol("vcol2_count"), Symbol("vcol1_count")).as("min_count"))
      .analyze
    val multiplierAttr = planFragment.output.last
    val output = planFragment.output.dropRight(1)
    val expectedPlan = Project(output,
      Generate(
        ReplicateRows(Seq(multiplierAttr) ++ output),
        Nil,
        false,
        None,
        output,
        planFragment
      ))
    comparePlans(expectedPlan, rewrittenPlan)
  }

  test("SPARK-23356 union: expressions with literal in project list are pushed down") {
    val unionQuery = testUnion.select((Symbol("a") + 1).as("aa"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select((Symbol("a") + 1).as("aa")) ::
        testRelation2.select((Symbol("d") + 1).as("aa")) ::
        testRelation3.select((Symbol("g") + 1).as("aa")) :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("SPARK-23356 union: expressions in project list are pushed down") {
    val unionQuery = testUnion.select((Symbol("a") + Symbol("b")).as("ab"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select((Symbol("a") + Symbol("b")).as("ab")) ::
        testRelation2.select((Symbol("d") + Symbol("e")).as("ab")) ::
        testRelation3.select((Symbol("g") + Symbol("h")).as("ab")) :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("SPARK-23356 union: no pushdown for non-deterministic expression") {
    val unionQuery = testUnion.select(Symbol("a"), Rand(10).as("rnd"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer = unionQuery.analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("CombineUnions only flatten the unions with same byName and allowMissingCol") {
    val union1 = Union(testRelation :: testRelation :: Nil, true, false)
    val union2 = Union(testRelation :: testRelation :: Nil, true, true)
    val union3 = Union(testRelation :: testRelation2 :: Nil, false, false)

    val union4 = Union(union1 :: union2 :: union3 :: Nil)
    val unionOptimized1 = Optimize.execute(union4)
    val unionCorrectAnswer1 = Union(union1 :: union2 :: testRelation :: testRelation2 :: Nil)
    comparePlans(unionOptimized1, unionCorrectAnswer1, false)

    val union5 = Union(union1 :: union1 :: Nil, true, false)
    val unionOptimized2 = Optimize.execute(union5)
    val unionCorrectAnswer2 =
      Union(testRelation :: testRelation :: testRelation :: testRelation :: Nil, true, false)
    comparePlans(unionOptimized2, unionCorrectAnswer2, false)
  }
}
