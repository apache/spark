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
import org.apache.spark.sql.types.{BooleanType, DecimalType}

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

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation2 = LocalRelation($"d".int, $"e".int, $"f".int)
  val testRelation3 = LocalRelation($"g".int, $"h".int, $"i".int)
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
    val unionQuery = testUnion.where($"a" === 1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.where($"a" === 1) ::
        testRelation2.where($"d" === 1) ::
        testRelation3.where($"g" === 1) :: Nil).analyze

    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("union: project to each side") {
    val unionQuery = testUnion.select($"a")
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select($"a") ::
        testRelation2.select($"d") ::
        testRelation3.select($"g") :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Remove unnecessary distincts in multiple unions") {
    val query1 = OneRowRelation()
      .select(Literal(1).as("a"))
    val query2 = OneRowRelation()
      .select(Literal(2).as("b"))
    val query3 = OneRowRelation()
      .select(Literal(3).as("c"))

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
      .select(Literal(1).as("a"))
    val query2 = OneRowRelation()
      .select(Literal(2).as("b"))
    val query3 = OneRowRelation()
      .select(Literal(3).as("c"))
    val query4 = OneRowRelation()
      .select(Literal(4).as("d"))

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

  test("SPARK-34283: Remove unnecessary deduplicate in multiple unions") {
    val query1 = OneRowRelation()
      .select(Literal(1).as("a"))
    val query2 = OneRowRelation()
      .select(Literal(2).as("b"))
    val query3 = OneRowRelation()
      .select(Literal(3).as("c"))

    // D - U - D - U - query1
    //     |       |
    //     query3  query2
    val unionQuery1 = Deduplicate(query1.output, Union(
      Deduplicate(query1.output, Union(query1, query2)), query3)).analyze
    val optimized1 = Optimize.execute(unionQuery1)
    val deduplicateUnionCorrectAnswer1 = Deduplicate(query1.output,
      Union(query1 :: query2 :: query3 :: Nil))
    comparePlans(deduplicateUnionCorrectAnswer1, optimized1)

    //         query1
    //         |
    // D - U - U - query2
    //     |
    //     D - U - query2
    //         |
    //         query3
    val unionQuery2 = Deduplicate(query1.output, Union(Union(query1, query2),
      Deduplicate(query2.output, Union(query2, query3)))).analyze
    val optimized2 = Optimize.execute(unionQuery2)
    val deduplicateUnionCorrectAnswer2 =
      Deduplicate(query1.output, Union(query1 :: query2 :: query2 :: query3 :: Nil))
    comparePlans(deduplicateUnionCorrectAnswer2, optimized2)

    // D  -  U  -  D  -  U  -  testRelation
    //       |           |
    //  testRelation  testRelation
    // Union with the same value of 'byName' and 'allowMissingCol'
    val unionQuery3 = Deduplicate(testRelation.output,
      Union(Deduplicate(testRelation.output,
        Union(testRelation :: testRelation :: Nil, true, false)) :: testRelation :: Nil,
        true, false))
    val optimized3 = Optimize.execute(unionQuery3)
    val deduplicateUnionCorrectAnswer3 =
      Deduplicate(testRelation.output,
        Union(testRelation :: testRelation :: testRelation :: Nil, true, false))
    comparePlans(deduplicateUnionCorrectAnswer3, optimized3, false)
  }

  test("SPARK-34283: Keep necessary deduplicate in multiple unions") {
    val query1 = OneRowRelation()
      .select(Literal(1).as("a"))
    val query2 = OneRowRelation()
      .select(Literal(2).as("b"))
    val query3 = OneRowRelation()
      .select(Literal(3).as("c"))
    val query4 = OneRowRelation()
      .select(Literal(4).as("d"))

    // U - D - U - query1
    // |       |
    // query3  query2
    val unionQuery1 = Union(Deduplicate(query1.output, Union(query1, query2)), query3).analyze
    val optimized1 = Optimize.execute(unionQuery1)
    comparePlans(unionQuery1, optimized1)

    //         query1
    //         |
    // U - D - U - query2
    // |
    // D - U - query3
    //     |
    //     query4
    val unionQuery2 = Union(Deduplicate(query1.output, Union(query1, query2)),
      Deduplicate(query3.output, Union(query3, query4))).analyze
    val optimized2 = Optimize.execute(unionQuery2)
    comparePlans(unionQuery2, optimized2)

    // D  -  U  -  D  -  U  -  testRelation
    //       |           |
    //  testRelation  testRelation
    // Union with different value of 'byName' and 'allowMissingCol'
    val unionQuery3 = Deduplicate(testRelation.output,
      Union(Deduplicate(testRelation.output,
        Union(testRelation :: testRelation :: Nil, true, false)) :: testRelation :: Nil,
        true, true))
    val optimized3 = Optimize.execute(unionQuery3)
    comparePlans(unionQuery3, optimized3, false)
  }

  test("EXCEPT ALL rewrite") {
    val input = Except(testRelation, testRelation2, isAll = true)
    val rewrittenPlan = RewriteExceptAll(input)

    val planFragment = testRelation.select(Literal(1L).as("vcol"), $"a", $"b", $"c")
      .union(testRelation2.select(Literal(-1L).as("vcol"), $"d", $"e", $"f"))
      .groupBy($"a", $"b", $"c")($"a", $"b", $"c", sum($"vcol").as("sum"))
      .where(GreaterThan($"sum", Literal(0L))).analyze
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
      .select(Literal(true).as("vcol1"), Literal(null, BooleanType).as("vcol2"), $"a", $"b", $"c")
    val rightRelation = testRelation2
      .select(Literal(null, BooleanType).as("vcol1"), Literal(true).as("vcol2"), $"d", $"e", $"f")
    val planFragment = leftRelation.union(rightRelation)
      .groupBy($"a", $"b", $"c")(count($"vcol1").as("vcol1_count"),
        count($"vcol2").as("vcol2_count"), $"a", $"b", $"c")
      .where(And(GreaterThanOrEqual($"vcol1_count", Literal(1L)),
        GreaterThanOrEqual($"vcol2_count", Literal(1L))))
      .select($"a", $"b", $"c",
        If(GreaterThan($"vcol1_count", $"vcol2_count"), $"vcol2_count", $"vcol1_count")
          .as("min_count"))
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
    val unionQuery = testUnion.select(($"a" + 1).as("aa"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select(($"a" + 1).as("aa")) ::
        testRelation2.select(($"d" + 1).as("aa")) ::
        testRelation3.select(($"g" + 1).as("aa")) :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("SPARK-23356 union: expressions in project list are pushed down") {
    val unionQuery = testUnion.select(($"a" + $"b").as("ab"))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.select(($"a" + $"b").as("ab")) ::
        testRelation2.select(($"d" + $"e").as("ab")) ::
        testRelation3.select(($"g" + $"h").as("ab")) :: Nil).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("SPARK-23356 union: no pushdown for non-deterministic expression") {
    val unionQuery = testUnion.select($"a", Rand(10).as("rnd"))
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

  test("SPARK-37915: combine unions if there is a project between them") {
    val relation1 = LocalRelation($"a".decimal(18, 1), $"b".int)
    val relation2 = LocalRelation($"a".decimal(18, 2), $"b".int)
    val relation3 = LocalRelation($"a".decimal(18, 3), $"b".int)
    val relation4 = LocalRelation($"a".decimal(18, 4), $"b".int)
    val relation5 = LocalRelation($"a".decimal(18, 5), $"b".int)

    val optimizedRelation1 = relation1.select($"a".cast(DecimalType(19, 2)).cast(DecimalType(20, 3))
      .cast(DecimalType(21, 4)).cast(DecimalType(22, 5)).as("a"), $"b")
    val optimizedRelation2 = relation2.select($"a".cast(DecimalType(19, 2)).cast(DecimalType(20, 3))
      .cast(DecimalType(21, 4)).cast(DecimalType(22, 5)).as("a"), $"b")
    val optimizedRelation3 = relation3.select($"a".cast(DecimalType(20, 3))
      .cast(DecimalType(21, 4)).cast(DecimalType(22, 5)).as("a"), $"b")
    val optimizedRelation4 = relation4
      .select($"a".cast(DecimalType(21, 4)).cast(DecimalType(22, 5)).as("a"), $"b")
    val optimizedRelation5 = relation5.select($"a".cast(DecimalType(22, 5)).as("a"), $"b")

    // SQL UNION ALL
    comparePlans(
      Optimize.execute(relation1.union(relation2)
        .union(relation3).union(relation4).union(relation5).analyze),
      Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
        optimizedRelation4, optimizedRelation5)).analyze)

    // SQL UNION
    comparePlans(
      Optimize.execute(Distinct(Distinct(Distinct(Distinct(relation1.union(relation2))
        .union(relation3)).union(relation4)).union(relation5)).analyze),
      Distinct(Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
        optimizedRelation4, optimizedRelation5))).analyze)

    // Deduplicate
    comparePlans(
      Optimize.execute(relation1.union(relation2).deduplicate($"a", $"b").union(relation3)
        .deduplicate($"a", $"b").union(relation4).deduplicate($"a", $"b").union(relation5)
        .deduplicate($"a", $"b").analyze),
      Deduplicate(
        Seq($"a", $"b"),
        Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
          optimizedRelation4, optimizedRelation5))).analyze)

    // Other cases
    comparePlans(
      Optimize.execute(Distinct(Distinct(Distinct(Distinct(relation1.union(relation2))
        .union(relation3)).union(relation4)).union(relation5)).select($"a" % 2).analyze),
      Distinct(Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
        optimizedRelation4, optimizedRelation5))).select($"a" % 2).analyze)

    comparePlans(
      Optimize.execute(Distinct(Distinct(Distinct(Distinct(relation1.union(relation2))
        .union(relation3)).union(relation4)).union(relation5)).select($"a" + $"b").analyze),
      Distinct(Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
        optimizedRelation4, optimizedRelation5))).select($"a" + $"b").analyze)

    comparePlans(
      Optimize.execute(Distinct(Distinct(Distinct(Distinct(relation1.union(relation2))
        .union(relation3)).union(relation4)).union(relation5)).select($"a").analyze),
      Distinct(Union(Seq(optimizedRelation1, optimizedRelation2, optimizedRelation3,
        optimizedRelation4, optimizedRelation5))).select($"a").analyze)

  }
}
