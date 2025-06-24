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

package org.apache.spark.sql.catalyst.analysis

import java.util.TimeZone

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class ResolveGroupingAnalyticsSuite extends AnalysisTest {

  lazy val a = $"a".int
  lazy val b = $"b".string
  lazy val c = $"c".string
  lazy val unresolved_a = UnresolvedAttribute("a")
  lazy val unresolved_b = UnresolvedAttribute("b")
  lazy val unresolved_c = UnresolvedAttribute("c")
  lazy val gid = $"spark_grouping_id".long.withNullability(false)
  lazy val hive_gid = $"grouping__id".long.withNullability(false)
  lazy val grouping_a = Cast(ShiftRight(gid, 1) & 1L, ByteType, Option(TimeZone.getDefault().getID))
  lazy val nulInt = Literal(null, IntegerType)
  lazy val nulStr = Literal(null, StringType)
  lazy val r1 = LocalRelation(a, b, c)

  test("rollupExprs") {
    val testRollup = (exprs: Seq[Expression], rollup: Seq[Seq[Expression]]) => {
      val result = BaseGroupingSets.rollupExprs(exprs.map(Seq(_)))
      assert(result.sortBy(_.hashCode) == rollup.sortBy(_.hashCode))
    }

    testRollup(Seq(a, b, c), Seq(Seq(), Seq(a), Seq(a, b), Seq(a, b, c)))
    testRollup(Seq(c, b, a), Seq(Seq(), Seq(c), Seq(c, b), Seq(c, b, a)))
    testRollup(Seq(a), Seq(Seq(), Seq(a)))
    testRollup(Seq(), Seq(Seq()))
  }

  test("cubeExprs") {
    val testCube = (exprs: Seq[Expression], cube: Seq[Seq[Expression]]) => {
      val result = BaseGroupingSets.cubeExprs(exprs.map(Seq(_)))
      assert(result.sortBy(_.hashCode) == cube.sortBy(_.hashCode))
    }

    testCube(Seq(a, b, c),
      Seq(Seq(), Seq(a), Seq(b), Seq(c), Seq(a, b), Seq(a, c), Seq(b, c), Seq(a, b, c)))
    testCube(Seq(c, b, a),
      Seq(Seq(), Seq(a), Seq(b), Seq(c), Seq(c, b), Seq(c, a), Seq(b, a), Seq(c, b, a)))
    testCube(Seq(a), Seq(Seq(), Seq(a)))
    testCube(Seq(), Seq(Seq()))
  }

  test("grouping sets") {
    val originalPlan = Aggregate(
      Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
        Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, a, b, 0L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Aggregate(
      Seq(GroupingSets(Seq(), Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected2 = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    // `b` should be included in the GROUP BY expressions even though it's not in the grouping sets.
    val originalPlan3 = Aggregate(
      Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a)), Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected3 = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan3, expected3)

    // Computation of grouping expression should remove duplicate expression based on their
    // semantics (semanticEqual).
    val originalPlan4 = Aggregate(
      Seq(GroupingSets(Seq(
        Seq(Multiply(unresolved_a, Literal(2))),
        Seq(Multiply(Literal(2), unresolved_a), unresolved_b)))),
      Seq(UnresolvedAlias(Multiply(unresolved_a, Literal(2))),
        unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)

    val resultPlan = getAnalyzer.executeAndCheck(originalPlan4, new QueryPlanningTracker)
    val gExpressions = resultPlan.asInstanceOf[Aggregate].groupingExpressions
    assert(gExpressions.size == 3)
    val firstGroupingExprAttrName =
      gExpressions(0).asInstanceOf[AttributeReference].name.replaceAll("#[0-9]*", "#0")
    assert(firstGroupingExprAttrName == "(a * 2)")
    assert(gExpressions(1).asInstanceOf[AttributeReference].name == "b")
    assert(gExpressions(2).asInstanceOf[AttributeReference].name == VirtualColumn.groupingIdName)
  }

  test("cube") {
    val originalPlan = Aggregate(Seq(Cube(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L),
          Seq(a, b, c, nulInt, b, 2L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Aggregate(Seq(Cube(Seq())), Seq(UnresolvedAlias(count(unresolved_c))), r1)
    val expected2 = Aggregate(Seq(gid), Seq(count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, 0L)),
        Seq(a, b, c, gid),
        Project(Seq(a, b, c), r1)))
    checkAnalysis(originalPlan2, expected2)
  }

  test("rollup") {
    val originalPlan = Aggregate(Seq(Rollup(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Aggregate(Seq(Rollup(Seq())), Seq(UnresolvedAlias(count(unresolved_c))), r1)
    val expected2 = Aggregate(Seq(gid), Seq(count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, 0L)),
        Seq(a, b, c, gid),
        Project(Seq(a, b, c), r1)))
    checkAnalysis(originalPlan2, expected2)
  }

  test("grouping function") {
    // GrouingSets
    val originalPlan = Aggregate(
      Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))), r1)
    val expected = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, a, b, 0L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    // Cube
    val originalPlan2 = Aggregate(Seq(Cube(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))), r1)
    val expected2 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L),
          Seq(a, b, c, nulInt, b, 2L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    // Rollup
    val originalPlan3 = Aggregate(Seq(Rollup(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))), r1)
    val expected3 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan3, expected3)
  }

  test("grouping_id") {
    // GrouingSets
    val originalPlan = Aggregate(
      Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))), r1)
    val expected = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, a, b, 0L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    // Cube
    val originalPlan2 = Aggregate(Seq(Cube(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))), r1)
    val expected2 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L),
          Seq(a, b, c, nulInt, b, 2L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    // Rollup
    val originalPlan3 = Aggregate(Seq(Rollup(Seq(Seq(unresolved_a), Seq(unresolved_b)))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))), r1)
    val expected3 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0L), Seq(a, b, c, a, nulStr, 1L), Seq(a, b, c, nulInt, nulStr, 3L)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan3, expected3)
  }

  test("filter with grouping function") {
    // Filter with Grouping function
    val originalPlan = Filter(Grouping(unresolved_a) === 0,
      Aggregate(
        Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
        Seq(unresolved_a, unresolved_b), r1))
    val expected = Project(Seq(a, b),
      Filter(Cast(grouping_a, IntegerType, Option(TimeZone.getDefault().getID)) === 0,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L),
            Seq(a, b, c, a, b, 0L)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Filter(Grouping(unresolved_a) === 0,
      Aggregate(Seq(unresolved_a), Seq(UnresolvedAlias(count(unresolved_b))), r1))
    assertAnalysisError(originalPlan2,
      Seq("grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup"))

    // Filter with GroupingID
    val originalPlan3 = Filter(GroupingID(Seq(unresolved_a, unresolved_b)) === 1L,
      Aggregate(
        Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
        Seq(unresolved_a, unresolved_b), r1))
    val expected3 = Project(Seq(a, b), Filter(gid === 1L,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L),
            Seq(a, b, c, a, b, 0L)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan3, expected3)

    val originalPlan4 = Filter(GroupingID(Seq(unresolved_a)) === 1,
      Aggregate(Seq(unresolved_a), Seq(UnresolvedAlias(count(unresolved_b))), r1))
    assertAnalysisError(originalPlan4,
      Seq("grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup"))
  }

  test("sort with grouping function") {
    // Sort with Grouping function
    val originalPlan = Sort(
      Seq(SortOrder(Grouping(unresolved_a), Ascending)), true,
      Aggregate(
        Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
        Seq(unresolved_a, unresolved_b), r1))
    val expected = Project(Seq(a, b), Sort(
      Seq(SortOrder(grouping_a, Ascending)), true,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L),
            Seq(a, b, c, a, b, 0L)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Sort(Seq(SortOrder(Grouping(unresolved_a), Ascending)), true,
      Aggregate(Seq(unresolved_a), Seq(unresolved_a, UnresolvedAlias(count(unresolved_b))), r1))
    assertAnalysisError(originalPlan2,
      Seq("grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup"))

    // Sort with GroupingID
    val originalPlan3 = Sort(
      Seq(SortOrder(GroupingID(Seq(unresolved_a, unresolved_b)), Ascending)), true,
      Aggregate(
        Seq(GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)))),
        Seq(unresolved_a, unresolved_b), r1))
    val expected3 = Project(Seq(a, b), Sort(
      Seq(SortOrder(gid, Ascending)), true,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3L), Seq(a, b, c, a, nulStr, 1L),
            Seq(a, b, c, a, b, 0L)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan3, expected3)

    val originalPlan4 = Sort(
      Seq(SortOrder(GroupingID(Seq(unresolved_a)), Ascending)), true,
      Aggregate(Seq(unresolved_a), Seq(unresolved_a, UnresolvedAlias(count(unresolved_b))), r1))
    assertAnalysisError(originalPlan4,
      Seq("grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup"))
  }
}
