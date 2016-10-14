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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class ResolveGroupingAnalyticsSuite extends AnalysisTest {

  lazy val a = 'a.int
  lazy val b = 'b.string
  lazy val c = 'c.string
  lazy val unresolved_a = UnresolvedAttribute("a")
  lazy val unresolved_b = UnresolvedAttribute("b")
  lazy val unresolved_c = UnresolvedAttribute("c")
  lazy val gid = 'spark_grouping_id.int.withNullability(false)
  lazy val hive_gid = 'grouping__id.int.withNullability(false)
  lazy val grouping_a = Cast(ShiftRight(gid, 1) & 1, ByteType)
  lazy val nulInt = Literal(null, IntegerType)
  lazy val nulStr = Literal(null, StringType)
  lazy val r1 = LocalRelation(a, b, c)

  test("grouping sets") {
    val originalPlan = GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
      Seq(unresolved_a, unresolved_b), r1,
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))))
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = GroupingSets(Seq(), Seq(unresolved_a, unresolved_b), r1,
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))))
    val expected2 = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    val originalPlan3 = GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b),
      Seq(unresolved_c)), Seq(unresolved_a, unresolved_b), r1,
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))))
    assertAnalysisError(originalPlan3, Seq("doesn't show up in the GROUP BY list"))
  }

  test("cube") {
    val originalPlan = Aggregate(Seq(Cube(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1),
          Seq(a, b, c, nulInt, b, 2), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Aggregate(Seq(Cube(Seq())), Seq(UnresolvedAlias(count(unresolved_c))), r1)
    val expected2 = Aggregate(Seq(gid), Seq(count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, 0)),
        Seq(a, b, c, gid),
        Project(Seq(a, b, c), r1)))
    checkAnalysis(originalPlan2, expected2)
  }

  test("rollup") {
    val originalPlan = Aggregate(Seq(Rollup(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c))), r1)
    val expected = Aggregate(Seq(a, b, gid), Seq(a, b, count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    val originalPlan2 = Aggregate(Seq(Rollup(Seq())), Seq(UnresolvedAlias(count(unresolved_c))), r1)
    val expected2 = Aggregate(Seq(gid), Seq(count(c).as("count(c)")),
      Expand(
        Seq(Seq(a, b, c, 0)),
        Seq(a, b, c, gid),
        Project(Seq(a, b, c), r1)))
    checkAnalysis(originalPlan2, expected2)
  }

  test("grouping function") {
    // GrouingSets
    val originalPlan = GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
      Seq(unresolved_a, unresolved_b), r1,
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))))
    val expected = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    // Cube
    val originalPlan2 = Aggregate(Seq(Cube(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))), r1)
    val expected2 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1),
          Seq(a, b, c, nulInt, b, 2), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    // Rollup
    val originalPlan3 = Aggregate(Seq(Rollup(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(Grouping(unresolved_a))), r1)
    val expected3 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), grouping_a.as("grouping(a)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan3, expected3)
  }

  test("grouping_id") {
    // GrouingSets
    val originalPlan = GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
      Seq(unresolved_a, unresolved_b), r1,
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))))
    val expected = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan, expected)

    // Cube
    val originalPlan2 = Aggregate(Seq(Cube(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))), r1)
    val expected2 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1),
          Seq(a, b, c, nulInt, b, 2), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan2, expected2)

    // Rollup
    val originalPlan3 = Aggregate(Seq(Rollup(Seq(unresolved_a, unresolved_b))),
      Seq(unresolved_a, unresolved_b, UnresolvedAlias(count(unresolved_c)),
        UnresolvedAlias(GroupingID(Seq(unresolved_a, unresolved_b)))), r1)
    val expected3 = Aggregate(Seq(a, b, gid),
      Seq(a, b, count(c).as("count(c)"), gid.as("grouping_id(a, b)")),
      Expand(
        Seq(Seq(a, b, c, a, b, 0), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, nulInt, nulStr, 3)),
        Seq(a, b, c, a, b, gid),
        Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))
    checkAnalysis(originalPlan3, expected3)
  }

  test("filter with grouping function") {
    // Filter with Grouping function
    val originalPlan = Filter(Grouping(unresolved_a) === 0,
      GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
        Seq(unresolved_a, unresolved_b), r1, Seq(unresolved_a, unresolved_b)))
    val expected = Project(Seq(a, b), Filter(Cast(grouping_a, IntegerType) === 0,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan, expected)

    // Filter with GroupingID
    val originalPlan2 = Filter(GroupingID(Seq(unresolved_a, unresolved_b)) === 1,
      GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
        Seq(unresolved_a, unresolved_b), r1, Seq(unresolved_a, unresolved_b)))
    val expected2 = Project(Seq(a, b), Filter(gid === 1,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan2, expected2)
  }

  test("sort with grouping function") {
    // Sort with Grouping function
    val originalPlan = Sort(
      Seq(SortOrder(Grouping(unresolved_a), Ascending)), true,
      GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
        Seq(unresolved_a, unresolved_b), r1, Seq(unresolved_a, unresolved_b)))
    val expected = Project(Seq(a, b), Sort(
      Seq(SortOrder('aggOrder.byte.withNullability(false), Ascending)), true,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, grouping_a.as("aggOrder")),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan, expected)

    // Sort with GroupingID
    val originalPlan2 = Sort(
      Seq(SortOrder(GroupingID(Seq(unresolved_a, unresolved_b)), Ascending)), true,
      GroupingSets(Seq(Seq(), Seq(unresolved_a), Seq(unresolved_a, unresolved_b)),
        Seq(unresolved_a, unresolved_b), r1, Seq(unresolved_a, unresolved_b)))
    val expected2 = Project(Seq(a, b), Sort(
      Seq(SortOrder('aggOrder.int.withNullability(false), Ascending)), true,
      Aggregate(Seq(a, b, gid),
        Seq(a, b, gid.as("aggOrder")),
        Expand(
          Seq(Seq(a, b, c, nulInt, nulStr, 3), Seq(a, b, c, a, nulStr, 1), Seq(a, b, c, a, b, 0)),
          Seq(a, b, c, a, b, gid),
          Project(Seq(a, b, c, a.as("a"), b.as("b")), r1)))))
    checkAnalysis(originalPlan2, expected2)
  }
}
