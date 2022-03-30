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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, GetStructField, InSubquery, LateralSubquery, ListQuery, OuterReference, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Unit tests for [[ResolveSubquery]].
 */
class ResolveSubquerySuite extends AnalysisTest {

  val a = $"a".int
  val b = $"b".int
  val c = $"c".int
  val x = $"x".struct(a)
  val y = $"y".struct(a)
  val t0 = OneRowRelation()
  val t1 = LocalRelation(a, b)
  val t2 = LocalRelation(b, c)
  val t3 = LocalRelation(c)
  val t4 = LocalRelation(x, y)

  private def lateralJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType = Inner,
      condition: Option[Expression] = None): LateralJoin =
    LateralJoin(left, LateralSubquery(right), joinType, condition)

  test("SPARK-17251 Improve `OuterReference` to be `NamedExpression`") {
    val expr = Filter(
      InSubquery(Seq(a), ListQuery(Project(Seq(UnresolvedAttribute("a")), t2))), t1)
    val m = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(SimpleAnalyzer.ResolveSubquery(expr))
    }.getMessage
    assert(m.contains(
      "Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses"))
  }

  test("SPARK-29145 Support subquery in join condition") {
    val expr = Join(t1,
      t2,
      Inner,
      Some(InSubquery(Seq(a), ListQuery(Project(Seq(UnresolvedAttribute("c")), t3)))),
      JoinHint.NONE)
    assertAnalysisSuccess(expr)
  }

  test("deduplicate lateral subquery") {
    val plan = lateralJoin(t1, t0.select($"a"))
    // The subquery's output OuterReference(a#0) conflicts with the left child output
    // attribute a#0. So an alias should be added to deduplicate the subquery's outputs.
    val expected = LateralJoin(
      t1,
      LateralSubquery(Project(Seq(OuterReference(a).as(a.name)), t0), Seq(a)),
      Inner,
      None)
    checkAnalysis(plan, expected)
  }

  test("lateral join with ambiguous join conditions") {
    val plan = lateralJoin(t1, t0.select($"b"), condition = Some($"b" ===  1))
    assertAnalysisError(plan, "Reference 'b' is ambiguous, could be: b, b." :: Nil)
  }

  test("prefer resolving lateral subquery attributes from the inner query") {
    val plan = lateralJoin(t1, t2.select($"a", $"b", $"c"))
    val expected = LateralJoin(
      t1,
      LateralSubquery(Project(Seq(OuterReference(a).as(a.name), b, c), t2), Seq(a)),
      Inner, None)
    checkAnalysis(plan, expected)
  }

  test("qualified column names in lateral subquery") {
    val t1b = b.withQualifier(Seq("t1"))
    val t2b = b.withQualifier(Seq("t2"))
    checkAnalysis(
      lateralJoin(t1.as("t1"), t0.select($"t1.b")),
      LateralJoin(
        t1,
        LateralSubquery(Project(Seq(OuterReference(t1b).as(b.name)), t0), Seq(t1b)),
        Inner, None)
    )
    checkAnalysis(
      lateralJoin(t1.as("t1"), t2.as("t2").select($"t1.b", $"t2.b")),
      LateralJoin(
        t1,
        LateralSubquery(Project(Seq(OuterReference(t1b).as(b.name), t2b), t2.as("t2")), Seq(t1b)),
        Inner, None)
    )
  }

  test("resolve nested lateral subqueries") {
    // SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT a, b, c FROM t2), LATERAL (SELECT b, c))
    checkAnalysis(
      lateralJoin(t1, lateralJoin(t2.select($"a", $"b", $"c"), t0.select($"b", $"c"))),
      LateralJoin(
        t1,
        LateralSubquery(
          LateralJoin(
            Project(Seq(OuterReference(a).as(a.name), b, c), t2),
            LateralSubquery(
              Project(Seq(OuterReference(b).as(b.name), OuterReference(c).as(c.name)), t0),
              Seq(b, c)),
            Inner, None),
          Seq(a)),
        Inner, None)
    )

    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT a, b, c))
    // TODO: support accessing columns from outer outer query.
    assertAnalysisErrorClass(
      lateralJoin(t1, lateralJoin(t2, t0.select($"a", $"b", $"c"))),
      "MISSING_COLUMN",
      Array("a", ""))
  }

  test("lateral subquery with unresolvable attributes") {
    // SELECT * FROM t1, LATERAL (SELECT a, c)
    assertAnalysisErrorClass(
      lateralJoin(t1, t0.select($"a", $"c")),
      "MISSING_COLUMN",
      Array("c", "")
    )
    // SELECT * FROM t1, LATERAL (SELECT a, b, c, d FROM t2)
    assertAnalysisErrorClass(
      lateralJoin(t1, t2.select($"a", $"b", $"c", $"d")),
      "MISSING_COLUMN",
      Array("d", "b, c")
    )
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT t1.a))
    assertAnalysisErrorClass(
      lateralJoin(t1, lateralJoin(t2, t0.select($"t1.a"))),
      "MISSING_COLUMN",
      Array("t1.a", "")
    )
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT a, b))
    assertAnalysisErrorClass(
      lateralJoin(t1, lateralJoin(t2, t0.select($"a", $"b"))),
      "MISSING_COLUMN",
      Array("a", "")
    )
  }

  test("lateral subquery with struct type") {
    val xa = GetStructField(OuterReference(x), 0, Some("a")).as(a.name)
    val ya = GetStructField(OuterReference(y), 0, Some("a")).as(a.name)
    checkAnalysis(
      lateralJoin(t4, t0.select($"x.a", $"y.a")),
      LateralJoin(t4, LateralSubquery(Project(Seq(xa, ya), t0), Seq(x, y)), Inner, None)
    )
    // Analyzer will try to resolve struct first before subquery alias.
    assertAnalysisError(
      lateralJoin(t1.as("x"), t4.select($"x.a", $"x.b")),
      Seq("No such struct field b in a")
    )
  }

  test("lateral join with unsupported expressions") {
    val plan = lateralJoin(t1, t0.select(($"a" + $"b").as("c")),
      condition = Some(sum($"a") === sum($"c")))
    assertAnalysisError(plan, Seq("Invalid expressions: [sum(a), sum(c)]"))
  }

  test("SPARK-35618: lateral join with star expansion") {
    val outerA = OuterReference(a.withQualifier(Seq("t1"))).as(a.name)
    val outerB = OuterReference(b.withQualifier(Seq("t1"))).as(b.name)
    // SELECT * FROM t1, LATERAL (SELECT *)
    checkAnalysis(
      lateralJoin(t1.as("t1"), t0.select(star())),
      LateralJoin(t1, LateralSubquery(Project(Nil, t0)), Inner, None)
    )
    // SELECT * FROM t1, LATERAL (SELECT t1.*)
    checkAnalysis(
      lateralJoin(t1.as("t1"), t0.select(star("t1"))),
      LateralJoin(t1, LateralSubquery(Project(Seq(outerA, outerB), t0), Seq(a, b)), Inner, None)
    )
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2)
    checkAnalysis(
      lateralJoin(t1.as("t1"), t2.select(star())),
      LateralJoin(t1, LateralSubquery(Project(Seq(b, c), t2)), Inner, None)
    )
    // SELECT * FROM t1, LATERAL (SELECT t1.*, t2.* FROM t2)
    checkAnalysis(
      lateralJoin(t1.as("t1"), t2.as("t2").select(star("t1"), star("t2"))),
      LateralJoin(t1,
        LateralSubquery(Project(Seq(outerA, outerB, b, c), t2.as("t2")), Seq(a, b)), Inner, None)
    )
    // SELECT * FROM t1, LATERAL (SELECT t2.*)
    assertAnalysisError(
      lateralJoin(t1.as("t1"), t0.select(star("t2"))),
      Seq("cannot resolve 't2.*' given input columns ''")
    )
    // Check case sensitivities.
    // SELECT * FROM t1, LATERAL (SELECT T1.*)
    val plan = lateralJoin(t1.as("t1"), t0.select(star("T1")))
    assertAnalysisError(plan, "cannot resolve 'T1.*' given input columns ''" :: Nil)
    assertAnalysisSuccess(plan, caseSensitive = false)
  }

  test("SPARK-35618: lateral join with star expansion in functions") {
    val outerA = OuterReference(a.withQualifier(Seq("t1")))
    val outerB = OuterReference(b.withQualifier(Seq("t1")))
    val array = CreateArray(Seq(star("t1")))
    val newArray = CreateArray(Seq(outerA, outerB))
    checkAnalysis(
      lateralJoin(t1.as("t1"), t0.select(array)),
      LateralJoin(t1,
        LateralSubquery(t0.select(newArray.as(newArray.sql)), Seq(a, b)), Inner, None)
    )
    assertAnalysisError(
      lateralJoin(t1.as("t1"), t0.select(Count(star("t1")))),
      Seq("Invalid usage of '*' in expression 'count'")
    )
  }

  test("SPARK-35618: lateral join with struct type star expansion") {
    // SELECT * FROM t4, LATERAL (SELECT x.*)
    checkAnalysis(
      lateralJoin(t4, t0.select(star("x"))),
      LateralJoin(t4, LateralSubquery(
        Project(Seq(GetStructField(OuterReference(x), 0).as(a.name)), t0), Seq(x)),
        Inner, None)
    )
  }

  test("SPARK-36028: resolve scalar subqueries with outer references in Project") {
    // SELECT (SELECT a) FROM t1
    checkAnalysis(
      Project(ScalarSubquery(t0.select($"a")).as("sub") :: Nil, t1),
      Project(ScalarSubquery(Project(OuterReference(a) :: Nil, t0), Seq(a)).as("sub") :: Nil, t1)
    )
    // SELECT (SELECT a + b + c AS r FROM t2) FROM t1
    checkAnalysis(
      Project(ScalarSubquery(
        t2.select(($"a" + $"b" + $"c").as("r"))).as("sub") :: Nil, t1),
      Project(ScalarSubquery(
        Project((OuterReference(a) + b + c).as("r") :: Nil, t2), Seq(a)).as("sub") :: Nil, t1)
    )
    // SELECT (SELECT array(t1.*) AS arr) FROM t1
    checkAnalysis(
      Project(ScalarSubquery(t0.select(
        CreateArray(Seq(star("t1"))).as("arr"))
      ).as("sub") :: Nil, t1.as("t1")),
      Project(ScalarSubquery(Project(
        CreateArray(Seq(OuterReference(a), OuterReference(b))).as("arr") :: Nil, t0
      ), Seq(a, b)).as("sub") :: Nil, t1)
    )
  }
}
