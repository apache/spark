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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CreateArray, GetStructField, Murmur3Hash, OuterReference}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, LateralJoin}
import org.apache.spark.sql.catalyst.plans.logical._

class ResolveLateralJoinSuite extends AnalysisTest {

  val a = 'a.int
  val b = 'b.int
  val c = 'c.int
  val d = 'd.int
  val x = 'x.struct(a)
  val y = 'y.struct(a)
  val t0 = OneRowRelation()
  val t1 = LocalRelation(a, b)
  val t2 = LocalRelation(a, c)
  val t3 = LocalRelation(x, y)

  test("deduplicate lateral join children") {
    val plan = t1.join(t0.select('a), LateralJoin(Inner))
    // The right child output is OuterReference(a#0) which conflicts with the left output
    // attribute a#0. So an alias should be added to deduplicate children's outputs.
    val expected = t1.join(Project(Seq(OuterReference(a).as(a.name)), t0), LateralJoin(Inner))
    checkAnalysis(plan, expected)
  }

  test("prefer resolving lateral subquery attributes from the inner query") {
    val plan = t1.join(t2.select('a, 'b, 'c), LateralJoin(Inner))
    // `a` should be resolved to t2.a.
    val newRight = Project(Seq(a, OuterReference(b).as(b.name), c), t2)
    val expected = t1.join(newRight, LateralJoin(Inner))
    checkAnalysis(plan, expected)
  }

  test("lateral join with qualified column names") {
    val t1a = a.withQualifier(Seq("t1"))
    val t2a = a.withQualifier(Seq("t2"))
    checkAnalysis(
      t1.as("t1").join(t0.select($"t1.a"), LateralJoin(Inner)),
      t1.join(Project(Seq(OuterReference(t1a).as(a.name)), t0), LateralJoin(Inner))
    )
    checkAnalysis(
      t1.as("t1").join(t2.as("t2").select($"t1.a", $"t2.a"), LateralJoin(Inner)),
      t1.join(Project(Seq(OuterReference(t1a).as(a.name), t2a), t2), LateralJoin(Inner))
    )
  }

  test("resolve nested lateral joins") {
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT a, c))
    val plan = t1.join(
      t2.join(t0.select('a, 'c), LateralJoin(Inner)),
      LateralJoin(Inner))
    val expected =
      t1.join(
        t2.join(
          Project(Seq(OuterReference(a).as(a.name), OuterReference(c).as(c.name)), t0),
          LateralJoin(Inner)),
      LateralJoin(Inner))
    checkAnalysis(plan, expected)
  }

  test("lateral join with unresolvable attributes") {
    // SELECT * FROM t1, LATERAL (SELECT a, c)
    assertAnalysisError(
      t1.join(t0.select('a, 'c), LateralJoin(Inner)),
      Seq("cannot resolve 'c' given input columns: []")
    )
    // SELECT * FROM t1, LATERAL (SELECT a, b, c, d FROM t2)
    assertAnalysisError(
      t1.join(t2.select('a, 'b, 'c, 'd), LateralJoin(Inner)),
      Seq("cannot resolve 'd' given input columns: [a, c]")
    )
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT t1.a))
    assertAnalysisError(
      t1.join(t2.join(t0.select($"t1.a"), LateralJoin(Inner)), LateralJoin(Inner)),
      Seq("cannot resolve 't1.a' given input columns: []")
    )
  }

  test("lateral joins with attributes referencing multiple query levels") {
    // SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT a, b))
    assertAnalysisError(
      t1.join(t2.join(t0.select('a, 'b), LateralJoin(Inner)), LateralJoin(Inner)),
      Seq("Found an outer column reference 'b' in a lateral subquery that is not present in " +
        "the preceding FROM items of its own query level: [a, c], which is not supported yet.")
    )
  }

  test("lateral join with star expansion") {
    val outerA = OuterReference(a.withQualifier(Seq("t1"))).as(a.name)
    val outerB = OuterReference(b.withQualifier(Seq("t1"))).as(b.name)
    val tpe = LateralJoin(Inner)
    // t1 JOIN LATERAL (SELECT *)
    checkAnalysis(
      t1.as("t1").join(t0.select(star()), tpe),
      t1.join(Project(Nil, t0), tpe)
    )
    // t1 JOIN LATERAL (SELECT t1.*)
    checkAnalysis(
      t1.as("t1").join(t0.select(star("t1")), tpe),
      t1.join(Project(Seq(outerA, outerB), t0), tpe)
    )
    // t1 JOIN LATERAL (SELECT * FROM t2)
    checkAnalysis(
      t1.as("t1").join(t2.select(star()), tpe),
      t1.join(Project(Seq(a, c), t2), tpe)
    )
    // t1 JOIN LATERAL (SELECT t1.*, t2.* FROM t2)
    checkAnalysis(
      t1.as("t1").join(t2.as("t2").select(star("t1"), star("t2")), tpe),
      t1.join(Project(Seq(outerA, outerB, a, c), t2), tpe)
    )
    // t1 JOIN LATERAL (SELECT t2.*)
    assertAnalysisError(
      t1.as("t1").join(t0.select(star("t2")), tpe),
      Seq("cannot resolve 't2.*' given input columns ''")
    )
  }

  test("lateral join with star expansion in functions") {
    val outerA = OuterReference(a.withQualifier(Seq("t1")))
    val outerB = OuterReference(b.withQualifier(Seq("t1")))
    val hash = Murmur3Hash(Seq(star("t1")), 42)
    val newHash = Murmur3Hash(Seq(outerA, outerB), 42)
    val array = CreateArray(Seq(star("t1")))
    val newArray = CreateArray(Seq(outerA, outerB))
    checkAnalysis(
      t1.as("t1").join(t0.select(hash), LateralJoin(Inner)),
      t1.join(t0.select(newHash.as(newHash.sql)), LateralJoin(Inner))
    )
    checkAnalysis(
      t1.as("t1").join(t0.select(array), LateralJoin(Inner)),
      t1.join(t0.select(newArray.as(newArray.sql)), LateralJoin(Inner))
    )
    assertAnalysisError(
      t1.as("t1").join(t0.select(Count(star("t1"))), LateralJoin(Inner)),
      Seq("Invalid usage of '*' in expression 'count'")
    )
  }

  test("lateral join with struct type") {
    val xa = GetStructField(OuterReference(x), 0, Some("a")).as(a.name)
    val ya = GetStructField(OuterReference(y), 0, Some("a")).as(a.name)
    checkAnalysis(
      t3.join(t0.select($"x.a", $"y.a"), LateralJoin(Inner)),
      t3.join(Project(Seq(xa, ya), t0), LateralJoin(Inner))
    )
    checkAnalysis(
      t3.join(t0.select(star("x")), LateralJoin(Inner)),
      t3.join(
        Project(Seq(GetStructField(OuterReference(x), 0).as(a.name)), t0),
        LateralJoin(Inner))
    )
    // Analyzer will try to resolve struct first before subquery alias.
    assertAnalysisError(
      t1.as("x").join(t3.select($"x.a", $"x.b"), LateralJoin(Inner)),
      Seq("No such struct field b in a")
    )
  }

  test("resolve missing references before resolving lateral references") {
    val func = abs(c).as("abs")
    checkAnalysis(
      t1.join(t2.select(func).where('a > 0), LateralJoin(Inner)),
      t1.join(t2.select(func, a).where(a > 0).select(func.toAttribute), LateralJoin(Inner))
    )
  }
}
