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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class ResolveNaturalJoinSuite extends AnalysisTest {
  lazy val a = 'a.string
  lazy val b = 'b.string
  lazy val c = 'c.string
  lazy val d = 'd.struct('f1.int, 'f2.long)
  lazy val aNotNull = a.notNull
  lazy val bNotNull = b.notNull
  lazy val cNotNull = c.notNull
  lazy val r1 = LocalRelation(b, a)
  lazy val r2 = LocalRelation(c, a)
  lazy val r3 = LocalRelation(aNotNull, bNotNull)
  lazy val r4 = LocalRelation(cNotNull, bNotNull)
  lazy val r5 = LocalRelation(d)
  lazy val r6 = LocalRelation(d)

  test("natural/using inner join") {
    val naturalPlan = r1.join(r2, NaturalJoin(Inner), None)
    val usingPlan = r1.join(r2, UsingJoin(Inner, Seq("a")), None)
    val expected = r1.join(r2, Inner, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using left join") {
    val naturalPlan = r1.join(r2, NaturalJoin(LeftOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(LeftOuter, Seq("a")), None)
    val expected = r1.join(r2, LeftOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using right join") {
    val naturalPlan = r1.join(r2, NaturalJoin(RightOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(RightOuter, Seq("a")), None)
    val expected = r1.join(r2, RightOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using full outer join") {
    val naturalPlan = r1.join(r2, NaturalJoin(FullOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(FullOuter, Seq("a")), None)
    val expected = r1.join(r2, FullOuter, Some(EqualTo(a, a))).select(
      Alias(Coalesce(Seq(a, a)), "a")(), b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using inner join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(Inner), None)
    val usingPlan = r3.join(r4, UsingJoin(Inner, Seq("b")), None)
    val expected = r3.join(r4, Inner, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, cNotNull)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using left join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(LeftOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(LeftOuter, Seq("b")), None)
    val expected = r3.join(r4, LeftOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using right join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(RightOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(RightOuter, Seq("b")), None)
    val expected = r3.join(r4, RightOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, a, cNotNull)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using full outer join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(FullOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(FullOuter, Seq("b")), None)
    val expected = r3.join(r4, FullOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      Alias(Coalesce(Seq(b, b)), "b")(), a, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("using unresolved attribute") {
    assertAnalysisError(
      r1.join(r2, UsingJoin(Inner, Seq("d"))),
      "USING column `d` cannot be resolved on the left side of the join" :: Nil)
    assertAnalysisError(
      r1.join(r2, UsingJoin(Inner, Seq("b"))),
      "USING column `b` cannot be resolved on the right side of the join" :: Nil)
  }

  test("using join with a case sensitive analyzer") {
    val expected = r1.join(r2, Inner, Some(EqualTo(a, a))).select(a, b, c)

    val usingPlan = r1.join(r2, UsingJoin(Inner, Seq("a")), None)
    checkAnalysis(usingPlan, expected, caseSensitive = true)

    assertAnalysisError(
      r1.join(r2, UsingJoin(Inner, Seq("A"))),
      "USING column `A` cannot be resolved on the left side of the join" :: Nil)
  }

  test("using join on nested fields") {
    assertAnalysisError(
      r5.join(r6, UsingJoin(Inner, Seq("d.f1"))),
      "USING column `d.f1` cannot be resolved on the left side of the join. " +
        "The left-side columns: [d]" :: Nil)
  }

  test("using join with a case insensitive analyzer") {
    val expected = r1.join(r2, Inner, Some(EqualTo(a, a))).select(a, b, c)

    {
      val usingPlan = r1.join(r2, UsingJoin(Inner, Seq("a")), None)
      checkAnalysis(usingPlan, expected, caseSensitive = false)
    }

    {
      val usingPlan = r1.join(r2, UsingJoin(Inner, Seq("A")), None)
      checkAnalysis(usingPlan, expected, caseSensitive = false)
    }
  }
}
