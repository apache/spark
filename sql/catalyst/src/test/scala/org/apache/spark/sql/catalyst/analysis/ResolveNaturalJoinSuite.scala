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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class ResolveNaturalJoinSuite extends AnalysisTest {
  lazy val a = 'a.string
  lazy val b = 'b.string
  lazy val c = 'c.string
  lazy val aNotNull = a.notNull
  lazy val bNotNull = b.notNull
  lazy val cNotNull = c.notNull
  lazy val r1 = LocalRelation(b, a)
  lazy val r2 = LocalRelation(c, a)
  lazy val r3 = LocalRelation(aNotNull, bNotNull)
  lazy val r4 = LocalRelation(cNotNull, bNotNull)
  lazy val innerJoin = Inner(false)

  test("natural/using inner join") {
    val naturalPlan = r1.join(r2, NaturalJoin(innerJoin), None)
    val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("a"))), None)
    val expected = r1.join(r2, innerJoin, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using left join") {
    val naturalPlan = r1.join(r2, NaturalJoin(LeftOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(LeftOuter, Seq(UnresolvedAttribute("a"))), None)
    val expected = r1.join(r2, LeftOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using right join") {
    val naturalPlan = r1.join(r2, NaturalJoin(RightOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(RightOuter, Seq(UnresolvedAttribute("a"))), None)
    val expected = r1.join(r2, RightOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using full outer join") {
    val naturalPlan = r1.join(r2, NaturalJoin(FullOuter), None)
    val usingPlan = r1.join(r2, UsingJoin(FullOuter, Seq(UnresolvedAttribute("a"))), None)
    val expected = r1.join(r2, FullOuter, Some(EqualTo(a, a))).select(
      Alias(Coalesce(Seq(a, a)), "a")(), b, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using inner join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(innerJoin), None)
    val usingPlan = r3.join(r4, UsingJoin(innerJoin, Seq(UnresolvedAttribute("b"))), None)
    val expected = r3.join(r4, innerJoin, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, cNotNull)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using left join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(LeftOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(LeftOuter, Seq(UnresolvedAttribute("b"))), None)
    val expected = r3.join(r4, LeftOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using right join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(RightOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(RightOuter, Seq(UnresolvedAttribute("b"))), None)
    val expected = r3.join(r4, RightOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, a, cNotNull)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("natural/using full outer join with no nullability") {
    val naturalPlan = r3.join(r4, NaturalJoin(FullOuter), None)
    val usingPlan = r3.join(r4, UsingJoin(FullOuter, Seq(UnresolvedAttribute("b"))), None)
    val expected = r3.join(r4, FullOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      Alias(Coalesce(Seq(b, b)), "b")(), a, c)
    checkAnalysis(naturalPlan, expected)
    checkAnalysis(usingPlan, expected)
  }

  test("using unresolved attribute") {
    val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("d"))), None)
    val error = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(usingPlan)
    }
    assert(error.message.contains(
      "using columns ['d] can not be resolved given input columns: [b, a, c]"))
  }

  test("using join with a case sensitive analyzer") {
    val expected = r1.join(r2, innerJoin, Some(EqualTo(a, a))).select(a, b, c)

    {
      val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("a"))), None)
      checkAnalysis(usingPlan, expected, caseSensitive = true)
    }

    {
      val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("A"))), None)
      assertAnalysisError(
        usingPlan,
        Seq("using columns ['A] can not be resolved given input columns: [b, a, c, a]"))
    }
  }

  test("using join with a case insensitive analyzer") {
    val expected = r1.join(r2, innerJoin, Some(EqualTo(a, a))).select(a, b, c)

    {
      val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("a"))), None)
      checkAnalysis(usingPlan, expected, caseSensitive = false)
    }

    {
      val usingPlan = r1.join(r2, UsingJoin(innerJoin, Seq(UnresolvedAttribute("A"))), None)
      checkAnalysis(usingPlan, expected, caseSensitive = false)
    }
  }
}
