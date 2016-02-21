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
  lazy val aNotNull = a.notNull
  lazy val bNotNull = b.notNull
  lazy val cNotNull = c.notNull
  lazy val r1 = LocalRelation(b, a)
  lazy val r2 = LocalRelation(c, a)
  lazy val r3 = LocalRelation(aNotNull, bNotNull)
  lazy val r4 = LocalRelation(cNotNull, bNotNull)

  test("natural inner join") {
    val plan = r1.join(r2, NaturalJoin(Inner), None)
    val expected = r1.join(r2, Inner, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural left join") {
    val plan = r1.join(r2, NaturalJoin(LeftOuter), None)
    val expected = r1.join(r2, LeftOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural right join") {
    val plan = r1.join(r2, NaturalJoin(RightOuter), None)
    val expected = r1.join(r2, RightOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join") {
    val plan = r1.join(r2, NaturalJoin(FullOuter), None)
    val expected = r1.join(r2, FullOuter, Some(EqualTo(a, a))).select(
      Alias(Coalesce(Seq(a, a)), "a")(), b, c)
    checkAnalysis(plan, expected)
  }

  test("natural inner join with no nullability") {
    val plan = r3.join(r4, NaturalJoin(Inner), None)
    val expected = r3.join(r4, Inner, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, cNotNull)
    checkAnalysis(plan, expected)
  }

  test("natural left join with no nullability") {
    val plan = r3.join(r4, NaturalJoin(LeftOuter), None)
    val expected = r3.join(r4, LeftOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, aNotNull, c)
    checkAnalysis(plan, expected)
  }

  test("natural right join with no nullability") {
    val plan = r3.join(r4, NaturalJoin(RightOuter), None)
    val expected = r3.join(r4, RightOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      bNotNull, a, cNotNull)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join with no nullability") {
    val plan = r3.join(r4, NaturalJoin(FullOuter), None)
    val expected = r3.join(r4, FullOuter, Some(EqualTo(bNotNull, bNotNull))).select(
      Alias(Coalesce(Seq(bNotNull, bNotNull)), "b")(), a, c)
    checkAnalysis(plan, expected)
  }
}
