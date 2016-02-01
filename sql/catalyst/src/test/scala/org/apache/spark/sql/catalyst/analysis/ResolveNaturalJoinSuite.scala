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
import org.apache.spark.sql.types.StringType

class ResolveNaturalJoinSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  lazy val t1 = testRelation2.select('a, 'b)
  lazy val t2 = testRelation2.select('a, 'c)
  lazy val a = testRelation2.output(0)
  lazy val b = testRelation2.output(1)
  lazy val c = testRelation2.output(2)
  lazy val testRelation0 = LocalRelation(
    AttributeReference("d", StringType, nullable = false)(),
    AttributeReference("e", StringType, nullable = false)(),
    AttributeReference("f", StringType, nullable = false)())
  lazy val t3 = testRelation0.select('d, 'e)
  lazy  val t4 = testRelation0.select('d, 'f)
  lazy val d = testRelation0.output(0)
  lazy val e = testRelation0.output(1)
  lazy val f = testRelation0.output(2)
  lazy val nullableE = testRelation0.output(1).withNullability(true)
  lazy val nullableF = testRelation0.output(2).withNullability(true)

  test("natural inner join") {
    val plan = t1.join(t2, NaturalJoin(Inner), None)
    val expected = testRelation2.select(a, b).join(
      testRelation2.select(a, c), Inner, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural left join") {
    val plan = t1.join(t2, NaturalJoin(LeftOuter), None)
    val expected = testRelation2.select(a, b).join(
      testRelation2.select(a, c), LeftOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural right join") {
    val plan = t1.join(t2, NaturalJoin(RightOuter), None)
    val expected = testRelation2.select(a, b).join(
      testRelation2.select(a, c), RightOuter, Some(EqualTo(a, a))).select(a, b, c)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join") {
    val plan = t1.join(t2, NaturalJoin(FullOuter), None)
    val expected = testRelation2.select(a, b).join(testRelation2.select(
      a, c), FullOuter, Some(EqualTo(a, a))).select(Alias(Coalesce(Seq(a, a)), "a")(), b, c)
    checkAnalysis(plan, expected)
  }

  test("natural inner join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(Inner), None)
    val expected = testRelation0.select(d, e).join(
      testRelation0.select(d, f), Inner, Some(EqualTo(d, d))).select(d, e, f)
    checkAnalysis(plan, expected)
  }

  test("natural left join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(LeftOuter), None)
    val expected = testRelation0.select(d, e).join(
      testRelation0.select(d, f), LeftOuter, Some(EqualTo(d, d))).select(d, e, nullableF)
    checkAnalysis(plan, expected)
  }

  test("natural right join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(RightOuter), None)
    val expected = testRelation0.select(d, e).join(
      testRelation0.select(d, f), RightOuter, Some(EqualTo(d, d))).select(d, nullableE, f)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(FullOuter), None)
    val expected = testRelation0.select(d, e).join(testRelation0.select(d, f), FullOuter, Some(
      EqualTo(d, d))).select(Alias(Coalesce(Seq(d, d)), "d")(), nullableE, nullableF)
    checkAnalysis(plan, expected)
  }
}
