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
    AttributeReference("a", StringType, nullable = false)(),
    AttributeReference("b", StringType, nullable = false)(),
    AttributeReference("c", StringType, nullable = false)())
  lazy val tt1 = testRelation0.select('a, 'b)
  lazy  val tt2 = testRelation0.select('a, 'c)
  lazy val aa = testRelation0.output(0)
  lazy val bb = testRelation0.output(1)
  lazy val cc = testRelation0.output(2)
  lazy val trueB = testRelation0.output(1).withNullability(true)
  lazy val trueC = testRelation0.output(2).withNullability(true)

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
    val plan = tt1.join(tt2, NaturalJoin(Inner), None)
    val expected = testRelation0.select(aa, bb).join(
      testRelation0.select(aa, cc), Inner, Some(EqualTo(aa, aa))).select(aa, bb, cc)
    checkAnalysis(plan, expected)
  }

  test("natural left join with no nullability") {
    val plan = tt1.join(tt2, NaturalJoin(LeftOuter), None)
    val expected = testRelation0.select(aa, bb).join(
      testRelation0.select(aa, cc), LeftOuter, Some(EqualTo(aa, aa))).select(aa, bb, trueC)
    checkAnalysis(plan, expected)
  }

  test("natural right join with no nullability") {
    val plan = tt1.join(tt2, NaturalJoin(RightOuter), None)
    val expected = testRelation0.select(aa, bb).join(
      testRelation0.select(aa, cc), RightOuter, Some(EqualTo(aa, aa))).select(aa, trueB, cc)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join with no nullability") {
    val plan = tt1.join(tt2, NaturalJoin(FullOuter), None)
    val expected = testRelation0.select(aa, bb).join(
      testRelation0.select(aa, cc), FullOuter, Some(EqualTo(aa, aa))).select(
      Alias(Coalesce(Seq(aa, aa)), "a")(), trueB, trueC)
    checkAnalysis(plan, expected)
  }
}
