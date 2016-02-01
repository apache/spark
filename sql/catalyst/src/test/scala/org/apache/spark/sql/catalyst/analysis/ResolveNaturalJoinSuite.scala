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
  lazy val r1 = LocalRelation(
    AttributeReference("r1_a", StringType, nullable = true)(),
    AttributeReference("r1_b", StringType, nullable = true)(),
    AttributeReference("r1_c", StringType, nullable = true)())
  lazy val r2 = LocalRelation(
    AttributeReference("r2_a", StringType, nullable = false)(),
    AttributeReference("r2_b", StringType, nullable = false)(),
    AttributeReference("r2_c", StringType, nullable = false)())
  lazy val t1 = r1.select('r1_a, 'r1_b)
  lazy val t2 = r1.select('r1_a, 'r1_c)
  lazy val r1a = r1.output(0)
  lazy val r1b = r1.output(1)
  lazy val r1c = r1.output(2)
  lazy val t3 = r2.select('r2_a, 'r2_b)
  lazy val t4 = r2.select('r2_a, 'r2_c)
  lazy val r2a = r2.output(0)
  lazy val r2b = r2.output(1)
  lazy val r2c = r2.output(2)
  lazy val nullableR2B = r2.output(1).withNullability(true)
  lazy val nullableR2C = r2.output(2).withNullability(true)

  test("natural inner join") {
    val plan = t1.join(t2, NaturalJoin(Inner), None)
    val expected = r1.select(r1a, r1b).join(
      r1.select(r1a, r1c), Inner, Some(EqualTo(r1a, r1a))).select(r1a, r1b, r1c)
    checkAnalysis(plan, expected)
  }

  test("natural left join") {
    val plan = t1.join(t2, NaturalJoin(LeftOuter), None)
    val expected = r1.select(r1a, r1b).join(
      r1.select(r1a, r1c), LeftOuter, Some(EqualTo(r1a, r1a))).select(r1a, r1b, r1c)
    checkAnalysis(plan, expected)
  }

  test("natural right join") {
    val plan = t1.join(t2, NaturalJoin(RightOuter), None)
    val expected = r1.select(r1a, r1b).join(
      r1.select(r1a, r1c), RightOuter, Some(EqualTo(r1a, r1a))).select(r1a, r1b, r1c)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join") {
    val plan = t1.join(t2, NaturalJoin(FullOuter), None)
    val expected = r1.select(r1a, r1b).join(r1.select(r1a, r1c), FullOuter, Some(
      EqualTo(r1a, r1a))).select(Alias(Coalesce(Seq(r1a, r1a)), "r1_a")(), r1b, r1c)
    checkAnalysis(plan, expected)
  }

  test("natural inner join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(Inner), None)
    val expected = r2.select(r2a, r2b).join(
      r2.select(r2a, r2c), Inner, Some(EqualTo(r2a, r2a))).select(r2a, r2b, r2c)
    checkAnalysis(plan, expected)
  }

  test("natural left join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(LeftOuter), None)
    val expected = r2.select(r2a, r2b).join(
      r2.select(r2a, r2c), LeftOuter, Some(EqualTo(r2a, r2a))).select(r2a, r2b, nullableR2C)
    checkAnalysis(plan, expected)
  }

  test("natural right join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(RightOuter), None)
    val expected = r2.select(r2a, r2b).join(
      r2.select(r2a, r2c), RightOuter, Some(EqualTo(r2a, r2a))).select(r2a, nullableR2B, r2c)
    checkAnalysis(plan, expected)
  }

  test("natural full outer join with no nullability") {
    val plan = t3.join(t4, NaturalJoin(FullOuter), None)
    val expected = r2.select(r2a, r2b).join(r2.select(r2a, r2c), FullOuter, Some(EqualTo(
      r2a, r2a))).select(Alias(Coalesce(Seq(r2a, r2a)), "r2_a")(), nullableR2B, nullableR2C)
    checkAnalysis(plan, expected)
  }
}
