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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util._

/**
 * Tests for the sameResult function of [[LogicalPlan]].
 */
class SameResultSuite extends SparkFunSuite {
  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation2 = LocalRelation('a.int, 'b.int, 'c.int)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("EliminateResolvedHint", Once, EliminateResolvedHint) :: Nil
  }

  def assertSameResult(a: LogicalPlan, b: LogicalPlan, result: Boolean = true): Unit = {
    val aAnalyzed = a.analyze
    val bAnalyzed = b.analyze

    if (aAnalyzed.sameResult(bAnalyzed) != result) {
      val comparison = sideBySide(aAnalyzed.toString, bAnalyzed.toString).mkString("\n")
      fail(s"Plans should return sameResult = $result\n$comparison")
    }
  }

  test("relations") {
    assertSameResult(testRelation, testRelation2)
  }

  test("projections") {
    assertSameResult(testRelation.select('a), testRelation2.select('a))
    assertSameResult(testRelation.select('b), testRelation2.select('b))
    assertSameResult(testRelation.select('a, 'b), testRelation2.select('a, 'b))
    assertSameResult(testRelation.select('b, 'a), testRelation2.select('b, 'a))

    assertSameResult(testRelation, testRelation2.select('a), result = false)
    assertSameResult(testRelation.select('b, 'a), testRelation2.select('a, 'b), result = false)
  }

  test("filters") {
    assertSameResult(testRelation.where('a === 'b), testRelation2.where('a === 'b))
  }

  test("sorts") {
    assertSameResult(testRelation.orderBy('a.asc), testRelation2.orderBy('a.asc))
  }

  test("union") {
    assertSameResult(Union(Seq(testRelation, testRelation2)),
      Union(Seq(testRelation2, testRelation)))
  }

  test("hint") {
    val df1 = testRelation.join(ResolvedHint(testRelation))
    val df2 = testRelation.join(testRelation)
    assertSameResult(df1, df2)
  }

  test("join hint") {
    val df1 = testRelation.join(testRelation.hint("broadcast"))
    val df2 = testRelation.join(testRelation)
    val df1Optimized = Optimize.execute(df1.analyze)
    val df2Optimized = Optimize.execute(df2.analyze)
    assertSameResult(df1Optimized, df2Optimized)
  }
}
