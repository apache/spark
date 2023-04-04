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

package org.apache.spark.sql.catalyst.optimizer

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Distinct, LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class CombineUnionedSubquerySuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Unions by combine subquery", Once, CombineUnionedSubquery) :: Nil
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.COMBINE_UNIONED_SUBQUERYS_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(6).map(i => Row(i, 2 * i, 3 * i)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)

  test("union two side are Plans without Filter") {
    val originalQuery1 = testRelation.union(testRelation)
    comparePlans(Optimize.execute(originalQuery1.analyze), originalQuery1)

    val originalQuery2 = testRelation.select(b, c).union(testRelation.select(b, c))
    comparePlans(Optimize.execute(originalQuery2.analyze), originalQuery2)
  }

  test("Union two subqueries and only one side with Filter") {
    val originalQuery1 = testRelation.union(testRelation.where(a < 2))
    comparePlans(Optimize.execute(originalQuery1), originalQuery1)

    val originalQuery2 = testRelation.where(a > 4).union(testRelation)
    comparePlans(Optimize.execute(originalQuery2), originalQuery2)
  }

  test("Union two subqueries with filters intersect") {
    val originalQuery1 = testRelation.where(a > 1).union(testRelation.where(a < 4))
    comparePlans(Optimize.execute(originalQuery1), originalQuery1)

    val originalQuery2 =
      testRelation.where(a > 3).union(testRelation.where(a < 4)).union(testRelation.where(a < 2))
    comparePlans(Optimize.execute(originalQuery2), originalQuery2)
  }

  test("Union two subqueries with filters do not intersect") {
    val originalQuery1 = testRelation.where(a > 4).union(testRelation.where(a < 2))
    val correctAnswer1 = testRelation.where(a > 4 || a < 2)
    comparePlans(Optimize.execute(originalQuery1), correctAnswer1)

    val originalQuery2 =
      testRelation.where(a > 4).select(b, c).union(testRelation.where(a < 2).select(b, c))
    val correctAnswer2 = testRelation.where(a > 4 || a < 2).select(b, c)
    comparePlans(Optimize.execute(originalQuery2), correctAnswer2)

    val originalQuery3 =
      testRelation.where(a > 4).select((b + 1).as("b1"), c).union(
        testRelation.where(a < 2).select((b + 1).as("b1"), c))
    val correctAnswer3 = testRelation.where(a > 4 || a < 2).select((b + 1).as("b1"), c)
    comparePlans(Optimize.execute(originalQuery3), correctAnswer3)

    val originalQuery4 =
      testRelation.select(a, b).where(a > 4).union(testRelation.select(a, b).where(a < 2))
    val correctAnswer4 = testRelation.select(a, b).where(a > 4 || a < 2)
    comparePlans(Optimize.execute(originalQuery4), correctAnswer4)

    val originalQuery5 =
      testRelation.select(a, (b + 1).as("b1")).where(a > 4).union(
        testRelation.select(a, (b + 1).as("b1")).where(a < 2))
    val correctAnswer5 = testRelation.select(a, (b + 1).as("b1")).where(a > 4 || a < 2)
    comparePlans(Optimize.execute(originalQuery5), correctAnswer5)

    val originalQuery6 =
      testRelation.where(a > 4).select((b + 1).as("b1"), c).union(
        testRelation.where(a < 2).select((b + 2).as("b1"), c))
    comparePlans(Optimize.execute(originalQuery6), originalQuery6)

    val originalQuery7 =
      testRelation.select(a, (b + 1).as("b1")).where(a > 4).union(
        testRelation.select(a, (b + 2).as("b1")).where(a < 2))
    comparePlans(Optimize.execute(originalQuery7), originalQuery7)
  }

  test("upstream union could be optimized") {
    val originalQuery1 =
      testRelation.where(a > 4).union(testRelation.where(a < 2)).union(testRelation.where(a < 3))
    val correctAnswer1 = testRelation.where(a > 4 || a < 2).union(testRelation.where(a < 3))
    comparePlans(Optimize.execute(originalQuery1), correctAnswer1)

    val originalQuery2 =
      testRelation.where(a > 4).union(testRelation.where(a < 2)).union(testRelation.select(a, b, c))
    val correctAnswer2 = testRelation.where(a > 4 || a < 2).union(testRelation.select(a, b, c))
    comparePlans(Optimize.execute(originalQuery2), correctAnswer2)

    val originalQuery3 =
      Union(Seq(testRelation.where(a > 4), testRelation.where(a < 2), testRelation.where(a < 3)))
    val correctAnswer3 = testRelation.where(a > 4 || a < 2).union(testRelation.where(a < 3))
    comparePlans(Optimize.execute(originalQuery3), correctAnswer3)

    val originalQuery4 =
      Union(Seq(testRelation.where(a < 3), testRelation.where(a < 2), testRelation.where(a > 4)))
    val correctAnswer4 = testRelation.where(a < 3 || a > 4).union(testRelation.where(a < 2))
    comparePlans(Optimize.execute(originalQuery4), correctAnswer4)

    val originalQuery5 =
      Union(Seq(testRelation.where(a > 1), testRelation.where(a < 2), testRelation.where(a > 4)))
    val correctAnswer5 = testRelation.where(a > 1).union(testRelation.where(a < 2 || a > 4))
    comparePlans(Optimize.execute(originalQuery5), correctAnswer5)
  }

  test("nested Union with filters intersect") {
    val originalQuery1 =
      testRelation.where(a > 4).union(testRelation.where(a < 2).union(testRelation.where(a < 3)))
    comparePlans(Optimize.execute(originalQuery1), originalQuery1)
  }

  test("Union the distinct between two filters") {
    val originalQuery1 = Distinct(testRelation.where(a > 4).union(testRelation.where(a < 2)))
    comparePlans(Optimize.execute(originalQuery1), originalQuery1)
  }

  test("Deduplicate the Union between two filters") {
    val originalQuery1 =
      Deduplicate(testRelation.output, testRelation.where(a > 4).union(testRelation.where(a < 2)))
    comparePlans(Optimize.execute(originalQuery1), originalQuery1)
  }
}
