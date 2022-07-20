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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.types.IntegerType

class InnerJoinEliminationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
        Batch("Outer Join Elimination", Once,
          EliminateInnerJoin,
          PushPredicateThroughJoin) :: Nil
  }

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()
  private val c = AttributeReference("c", IntegerType)()
  private val d = AttributeReference("d", IntegerType)()
  private val e = AttributeReference("e", IntegerType)()
  private val f = AttributeReference("f", IntegerType)()

  private val testRelation1 = StatsTestPlan(
    outputList = Seq(a, b, c),
    attributeStats = AttributeMap.empty,
    rowCount = 100000,
    size = Some(100000 * 3 * (8 + 4)))

  private val testRelation2 = StatsTestPlan(
    outputList = Seq(d, e, f),
    attributeStats = AttributeMap.empty,
    rowCount = 1000,
    size = Some(1000 * 3 * (8 + 4)))

  private val x = testRelation1.subquery('x)
  private val y = testRelation2.groupBy('d, 'e)('d, 'e).subquery('y)

  test("Inner join to left semi join") {
    comparePlans(
      Optimize.execute(
        x.join(y, Inner,
          Option("x.a".attr === "y.d".attr && "x.b".attr === "y.e".attr)).select('a).analyze),
      x.join(y, LeftSemi,
        Option("x.a".attr === "y.d".attr && "x.b".attr === "y.e".attr)).select('a).analyze)
  }

  test("Inner join to left semi join and rightKeys has duplicate values") {
    val joinCondition = Option("x.a".attr === "y.d".attr && "x.b".attr === "y.e".attr &&
      "x.c".attr === "y.e".attr)
    comparePlans(
      Optimize.execute(x.join(y, Inner, joinCondition).select('a).analyze),
      x.join(y, LeftSemi, joinCondition).select('a).analyze)
  }

  test("Inner join to left semi join and right output has duplicate values") {
    val right = testRelation2.groupBy('d, 'e)('d, 'e, 'e).subquery('y)
    val joinCondition = Option("x.a".attr === "y.d".attr && "x.b".attr === "y.e".attr &&
      "x.c".attr === "y.e".attr)
    comparePlans(
      Optimize.execute(x.join(right, Inner, joinCondition).select('a).analyze),
      x.join(right, LeftSemi, joinCondition).select('a).analyze)
  }

  test("Inner join to left semi join and should sort rightKeys") {
    val joinCondition = Option("x.b".attr === "y.e".attr && "x.c".attr === "y.e".attr &&
      "x.a".attr === "y.d".attr)
    comparePlans(
      Optimize.execute(x.join(y, Inner, joinCondition).select('a).analyze),
      x.join(y, LeftSemi, joinCondition).select('a).analyze)
  }

  test("Negative case: contains both side of attribute") {
    val originPlan = x.join(y, Inner, Option("x.a".attr === "y.d".attr)).select('a, 'd).analyze
    comparePlans(Optimize.execute(originPlan), originPlan)
  }

  test("Negative case: right side can not guarantee unique") {
    val originPlan = x.join(y, Inner, Option("x.a".attr === "y.d".attr)).select('a).analyze
    comparePlans(Optimize.execute(originPlan), originPlan)
  }

  test("Negative case: join condition is not an attribute") {
    val originPlan = x.join(y, Inner, Option("x.a".attr + 1 === "y.d".attr + 1)).select('a).analyze
    comparePlans(Optimize.execute(originPlan), originPlan)
  }

  test("Negative case: BroadcastBuildSide is BuildLeft") {
    val testRelation3 = StatsTestPlan(
      outputList = Seq(a, b, c),
      attributeStats = AttributeMap.empty,
      rowCount = 100,
      size = Some(100 * 3 * (8 + 4))).as("x")

    val originPlan =
      testRelation3.join(y, Inner, Option("x.a".attr === "y.d".attr)).select('a).analyze
    comparePlans(Optimize.execute(originPlan), originPlan)
  }

}
