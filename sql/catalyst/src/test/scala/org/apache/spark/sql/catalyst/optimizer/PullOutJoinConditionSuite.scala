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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Coalesce, IsNull, Literal, Substring, Upper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class PullOutJoinConditionSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Pull out join condition", Once,
        PullOutJoinCondition,
        CollapseProject) :: Nil
  }

  private val testRelation = LocalRelation('a.string, 'b.int, 'c.int)
  private val testRelation1 = LocalRelation('d.string, 'e.int)
  private val x = testRelation.subquery('x)
  private val y = testRelation1.subquery('y)

  test("Push down join keys evaluation(String expressions)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val joinType = Inner
      Seq(Upper("y.d".attr), Substring("y.d".attr, 1, 5)).foreach { udf =>
        val originalQuery = x.join(y, joinType, Option("x.a".attr === udf))
          .select("x.a".attr, "y.e".attr)
        val correctAnswer = x.select("x.a".attr, "x.b".attr, "x.c".attr)
          .join(y.select("y.d".attr, "y.e".attr, Alias(udf, "_right_complex_expr0")()),
            joinType,
            Option("x.a".attr === "_right_complex_expr0".attr)).select("x.a".attr, "y.e".attr)
        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
      }
    }
  }

  test("Push down join keys evaluation(null expressions)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val joinType = Inner
      val udf = Coalesce(Seq("x.b".attr, "x.c".attr))
      val originalQuery = x.join(y, joinType, Option(udf === "y.e".attr))
        .select("x.a".attr, "y.e".attr)
      val correctAnswer =
        x.select("x.a".attr, "x.b".attr, "x.c".attr, Alias(udf, "_left_complex_expr0")()).join(
          y.select("y.d".attr, "y.e".attr),
          joinType, Option("_left_complex_expr0".attr === "y.e".attr))
          .select("x.a".attr, "y.e".attr)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Join condition contains other predicates") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val udf = Substring("y.d".attr, 1, 5)
      val joinType = Inner
      val originalQuery = x.join(y, joinType, Option("x.a".attr === udf && "x.b".attr > "y.e".attr))
        .select("x.a".attr, "y.e".attr)
      val correctAnswer =
        x.select("x.a".attr, "x.b".attr, "x.c".attr).join(
          y.select("y.d".attr, "y.e".attr, Alias(udf, "_right_complex_expr0")()),
          joinType, Option("x.a".attr === "_right_complex_expr0".attr && "x.b".attr > "y.e".attr))
          .select("x.a".attr, "y.e".attr)

        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Push down EqualNullSafe join condition") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val joinType = Inner
      val udf = "x.b".attr + 1
      val originalQuery = x.join(y, joinType, Option(udf <=> "y.e".attr))
        .select("x.a".attr, "y.e".attr)
      val correctAnswer =
        x.select("x.a".attr, "x.b".attr, "x.c".attr,
          Alias(Coalesce(Seq(udf, Literal(0))), "_left_complex_expr0")(),
          Alias(IsNull(udf), "_left_complex_expr1")()).join(
          y.select("y.d".attr, "y.e".attr,
            Alias(Coalesce(Seq("y.e".attr, Literal(0))), "_right_complex_expr0")(),
            Alias(IsNull("y.e".attr), "_right_complex_expr1")()),
          joinType, Option("_left_complex_expr0".attr === "_right_complex_expr0".attr &&
            "_left_complex_expr1".attr === "_right_complex_expr1".attr))
          .select("x.a".attr, "y.e".attr)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Negative case: non-equality join keys") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val joinType = Inner
      val udf = "x.b".attr + 1
      val originalQuery = x.join(y, joinType, Option(udf > "y.e".attr))
        .select("x.a".attr, "y.e".attr)

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("Negative case: all children are Attributes") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val condition = Option("x.a".attr === "y.d".attr)
      val originalQuery = x.join(y, Inner, condition)
      val correctAnswer = x.join(y, Inner, condition)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Negative case: contains Literal") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val condition = Option("x.a".attr === "string")
      val originalQuery = x.join(y, Inner, condition)
      val correctAnswer = x.join(y, Inner, condition)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Negative case: BroadcastHashJoin") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      Seq(Upper("y.d".attr), Substring("y.d".attr, 1, 5)).foreach { udf =>
        val originalQuery = x.join(y, Inner, Option("x.a".attr === udf))
          .select("x.a".attr, "y.e".attr)
        comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
      }
    }
  }
}
