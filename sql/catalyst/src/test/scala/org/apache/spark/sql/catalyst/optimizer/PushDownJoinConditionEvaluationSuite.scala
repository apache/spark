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
import org.apache.spark.sql.catalyst.expressions.{Alias, Coalesce, Substring, Upper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PushDownJoinConditionEvaluationSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Push down join condition evaluation", Once,
        PushDownJoinConditionEvaluation,
        CollapseProject) :: Nil
  }

  private val testRelation = LocalRelation('a.string, 'b.int, 'c.int)
  private val testRelation1 = LocalRelation('d.string, 'e.int)
  private val x = testRelation.subquery('x)
  private val y = testRelation1.subquery('y)

  test("Push down join condition evaluation(String expressions)") {
    val joinType = Inner
    Seq(Upper("y.d".attr), Substring("y.d".attr, 1, 5)).foreach { udf =>
      val originalQuery = x.join(y, joinType, Option("x.a".attr === udf))
      val correctAnswer =
        x.join(y.select("y.d".attr, "y.e".attr, Alias(udf, udf.sql)()),
          joinType, Option("x.a".attr === s"`${udf.sql}`".attr))

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Push down join condition evaluation(null expressions)") {
    val joinType = Inner
    val udf = Coalesce(Seq("x.b".attr, "x.c".attr))
      val originalQuery = x.join(y, joinType, Option(udf === "y.e".attr))
      val correctAnswer =
        x.select("x.a".attr, "x.b".attr, "x.c".attr, Alias(udf, udf.sql)()).join(y,
          joinType, Option(s"`${udf.sql}`".attr === "y.e".attr))

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Negative case: all children are Attributes") {
    val condition = Option("x.a".attr === "y.d".attr)
    val originalQuery = x.join(y, Inner, condition)
    val correctAnswer = x.join(y, Inner, condition)

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Negative case: contains Literal") {
    val condition = Option("x.a".attr === "string")
    val originalQuery = x.join(y, Inner, condition)
    val correctAnswer = x.join(y, Inner, condition)

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}
