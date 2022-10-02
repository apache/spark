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
import org.apache.spark.sql.catalyst.expressions.{Alias, Substring, Upper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PullOutComplexJoinConditionSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Pull out complex join condition", Once,
        PullOutComplexJoinCondition,
        CollapseProject) :: Nil
  }

  private val testRelation = LocalRelation($"a".string, $"b".int, $"c".int)
  private val testRelation1 = LocalRelation($"d".string, $"e".int)
  private val x = testRelation.subquery("x")
  private val y = testRelation1.subquery("y")

  test("Pull out join keys evaluation(String expressions)") {
    Seq(Upper("y.d".attr), Substring("y.d".attr, 1, 5)).foreach { udf =>
      val originalQuery = x.join(y, condition = Option($"a" === udf)).select($"a", $"e")
      val correctAnswer = x.select($"a", $"b", $"c")
        .join(y.select($"d", $"e", Alias(udf, udf.sql)()),
          condition = Option($"a" === s"`${udf.sql}`".attr)).select($"a", $"e")

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Pull out join condition contains other predicates") {
    val udf = Substring("y.d".attr, 1, 5)
    val originalQuery =
      x.join(y, condition = Option($"a" === udf && $"b" > $"e")).select($"a", $"e")
    val correctAnswer = x.select($"a", $"b", $"c")
      .join(y.select($"d", $"e", Alias(udf, udf.sql)()),
        condition = Option($"a" === s"`${udf.sql}`".attr && $"b" > $"e")).select($"a", $"e")

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Pull out EqualNullSafe join condition") {
    val udf = "x.b".attr + 1

    val originalQuery = x.join(y, condition = Option(udf <=> $"e")).select($"a", $"e")
    val correctAnswer = x.select($"a", $"b", $"c", Alias(udf, udf.sql)())
      .join(y.select($"d", $"e"), condition = Option(s"`${udf.sql}`".attr <=> s"e".attr))
      .select($"a", $"e")

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Pull out non-equality join conditions") {
    val udf = "x.b".attr + 1
    val originalQuery = x.join(y, condition = Option(udf > $"e")).select($"a", $"e")

    val correctAnswer = x.select($"a", $"b", $"c", Alias(udf, udf.sql)())
      .join(y.select($"d", $"e"), condition = Option(s"`${udf.sql}`".attr > s"e".attr))
      .select($"a", $"e")

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Negative case: all children are Attributes") {
    val originalQuery = x.join(y, condition = Option($"a" === $"d"))

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Negative case: contains Literal") {
    val originalQuery = x.join(y, condition = Option($"a" === "string"))

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }
}
