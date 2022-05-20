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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class PullOutComplexJoinKeysSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PullOutComplexJoinKeys", FixedPoint(1),
      PullOutComplexJoinKeys,
      CollapseProject) :: Nil
  }

  val testRelation1 = LocalRelation($"a".int, $"b".int)
  val testRelation2 = LocalRelation($"x".int, $"y".int)

  test("pull out complex join keys") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // join
      //   a (complex join key)
      //   b
      val plan1 = testRelation1.join(testRelation2, condition = Some($"a" % 2 === $"x"))
      val expected1 = testRelation1.select(($"a" % 2) as "_complexjoinkey_0").join(
        testRelation2, condition = Some($"_complexjoinkey_0" === $"x"))
      comparePlans(Optimize.execute(plan1.analyze), expected1.analyze)

      // join
      //   project
      //     a (complex join key)
      //   b
      val plan2 = testRelation1.select($"a").join(
        testRelation2, condition = Some($"a" % 2 === $"x"))
      val expected2 = testRelation1.select(($"a" % 2) as "_complexjoinkey_0")
        .join(testRelation2, condition = Some($"_complexjoinkey_0" === $"x"))
      comparePlans(Optimize.execute(plan2.analyze), expected2.analyze)

      // join
      //   a (two complex join keys)
      //   b
      val plan3 = testRelation1.join(testRelation2,
        condition = Some($"a" % 2 === $"x" && $"b" % 3 === $"y"))
      val expected3 = testRelation1.select(($"a" % 2) as "_complexjoinkey_0",
        ($"b" % 3) as "_complexjoinkey_1").join(testRelation2,
        condition = Some($"_complexjoinkey_0" === $"x" && $"_complexjoinkey_1" === $"y"))
      comparePlans(Optimize.execute(plan3.analyze), expected3.analyze)

      // join
      //   a
      //   b (complex join key)
      val plan4 = testRelation1.join(testRelation2, condition = Some($"a" === $"x" % 2))
      val expected4 = testRelation1.join(testRelation2.select(($"x" % 2) as "_complexjoinkey_0"),
        condition = Some($"a" === $"_complexjoinkey_0"))
      comparePlans(Optimize.execute(plan4.analyze), expected4.analyze)

      // join
      //   a (complex join key)
      //   b (complex join key)
      val plan5 = testRelation1.join(testRelation2, condition = Some($"a" % 2 === $"x" % 3))
      val expected5 = testRelation1.select(($"a" % 2) as "_complexjoinkey_0").join(
        testRelation2.select(($"x" % 3) as "_complexjoinkey_1"),
        condition = Some($"_complexjoinkey_0" === $"_complexjoinkey_1"))
      comparePlans(Optimize.execute(plan5.analyze), expected5.analyze)
    }
  }

  test("do not pull out complex join keys") {
    // can broadcast
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val p1 = testRelation1.join(testRelation2, condition = Some($"a" % 2 === $"x")).analyze
      comparePlans(Optimize.execute(p1), p1)

      val p2 = testRelation1.join(testRelation2, condition = Some($"a" === $"x" % 2)).analyze
      comparePlans(Optimize.execute(p2), p2)
    }

    // not contains complex expression
    val p1 = testRelation1.subquery("t1").join(
      testRelation2.subquery("t2"), condition = Some($"a" === $"x"))
    comparePlans(Optimize.execute(p1.analyze), p1.analyze)

    // not a equi-join
    val p2 = testRelation1.subquery("t1").join(testRelation2.subquery("t2"))
    comparePlans(Optimize.execute(p2.analyze), p2.analyze)
  }
}
