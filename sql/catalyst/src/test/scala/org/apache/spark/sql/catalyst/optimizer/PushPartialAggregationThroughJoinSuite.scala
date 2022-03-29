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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class PushPartialAggregationThroughJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimizer Batch", FixedPoint(100),
      PushPartialAggregationThroughJoin) :: Nil
  }

  private val testRelation1 = LocalRelation('a.int, 'b.int, 'c.int)
  private val testRelation2 = LocalRelation('x.int, 'y.long)

  test("Basic case") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'x))
        .groupBy('a, 'b, 'y)('a, 'b, 'y)

      val correctAnswer = Aggregate(Seq('a, 'b), Seq('a, 'b), true, testRelation1.where('c > 0))
        .join(Aggregate(Seq('y, 'x), Seq('y, 'x), true, testRelation2), Inner, Some('a === 'x))
        .groupBy('a, 'b, 'y)('a, 'b, 'y)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("AggregateExpressions contains alias") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'x))
        .groupBy('a, 'b, 'y)('a.as("alias_a"), 'b, 'y)

      val correctAnswer = Aggregate(Seq('a, 'b), Seq('a, 'b), true, testRelation1.where('c > 0))
        .join(Aggregate(Seq('y, 'x), Seq('y, 'x), true, testRelation2), Inner, Some('a === 'x))
        .groupBy('a, 'b, 'y)('a.as("alias_a"), 'b, 'y)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Complex join condition") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b)

      val correctAnswer = Aggregate(Seq('a, 'b), Seq('a, 'b), true, testRelation1.where('c > 0))
        .join(Aggregate(Seq('y), Seq('y), true, testRelation2), Inner, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("AggregateExpressions contains Literal") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b, 1)

      val correctAnswer = Aggregate(Seq('a, 'b), Seq('a, 'b), true, testRelation1.where('c > 0))
        .join(Aggregate(Seq('y), Seq('y), true, testRelation2), Inner, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b, 1)

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Push partial only aggregate through join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = Aggregate(Seq('c, 'y), Seq('c, 'y), true, testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'x)))

      val correctAnswer = Project(Seq('c, 'y),
        Aggregate(Seq('c, 'a), Seq('c, 'a), true, testRelation1.where('c > 0))
        .join(Aggregate(Seq('y, 'x), Seq('y, 'x), true, testRelation2), Inner, Some('a === 'x)))

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("Unsupported cast: contains AggregateFunction") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, Inner, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b, sum('c))

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("Unsupported cast: outer join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val originalQuery = testRelation1.where('c > 0)
        .join(testRelation2, LeftOuter, Some('a === 'y))
        .groupBy('a, 'b)('a.as("alias_a"), 'b)

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("Unsupported cast: broadcast join") {
    val originalQuery = testRelation1.where('c > 0)
      .join(testRelation2, Inner, Some('a === 'x))
      .groupBy('a, 'b, 'y)('a, 'b, 'y)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }
}
