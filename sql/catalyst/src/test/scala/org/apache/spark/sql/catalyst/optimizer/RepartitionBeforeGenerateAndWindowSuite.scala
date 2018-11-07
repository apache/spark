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
import org.apache.spark.sql.catalyst.expressions.{Explode, Rand}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.StringType


class RepartitionBeforeGenerateAndWindowSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RepartitionBeforeGenerateAndWindow", Once, RepartitionBeforeGenerateAndWindow) :: Nil
  }

  test("add repartition operator before generate followed by window")  {
    val rel = LocalRelation('a.int, 'b.array(StringType))

    val a = rel.output(0)
    val b = rel.output(1)
    val partitionSpec1 = Seq(a)
    val orderSpec1 = Seq(b.asc)

    val query = rel
        .generate(Explode('b), outputNames = "explode" :: Nil)
        .window(Seq(count(a).as('cnt_a)), partitionSpec1, Nil)
        .analyze

    val expected = RepartitionByExpression(Seq('a), rel, numPartitions = 200)
      .generate(Explode('b), outputNames = "explode" :: Nil)
      .window(Seq(count(a).as('cnt_a)), partitionSpec1, Nil)
      .analyze

    val optimized = Optimizer.execute(query.analyze)
    comparePlans(optimized, expected)
  }
}
