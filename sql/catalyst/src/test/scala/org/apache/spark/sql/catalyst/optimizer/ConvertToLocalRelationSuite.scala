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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class ConvertToLocalRelationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("LocalRelation", FixedPoint(100),
        ConvertToLocalRelation) :: Nil
  }

  test("Project on LocalRelation should be turned into a single LocalRelation") {
    val testRelation = LocalRelation(
      LocalRelation('a.int, 'b.int).output,
      Row(1, 2) ::
      Row(4, 5) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation('a1.int, 'b1.int).output,
      Row(1, 3) ::
      Row(4, 6) :: Nil)

    val projectOnLocal = testRelation.select(
      UnresolvedAttribute("a").as("a1"),
      (UnresolvedAttribute("b") + 1).as("b1"))

    val optimized = Optimize(projectOnLocal.analyze)

    comparePlans(optimized, correctAnswer)
  }

}
