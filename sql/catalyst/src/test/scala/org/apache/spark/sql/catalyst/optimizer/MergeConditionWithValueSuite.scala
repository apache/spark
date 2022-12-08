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

import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, Expression, IsNotNull, Literal, Or}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class MergeConditionWithValueSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Seq(Batch("MergeConditionWithValue", Once, MergeConditionWithValue))
  }

  test("SPARK-40099: Merge adjacent CaseWhen branches if their values are the same") {
    val branches = Seq[(Expression, Expression)]((IsNotNull(Literal("a")),
      Literal(1)), (IsNotNull(Literal("b")), Literal(1)))
    val in = Project(Seq(Alias(CaseWhen(branches, Option.apply(Literal(3))), "c")()),
      LocalRelation(Nil))

    val plan = Optimize.execute(in).asInstanceOf[Project]
    assert(plan.projectList.size == 1)
    assert(plan.projectList(0).children.size == 1)
    assert(plan.projectList(0).children(0).children.count(_.isInstanceOf[Or]) > 0)
  }
}
