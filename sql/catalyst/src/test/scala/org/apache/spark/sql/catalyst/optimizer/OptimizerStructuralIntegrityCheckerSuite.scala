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

import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf


class OptimizerStructuralIntegrityCheckerSuite extends PlanTest {

  object OptimizeRuleBreakSI extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(projectList, child) =>
        val newAttr = UnresolvedAttribute("unresolvedAttr")
        Project(projectList ++ Seq(newAttr), child)
    }
  }

  object Optimize extends Optimizer(
    new SessionCatalog(
      new InMemoryCatalog,
      EmptyFunctionRegistry,
      new SQLConf())) {
    val newBatch = Batch("OptimizeRuleBreakSI", Once, OptimizeRuleBreakSI)
    override def defaultBatches: Seq[Batch] = Seq(newBatch) ++ super.defaultBatches
  }

  test("check for invalid plan after execution of rule") {
    val analyzed = Project(Alias(Literal(10), "attr")() :: Nil, OneRowRelation()).analyze
    assert(analyzed.resolved)
    val message = intercept[TreeNodeException[LogicalPlan]] {
      Optimize.execute(analyzed)
    }.getMessage
    val ruleName = OptimizeRuleBreakSI.ruleName
    assert(message.contains(s"After applying rule $ruleName in batch OptimizeRuleBreakSI"))
    assert(message.contains("the structural integrity of the plan is broken"))
  }
}
