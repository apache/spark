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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.internal.SQLConf

/**
 * The [[ResolutionCheckRunner]] is used to run `resolutionChecks` on the logical plan.
 *
 * Important note: these checks are not always idempotent, and sometimes perform heavy network
 * operations.
 */
class ResolutionCheckRunner(resolutionChecks: Seq[LogicalPlan => Unit]) extends SQLConfHelper {

  /**
   * Runs the resolution checks on `plan`. Invokes all the checks for every subquery plan, and
   * eventually for the main query plan.
   */
  def runWithSubqueries(plan: LogicalPlan): Unit = {
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RUN_EXTENDED_RESOLUTION_CHECKS)) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        doRunWithSubqueries(plan)
      }
    }
  }

  private def doRunWithSubqueries(plan: LogicalPlan): Unit = {
    val planWithRewrittenSubqueries =
      plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
        case subqueryExpression: SubqueryExpression =>
          doRunWithSubqueries(subqueryExpression.plan)

          subqueryExpression
      }

    run(planWithRewrittenSubqueries)
  }

  private def run(plan: LogicalPlan): Unit = {
    for (check <- resolutionChecks) {
      check(plan)
    }
  }
}
