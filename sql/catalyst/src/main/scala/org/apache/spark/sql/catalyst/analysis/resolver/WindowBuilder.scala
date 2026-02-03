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

import com.databricks.sql.DatabricksSQLConf

import org.apache.spark.sql.catalyst.expressions.{ExprUtils, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Window}
import org.apache.spark.sql.internal.SQLConf

object WindowBuilder {

  /**
   * Builds a [[Window]] operator by delegating the resolution to [[WindowResolver]]. If the base
   * operator of the window is an [[Aggregate]], validate it using
   * [[ExprUtils.assertValidAggregation]].
   */
  def buildWindow(
      windowResolver: WindowResolver,
      originalOperator: LogicalPlan,
      resolvedProjectList: Seq[NamedExpression],
      childOperator: LogicalPlan,
      hasCorrelatedScalarSubqueryExpressions: Boolean,
      cteRegistry: CteRegistry): (Window, ResolvedProjectList) = {

    failIfIllegalRecursiveSelfReference(cteRegistry)

    val (window, windowBaseOperator) = windowResolver.buildWindow(
      originalOperator = originalOperator,
      originalOutputList = resolvedProjectList,
      sourceOperatorChild = childOperator,
      hasCorrelatedScalarSubqueryExpressions = hasCorrelatedScalarSubqueryExpressions
    )

    windowBaseOperator match {
      case aggregate: Aggregate =>
        // TODO: This validation function does a post-traversal. This is discouraged in
        // single-pass Analyzer.
        ExprUtils.assertValidAggregation(aggregate)
      case _ =>
    }

    val resolvedWindowProjectList = ResolvedProjectList(
      expressions = window.projectList,
      hasAggregateExpressionsOutsideWindow = false,
      hasWindowExpressions = false,
      hasLateralColumnAlias = false,
      hasCorrelatedScalarSubqueryExpressions = false,
      aggregateListAliases = Seq.empty
    )

    (window, resolvedWindowProjectList)
  }

  /**
   * Validates that recursive CTE self-references are not present when window functions are used,
   * if the corresponding flag is enabled. The SQL standard doesn't clearly define whether
   * recursive CTEs can contain window functions over the recursive reference, so this behavior
   * is controlled by a configuration flag.
   */
  private def failIfIllegalRecursiveSelfReference(cteRegistry: CteRegistry): Unit = {
    if (SQLConf.get.getConf(
      DatabricksSQLConf.DISABLE_SORT_AND_WINDOW_FUNCTIONS_WITH_RECURSIVE_REFERENCE
    )) {
      cteRegistry.currentScope.failIfScopeHasSelfReference()
    }
  }
}


