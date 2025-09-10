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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{ExecutableDuringAnalysis, LocalRelation, LogicalPlan, SetVariable, SupportsSubquery, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXECUTE_IMMEDIATE, TreePattern}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Logical plan representing execute immediate query.
 *
 * @param sqlStmtStr the query expression (first child)
 * @param args parameters from USING clause (subsequent children)
 * @param targetVariables variables to store the result of the query
 */
case class UnresolvedExecuteImmediate(
    sqlStmtStr: Expression,
    args: Seq[Expression],
    targetVariables: Seq[Expression])
  extends UnresolvedLeafNode with SupportsSubquery {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * Logical plan representing a resolved execute immediate command that will recursively
 * invoke SQL execution.
 *
 * @param sqlStmtStr the resolved query expression
 * @param args parameters from USING clause
 * @param hasIntoClause whether this EXECUTE IMMEDIATE has an INTO clause
 */
case class ExecuteImmediateCommand(
    sqlStmtStr: Expression,
    args: Seq[Expression],
    hasIntoClause: Boolean = false)
  extends UnaryNode with ExecutableDuringAnalysis {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)

  override def child: LogicalPlan = LocalRelation(Nil, Nil)

  override def output: Seq[Attribute] = child.output

  override lazy val resolved: Boolean = {
    // ExecuteImmediateCommand should not be considered resolved until it has been
    // executed and replaced by ExecuteImmediateCommands rule.
    // This ensures that SetVariable waits for execution to complete.
    false
  }

  override def stageForExplain(): LogicalPlan = {
    // For EXPLAIN, just show the command without executing it
    copy()
  }

  override protected def withNewChildInternal(
      newChild: LogicalPlan): ExecuteImmediateCommand = {
    copy()
  }
}

/**
 * This rule resolves execute immediate query node into a command node
 * that will handle recursive SQL execution.
 */
class ResolveExecuteImmediate(
    val catalogManager: CatalogManager) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case node @ UnresolvedExecuteImmediate(sqlStmtStr, args, targetVariables) =>
        if (sqlStmtStr.resolved && targetVariables.forall(_.resolved) && args.forall(_.resolved)) {
          // All resolved - transform based on whether we have target variables
          if (targetVariables.nonEmpty) {
            // EXECUTE IMMEDIATE ... INTO should generate SetVariable plan
            // At this point, all targetVariables are resolved, so we only expect VariableReference
            // or Alias containing VariableReference
            val finalTargetVars = targetVariables.map {
              case alias: Alias =>
                // Extract the VariableReference from the alias
                alias.child match {
                  case varRef: VariableReference =>
                    // Use resolved VariableReference directly with canFold = false
                    varRef.copy(canFold = false)
                  case _ =>
                    throw QueryCompilationErrors.unsupportedParameterExpression(alias.child)
                }
              case varRef: VariableReference =>
                // Use resolved VariableReference directly with canFold = false
                varRef.copy(canFold = false)
              case other =>
                throw QueryCompilationErrors.unsupportedParameterExpression(other)
            }

            // Create SetVariable plan with the execute immediate query as source
            val sourceQuery = ExecuteImmediateCommand(sqlStmtStr, args, hasIntoClause = true)
            SetVariable(finalTargetVars, sourceQuery)
          } else {
            // Regular EXECUTE IMMEDIATE without INTO
            ExecuteImmediateCommand(sqlStmtStr, args, hasIntoClause = false)
          }
        } else {
          // Not all resolved yet - wait for next iteration
          node
        }
    }
}
