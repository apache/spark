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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Exists, Expression, InSubquery, ListQuery, ScalarSubquery, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{ExecutableDuringAnalysis, LocalRelation, LogicalPlan, SetVariable, UnaryNode}
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
  extends UnresolvedLeafNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * Logical plan representing a resolved execute immediate command that will recursively
 * invoke SQL execution.
 *
 * @param sqlStmtStr the resolved query expression
 * @param args parameters from USING clause
 */
case class ExecuteImmediateCommand(
    sqlStmtStr: Expression,
    args: Seq[Expression])
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
      case UnresolvedExecuteImmediate(sqlStmtStr, args, targetVariables)
      if sqlStmtStr.resolved && targetVariables.forall(_.resolved) && args.forall(_.resolved) =>
        // Validate that USING clause expressions don't contain unsupported constructs
        validateUsingClauseExpressions(args)

        // Validate that query parameter is foldable (constant expression)
        validateSqlStmt(sqlStmtStr)

        // All resolved - transform based on whether we have target variables
        if (targetVariables.nonEmpty) {
          // EXECUTE IMMEDIATE ... INTO should generate SetVariable plan
          // SetVariable expects UnresolvedAttribute objects that ResolveSetVariable will resolve
          val finalTargetVars = targetVariables.map {
            case attr: UnresolvedAttribute =>
              // Keep as UnresolvedAttribute for ResolveSetVariable to handle
              attr
            case alias: Alias =>
              // Extract the UnresolvedAttribute from the alias
              alias.child match {
                case attr: UnresolvedAttribute =>
                  attr
                case varRef: VariableReference =>
                  // Convert back to UnresolvedAttribute for ResolveSetVariable
                  UnresolvedAttribute(varRef.originalNameParts)
                case _ =>
                  throw QueryCompilationErrors.unsupportedParameterExpression(alias.child)
              }
            case varRef: VariableReference =>
              // Convert back to UnresolvedAttribute for ResolveSetVariable
              UnresolvedAttribute(varRef.originalNameParts)
            case other =>
              throw QueryCompilationErrors.unsupportedParameterExpression(other)
          }

          // Create SetVariable plan with the execute immediate query as source
          val sourceQuery = ExecuteImmediateCommand(sqlStmtStr, args)
          SetVariable(finalTargetVars, sourceQuery)
        } else {
          // Regular EXECUTE IMMEDIATE without INTO
          ExecuteImmediateCommand(sqlStmtStr, args)
        }
      case other => other
          // Not all resolved yet - wait for next iteration
    }

  private def validateUsingClauseExpressions(args: Seq[Expression]): Unit = {
    args.foreach { expr =>
      // Check the expression and its children for unsupported constructs
      expr.foreach {
        case subquery: ScalarSubquery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(subquery)
        case exists: Exists =>
          throw QueryCompilationErrors.unsupportedParameterExpression(exists)
        case listQuery: ListQuery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(listQuery)
        case inSubquery: InSubquery =>
          throw QueryCompilationErrors.unsupportedParameterExpression(inSubquery)
        case _ => // Other expressions are fine
      }
    }
  }

  private def validateSqlStmt(sqlStmtStr: Expression): Unit = {
    // Only check for specific unsupported constructs like subqueries
    // Variable references and expressions like stringvar || 'hello' should be allowed
    sqlStmtStr.foreach {
      case subquery: ScalarSubquery =>
        throw QueryCompilationErrors.unsupportedParameterExpression(subquery)
      case exists: Exists =>
        throw QueryCompilationErrors.unsupportedParameterExpression(exists)
      case listQuery: ListQuery =>
        throw QueryCompilationErrors.unsupportedParameterExpression(listQuery)
      case inSubquery: InSubquery =>
        throw QueryCompilationErrors.unsupportedParameterExpression(inSubquery)
      case _ => // Other expressions including variables and concatenations are fine
    }
  }
}
