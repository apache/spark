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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.catalog.{SqlScriptingContextManager => SqlScriptingContextManagerTrait}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, ExecuteImmediateCommand, LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.trees.TreePattern.EXECUTE_IMMEDIATE
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.v2.ParameterBindingUtils
import org.apache.spark.sql.types.StringType

/**
 * Analysis rule that resolves EXECUTE IMMEDIATE statements during analysis, parsing and analyzing
 * the dynamic SQL and replacing the node with the resolved inner plan: a query is spliced, a
 * command payload is wrapped in [[ExecuteImmediateCommand]] to run at the execution level, and an
 * INTO clause becomes a [[SetVariable]]. Command payloads are not executed during analysis; nodes
 * that execute during analysis, such as CALL, are an exception (see `resolveInnerStatement`).
 *
 * {{{
 *   EXECUTE IMMEDIATE 'INSERT INTO t VALUES (?)' USING 1  =>  ExecuteImmediateCommand(...)
 *   EXECUTE IMMEDIATE 'SELECT ?' USING 1  =>  analyzed query, spliced and run lazily
 *   EXECUTE IMMEDIATE 'SELECT 1' INTO v  =>  SetVariable over the analyzed query
 * }}}
 *
 * When sub-expressions are not yet resolved, the node is returned unchanged so that the
 * fixed-point analyzer re-applies this rule on the next iteration.
 */
case class ResolveExecuteImmediate(sparkSession: SparkSession, catalogManager: CatalogManager)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case node @ UnresolvedExecuteImmediate(sqlStmtStr, args, targetVariables) =>
        if (sqlStmtStr.resolved && targetVariables.forall(_.resolved) && args.forall(_.resolved)) {
          ResolveExecuteImmediate.resolveExecuteImmediate(
            sparkSession, sqlStmtStr, args, targetVariables)
        } else {
          node
        }
    }
  }
}

/**
 * Companion object containing shared resolution logic for `EXECUTE IMMEDIATE` statements.
 */
object ResolveExecuteImmediate {

  /**
   * Resolves an [[UnresolvedExecuteImmediate]] node into the plan for its dynamic SQL.
   *
   * All expressions (`sqlStmtStr`, `args`, `targetVariables`) must already be resolved
   * before calling this method.
   *
   * When an `INTO` clause is present, the dynamic SQL is parsed and analyzed, and the analyzed
   * plan is wrapped in a [[SetVariable]] plan that assigns output columns to the target variables.
   * Without `INTO`, a command payload is wrapped in [[ExecuteImmediateCommand]] to run at the
   * execution level and a query is returned analyzed for lazy execution. Command payloads are not
   * executed during analysis.
   */
  def resolveExecuteImmediate(
      sparkSession: SparkSession,
      sqlStmtStr: Expression,
      args: Seq[Expression],
      targetVariables: Seq[Expression]): LogicalPlan = {
    if (targetVariables.nonEmpty) {
      val finalTargetVars = extractTargetVariables(targetVariables)
      val analyzedSource = resolveInnerStatement(
        sparkSession, sqlStmtStr, args, hasIntoClause = true)
      SetVariable(finalTargetVars, analyzedSource)
    } else {
      resolveInnerStatement(sparkSession, sqlStmtStr, args, hasIntoClause = false)
    }
  }

  private def extractTargetVariables(targetVariables: Seq[Expression]): Seq[VariableReference] = {
    targetVariables.map {
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
  }

  /**
   * Parses and analyzes the dynamic SQL and returns the plan that replaces the EXECUTE IMMEDIATE
   * node. No command payload is executed here: with an INTO clause the analyzed query is returned
   * for the caller to wrap in [[SetVariable]] (a command is rejected); otherwise a command payload
   * is wrapped in [[ExecuteImmediateCommand]] to run at the execution level and a query is returned
   * analyzed for lazy execution. Nodes that execute during analysis, such as CALL, are still run by
   * the analyzer here (see the branch below).
   */
  private def resolveInnerStatement(
      sparkSession: SparkSession,
      sqlStmtStr: Expression,
      args: Seq[Expression],
      hasIntoClause: Boolean): LogicalPlan = {
    // Extract the query string from the queryParam expression
    val sqlString = extractQueryString(sqlStmtStr)

    // Create the origin for EXECUTE IMMEDIATE context - this will be used by expressions
    // during parsing to set their queryContext, similar to how views work
    val executeImmediateOrigin = Origin(
      objectType = Some("EXECUTE IMMEDIATE"),
      objectName = None, // No named object for EXECUTE IMMEDIATE, unlike views
      sqlText = Some(sqlString),
      startIndex = Some(0),
      stopIndex = Some(sqlString.length - 1)
    )

    // Parse and analyze the inner statement with local variables hidden and EXECUTE IMMEDIATE
    // origin set, but without executing it (command payloads run later at the execution level).
    // Both must cover parsing and analysis.
    // CurrentOrigin.withOrigin ensures expressions created during parsing get the proper context.
    val analyzed = withHiddenLocalVariables {
      CurrentOrigin.withOrigin(executeImmediateOrigin) {
        parseAndAnalyzeInnerStatement(
          sparkSession.asInstanceOf[ClassicSparkSession], sqlString, args)
      }
    }

    if (hasIntoClause) {
      // If this EXECUTE IMMEDIATE has an INTO clause, commands are not allowed.
      // The caller wraps the analyzed query in SetVariable.
      if (QueryExecution.isEagerlyExecutedCommand(analyzed)) {
        throw QueryCompilationErrors.invalidStatementForExecuteInto(sqlString)
      }
      analyzed
    } else if (QueryExecution.isEagerlyExecutedCommand(analyzed)) {
      // Defer eager-command payloads to the execution level. This matches the shapes
      // QueryExecution.eagerlyExecuteCommands runs (not just Command). CALL is not among them and
      // already ran during the analysis above.
      ExecuteImmediateCommand(analyzed)
    } else {
      // Splice the query; it executes lazily like any other query.
      analyzed
    }
  }

  /**
   * Parses and analyzes the dynamic SQL, returning the analyzed plan without executing it. Parsing
   * and parameter binding are shared with `SparkSession.sql` via
   * [[org.apache.spark.sql.classic.SparkSession.parseParameterizedPlan]]. Unlike `sql`, the plan is
   * analyzed here but never wrapped in a `Dataset`, so command payloads are not run during
   * analysis; the caller defers them to the execution level.
   */
  private def parseAndAnalyzeInnerStatement(
      session: ClassicSparkSession,
      sqlString: String,
      args: Seq[Expression]): LogicalPlan = {
    // Extract the USING arguments the same way OPEN CURSOR does.
    val (values, paramNames) = ParameterBindingUtils.buildUnifiedParameters(args)
    // parseParameterizedPlan self-activates the session; withActive here also covers analysis.
    // The caller has already hidden local variables and set the EXECUTE IMMEDIATE origin.
    session.withActive {
      val parsedPlan = session.parseParameterizedPlan(sqlString, values, paramNames)
      // EXECUTE IMMEDIATE does not support SQL scripts.
      if (parsedPlan.isInstanceOf[CompoundBody]) {
        throw QueryCompilationErrors.sqlScriptInExecuteImmediate(sqlString)
      }
      session.sessionState.analyzer.executeAndCheck(parsedPlan, new QueryPlanningTracker)
    }
  }

  private def extractQueryString(queryExpr: Expression): String = {
    // Reject scalar subqueries and other non-evaluable expressions (same rule as USING clause).
    // Unwrap Alias so that EXECUTE IMMEDIATE sql_string (variable) is allowed.
    def checkAllowed(expr: Expression): Unit = expr match {
      case _: VariableReference => // allowed
      case Alias(child, _) => checkAllowed(child)
      case foldable if foldable.foldable => // allowed (literals, constants)
      case other =>
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
    checkAllowed(queryExpr)

    // Ensure the expression resolves to string type
    if (!queryExpr.dataType.sameType(StringType)) {
      throw QueryCompilationErrors.invalidExecuteImmediateExpressionType(queryExpr.dataType)
    }

    // Evaluate the expression to get the query string
    val value = queryExpr.eval(null)
    if (value == null) {
      // Extract the original text from the expression's origin for the error message
      val originalText = extractOriginalText(queryExpr)
      throw QueryCompilationErrors.nullSQLStringExecuteImmediate(originalText)
    }

    value.toString
  }

  private def extractOriginalText(queryExpr: Expression): String = {
    val origin = queryExpr.origin
    // Try to extract the original text from the origin information
    (origin.sqlText, origin.startIndex, origin.stopIndex) match {
      case (Some(sqlText), Some(startIndex), Some(stopIndex)) =>
        // Extract the substring from the original SQL text
        sqlText.substring(startIndex, stopIndex + 1)
      case _ =>
        // Fallback to the SQL representation if origin information is not available
        queryExpr.sql
    }
  }

  /**
   * Temporarily hides the local variable context while executing the `EXECUTE IMMEDIATE` command.
   * This is expected behavior, as local variables cannot be resolved within the body of this
   * command. This does not apply to session variables. The rest of the SQL scripting context is
   * preserved.
   *
   * {{{
   *   DECLARE VARIABLE v1 = 1; -- Session variable.
   *   BEGIN
   *     DECLARE v2 = 2; -- Local variable.
   *     EXECUTE IMMEDIATE 'SELECT v1'; -- Should work.
   *     EXECUTE IMMEDIATE 'SELECT v2'; -- Should fail.
   *     EXECUTE IMMEDIATE 'SELECT ?' USING v2; -- Should work.
   *   END
   * }}}
   */
  private def withHiddenLocalVariables[A](f: => A): A = {
    val newContextManager = SqlScriptingContextManager.get() match {
      case Some(contextManager) =>
        // SqlScriptingContextManagerTrait is catalog.SqlScriptingContextManager, renamed on import
        // to distinguish from the object of the same name in org.apache.spark.sql.catalyst.
        new SqlScriptingContextManagerTrait {
          override def getContext = contextManager.getContext
          override def getVariableManager = None
        }
      case None => null
    }
    SqlScriptingContextManager.create(newContextManager).runWith(f)
  }
}
