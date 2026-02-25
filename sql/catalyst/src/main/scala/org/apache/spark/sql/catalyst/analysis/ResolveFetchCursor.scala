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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{FetchCursor, LogicalPlan, SingleStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError

/**
 * Resolves the target SQL variables in FetchCursor command.
 * Variables can be either scripting local variables or session variables.
 */
class ResolveFetchCursor(val catalogManager: CatalogManager) extends Rule[LogicalPlan]
  with ColumnResolutionHelper {
  // VariableResolution looks up both scripting local variables (via SqlScriptingContextManager)
  // and session variables (via tempVariableManager), checking local variables first.
  private val variableResolution = new VariableResolution(catalogManager.tempVariableManager)

  /**
   * Checks for duplicate variable names and throws an exception if found.
   * Names are normalized when the variables are created.
   * No need for case insensitive comparison here.
   */
  private def checkForDuplicateVariables(variables: Seq[VariableReference]): Unit = {
    val dups = variables.groupBy(_.identifier).filter(kv => kv._2.length > 1)
    if (dups.nonEmpty) {
      throw new AnalysisException(
        errorClass = "DUPLICATE_ASSIGNMENTS",
        messageParameters = Map("nameList" ->
          dups.keys.map(key => toSQLId(key.name())).mkString(", ")))
    }
  }

  /**
   * Resolves and validates the target variables for a FetchCursor command.
   * Returns a sequence of resolved VariableReference expressions.
   */
  private def resolveAndValidateTargetVariables(
      targetVariables: Seq[Expression]): Seq[VariableReference] = {
    val resolvedVars = targetVariables.map {
      case u: UnresolvedAttribute =>
        variableResolution.lookupVariable(
          nameParts = u.nameParts
        ) match {
          case Some(variable) => variable.copy(canFold = false)
          case _ => throw unresolvedVariableError(u.nameParts, Seq("SYSTEM", "SESSION"))
        }

      case other => throw SparkException.internalError(
        "Unexpected target variable expression in FetchCursor: " + other)
    }

    // Check for duplicates immediately after resolution
    checkForDuplicateVariables(resolvedVars)
    resolvedVars
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    // Resolve FetchCursor wrapped in SingleStatement
    case s @ SingleStatement(fetchCursor: FetchCursor)
        if !fetchCursor.targetVariables.forall(_.resolved) =>
      val resolvedVars = resolveAndValidateTargetVariables(fetchCursor.targetVariables)
      s.copy(parsedPlan = fetchCursor.copy(targetVariables = resolvedVars))

    // Also resolve unwrapped FetchCursor (when extracted from SingleStatement for execution)
    case fetchCursor: FetchCursor if !fetchCursor.targetVariables.forall(_.resolved) =>
      val resolvedVars = resolveAndValidateTargetVariables(fetchCursor.targetVariables)
      fetchCursor.copy(targetVariables = resolvedVars)
  }
}
