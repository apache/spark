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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.types.IntegerType

/**
 * Resolves the target SQL variables that we want to set in SetVariable, and add cast if necessary
 * to make the assignment valid.
 */
class ResolveSetVariable(val catalogManager: CatalogManager) extends Rule[LogicalPlan]
  with ColumnResolutionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    // Resolve the left hand side of the SET VAR command
    case setVariable: SetVariable if !setVariable.targetVariables.forall(_.resolved) =>
      val resolvedVars = setVariable.targetVariables.map {
        case u: UnresolvedAttribute =>
          lookupVariable(u.nameParts) match {
            case Some(variable) => variable.copy(canFold = false)
            case _ => throw unresolvedVariableError(u.nameParts, Seq("SYSTEM", "SESSION"))
          }

        case other => throw SparkException.internalError(
          "Unexpected target variable expression in SetVariable: " + other)
      }

      // Protect against duplicate variable names
      // Names are normalized when the variables are created.
      // No need for case insensitive comparison here.
      // TODO: we need to group by the qualified variable name once other catalogs support it.
      val dups = resolvedVars.groupBy(_.identifier.name).filter(kv => kv._2.length > 1)
      if (dups.nonEmpty) {
        throw new AnalysisException(
          errorClass = "DUPLICATE_ASSIGNMENTS",
          messageParameters = Map("nameList" -> dups.keys.map(toSQLId).mkString(", ")))
      }

      setVariable.copy(targetVariables = resolvedVars)

    case setVariable: SetVariable
        if setVariable.targetVariables.forall(_.isInstanceOf[VariableReference]) &&
          setVariable.sourceQuery.resolved =>
      val targetVariables = setVariable.targetVariables.map(_.asInstanceOf[VariableReference])
      val withCasts = TableOutputResolver.resolveVariableOutputColumns(
        targetVariables, setVariable.sourceQuery, conf)
      val withLimit = if (withCasts.maxRows.exists(_ <= 2)) {
        withCasts
      } else {
        Limit(Literal(2, IntegerType), withCasts)
      }
      setVariable.copy(sourceQuery = withLimit)
  }
}
