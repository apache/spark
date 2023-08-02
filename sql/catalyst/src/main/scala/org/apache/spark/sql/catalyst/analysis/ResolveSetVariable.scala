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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.VariableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, SetVariable, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.types.IntegerType

/**
 * Resolves columns of an output table from the data in a logical plan. This rule will:
 *
 * - Insert casts when data types do not match
 * - Detect plans that are not compatible with the output table and throw AnalysisException
 */
case class ResolveSetVariable(catalog: SessionCatalog) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case setVariable: SetVariable
      if setVariable.sourceQuery.resolved && !setVariable.targetVariables.forall(_.resolved) =>

      /**
       * Resolve the left hand side of the SET
       */
      val resolvedVars = setVariable.targetVariables.map { variable =>
        variable match {
          case v: UnresolvedVariable =>
            val varIdent = VariableIdentifier(v.nameParts)
            catalog.getVariable(varIdent).map { varInfo =>
              VariableReference(varIdent.variableName, varInfo._1, canFold = false)
            }.getOrElse {
              throw unresolvedVariableError(varIdent, Seq("SESSION"))
            }
          case other => other
        }
      }

      /**
       * Protect against duplicate variable names
       * Names are normalized when the variables are created.
       * No need for case insensitive comparison here.
       */
      val varNames = resolvedVars.collect { case variable => variable.prettyName }
      val dups = varNames.diff(varNames.distinct).distinct
      if (dups.nonEmpty) {
        throw new AnalysisException(errorClass = "DUPLICATE_ASSIGNMENTS",
          messageParameters = Map("nameList" -> dups.map(toSQLId).mkString(", ")))
      }

      val withCasts = TableOutputResolver.resolveVariableOutputColumns(
        resolvedVars, setVariable.sourceQuery, conf)

      val withLimit = SubqueryAlias("T", UnresolvedSubqueryColumnAliases(varNames,
        Limit(Literal(2, IntegerType), withCasts)))

      setVariable.copy(sourceQuery = withLimit, targetVariables = resolvedVars)
  }
}
