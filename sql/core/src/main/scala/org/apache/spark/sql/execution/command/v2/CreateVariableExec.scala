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

package org.apache.spark.sql.execution.command.v2

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionsEvaluator, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DefaultValueExpression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for creating variables.
 */
case class CreateVariableExec(
    resolvedIdentifiers: Seq[ResolvedIdentifier],
    defaultExpr: DefaultValueExpression,
    replace: Boolean) extends LeafV2CommandExec with ExpressionsEvaluator {

  override protected def run(): Seq[InternalRow] = {
    val scriptingVariableManager = SqlScriptingContextManager.get().map(_.getVariableManager)
    val tempVariableManager = session.sessionState.catalogManager.tempVariableManager

    val exprs = prepareExpressions(Seq(defaultExpr.child), subExprEliminationEnabled = false)
    initializeExprs(exprs, 0)
    val initValue = Literal(exprs.head.eval(), defaultExpr.dataType)

    val variableTuples = resolvedIdentifiers.map(resolvedIdentifier => {
      val normalizedIdentifier = if (session.sessionState.conf.caseSensitiveAnalysis) {
        resolvedIdentifier.identifier
      } else {
        Identifier.of(
          resolvedIdentifier.identifier.namespace().map(_.toLowerCase(Locale.ROOT)),
          resolvedIdentifier.identifier.name().toLowerCase(Locale.ROOT))
      }
      val varDef = VariableDefinition(normalizedIdentifier, defaultExpr.originalSQL, initValue)

      (normalizedIdentifier.namespace().toSeq :+ normalizedIdentifier.name(), varDef)
    })

    // create local variables if we are in a script, otherwise create session variable
    val variableManager =
      scriptingVariableManager
      .filter(_ => resolvedIdentifiers.head.catalog == FakeLocalCatalog)
      // If resolvedIdentifier.catalog is FakeLocalCatalog, scriptingVariableManager
      // will always be present.
      .getOrElse(tempVariableManager)

    val uniqueNames = mutable.Set[String]()

    variableTuples.foreach(variable => {
      val nameParts: Seq[String] = variable._1
      val name = nameParts.last

      // Check if the variable name was already declared inside the same DECLARE statement
      if (uniqueNames.contains(name)) {
        throw new AnalysisException(
          errorClass = "DUPLICATE_VARIABLE_NAME_INSIDE_DECLARE",
          messageParameters = Map(
            "variableName" -> variableManager.getVariableNameForError(name)))
      }

      // If DECLARE statement does not have OR REPLACE part, check if any of the variable names
      // declared in the DECLARE statement already exists as a name of another variable
      if (!replace && variableManager.get(nameParts).isDefined) {
        throw new AnalysisException(
          errorClass = "VARIABLE_ALREADY_EXISTS",
          messageParameters = Map(
            "variableName" -> variableManager.getVariableNameForError(name)))
      }

      uniqueNames.add(name)
    })

    variableTuples.foreach(variable => {
      val nameParts: Seq[String] = variable._1
      val varDef: VariableDefinition = variable._2
      variableManager.create(nameParts, varDef, replace)
    })

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
