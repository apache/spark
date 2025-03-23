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

import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingLocalVariableManager}
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionsEvaluator, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DefaultValueExpression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for creating a variable.
 */
case class CreateVariableExec(
    resolvedIdentifier: ResolvedIdentifier,
    defaultExpr: DefaultValueExpression,
    replace: Boolean) extends LeafV2CommandExec with ExpressionsEvaluator {

  override protected def run(): Seq[InternalRow] = {
    val scriptingVariableManager = SqlScriptingLocalVariableManager.get()
    val tempVariableManager = session.sessionState.catalogManager.tempVariableManager

    val exprs = prepareExpressions(Seq(defaultExpr.child), subExprEliminationEnabled = false)
    initializeExprs(exprs, 0)
    val initValue = Literal(exprs.head.eval(), defaultExpr.dataType)

    val normalizedIdentifier = if (session.sessionState.conf.caseSensitiveAnalysis) {
      resolvedIdentifier.identifier
    } else {
      Identifier.of(
        resolvedIdentifier.identifier.namespace().map(_.toLowerCase(Locale.ROOT)),
        resolvedIdentifier.identifier.name().toLowerCase(Locale.ROOT))
    }
    val varDef = VariableDefinition(normalizedIdentifier, defaultExpr.originalSQL, initValue)

    // create local variable if we are in a script, otherwise create session variable
    scriptingVariableManager
      .filter(_ => resolvedIdentifier.catalog == FakeLocalCatalog)
      // If resolvedIdentifier.catalog is FakeLocalCatalog, scriptingVariableManager
      // will always be present.
      .getOrElse(tempVariableManager)
      .create(
        normalizedIdentifier.namespace().toSeq :+ normalizedIdentifier.name(),
        varDef,
        replace)

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
