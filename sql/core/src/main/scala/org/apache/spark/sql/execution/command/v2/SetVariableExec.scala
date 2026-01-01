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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, FakeSystemCatalog}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, VariableReference}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Helper object for setting variables, shared by SetVariableExec and SelectIntoVariableExec.
 */
private[v2] object VariableSetter {
  def setVariable(
      variable: VariableReference,
      value: Any,
      conf: SQLConf,
      tempVariableManager: org.apache.spark.sql.catalyst.catalog.TempVariableManager): Unit = {
    val namePartsCaseAdjusted = if (conf.caseSensitiveAnalysis) {
      variable.originalNameParts
    } else {
      variable.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val scriptingVariableManager = SqlScriptingContextManager.get().map(_.getVariableManager)

    val variableManager = variable.catalog match {
      case FakeLocalCatalog if scriptingVariableManager.isEmpty =>
        throw SparkException.internalError("Variable has FakeLocalCatalog, " +
          "but ScriptingVariableManager is None.")

      case FakeLocalCatalog if scriptingVariableManager.get.get(namePartsCaseAdjusted).isEmpty =>
        throw SparkException.internalError("Local variable should be present " +
          "because analysis has already determined it exists.")

      case FakeLocalCatalog => scriptingVariableManager.get

      case FakeSystemCatalog if tempVariableManager.get(namePartsCaseAdjusted).isEmpty =>
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))

      case FakeSystemCatalog => tempVariableManager

      case c => throw SparkException.internalError("Unexpected catalog: " + c)
    }

    val varDef = VariableDefinition(
      variable.identifier, variable.varDef.defaultValueSQL, Literal(value, variable.dataType))

    variableManager.set(namePartsCaseAdjusted, varDef)
  }
}

/**
 * Defines how variables should behave when the query returns zero rows.
 */
private[v2] sealed trait ZeroRowBehavior
private[v2] object ZeroRowBehavior {
  /**
   * Set variables to NULL when query returns zero rows.
   * Used by EXECUTE IMMEDIATE INTO.
   */
  case object SetToNull extends ZeroRowBehavior

  /**
   * Keep variables unchanged when query returns zero rows.
   * Used by SELECT INTO.
   */
  case object KeepUnchanged extends ZeroRowBehavior
}

/**
 * Helper object for executing variable assignment queries.
 * Consolidates common logic between SetVariableExec and SelectIntoVariableExec.
 *
 * The key difference between these two is zero-row behavior:
 * - EXECUTE IMMEDIATE INTO (SetVariableExec): Sets variables to NULL
 * - SELECT INTO (SelectIntoVariableExec): Keeps variables unchanged
 *
 * Both share the same logic for single-row (assign values) and multi-row (error) cases.
 */
private[v2] object VariableExecutor {
  /**
   * Execute a query and assign its results to variables.
   *
   * Behavior based on number of rows returned:
   * - 0 rows: Depends on zeroRowBehavior (SetToNull vs KeepUnchanged)
   * - 1 row: Assigns column values to variables
   * - 2+ rows: Throws ROW_SUBQUERY_TOO_MANY_ROWS error
   *
   * @param query The query to execute
   * @param variables The target variables
   * @param zeroRowBehavior How to handle zero-row results
   * @param conf SQL configuration
   * @param tempVariableManager Temp variable manager
   * @return Empty sequence (variable assignment produces no output)
   */
  def executeWithVariables(
      query: SparkPlan,
      variables: Seq[VariableReference],
      zeroRowBehavior: ZeroRowBehavior,
      conf: SQLConf,
      tempVariableManager: org.apache.spark.sql.catalyst.catalog.TempVariableManager
  ): Seq[InternalRow] = {
    val values = query.executeCollect()

    if (values.length == 0) {
      // Handle zero rows based on the behavior
      zeroRowBehavior match {
        case ZeroRowBehavior.SetToNull =>
          // EXECUTE IMMEDIATE INTO: set all variables to null
          variables.foreach { v =>
            VariableSetter.setVariable(v, null, conf, tempVariableManager)
          }
        case ZeroRowBehavior.KeepUnchanged =>
          // SELECT INTO: do nothing, variables remain unchanged
      }
    } else if (values.length > 1) {
      throw new SparkException(
        errorClass = "ROW_SUBQUERY_TOO_MANY_ROWS",
        messageParameters = Map.empty,
        cause = null)
    } else {
      // Exactly one row: assign values to variables
      val row = values(0)
      variables.zipWithIndex.foreach { case (v, index) =>
        val value = row.get(index, v.dataType)
        VariableSetter.setVariable(v, value, conf, tempVariableManager)
      }
    }

    Seq.empty
  }
}

/**
 * Physical plan node for setting a variable.
 * Used by EXECUTE IMMEDIATE INTO.
 */
case class SetVariableExec(variables: Seq[VariableReference], query: SparkPlan)
  extends V2CommandExec with UnaryLike[SparkPlan] {

  override protected def run(): Seq[InternalRow] = {
    VariableExecutor.executeWithVariables(
      query,
      variables,
      ZeroRowBehavior.SetToNull,
      session.sessionState.conf,
      session.sessionState.catalogManager.tempVariableManager)
  }

  override def output: Seq[Attribute] = Seq.empty
  override def child: SparkPlan = query
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}

/**
 * Physical plan node for SELECT INTO.
 * When query returns zero rows, variables remain unchanged.
 * When query returns more than one row, an error is thrown.
 * @param variables The variables to set
 * @param query The query that produces the values
 */
case class SelectIntoVariableExec(
    variables: Seq[VariableReference],
    query: SparkPlan)
  extends V2CommandExec with UnaryLike[SparkPlan] {

  override protected def run(): Seq[InternalRow] = {
    VariableExecutor.executeWithVariables(
      query,
      variables,
      ZeroRowBehavior.KeepUnchanged,
      session.sessionState.conf,
      session.sessionState.catalogManager.tempVariableManager)
  }

  override def output: Seq[Attribute] = Seq.empty
  override def child: SparkPlan = query
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}
