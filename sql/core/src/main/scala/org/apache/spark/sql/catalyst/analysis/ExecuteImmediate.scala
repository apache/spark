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

import scala.util.{Either, Left, Right}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXECUTE_IMMEDIATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Analysis rule that executes ExecuteImmediateCommand during analysis and replaces it
 * with the results, similar to how CALL statements work.
 */
case class ExecuteExecutableDuringAnalysis(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case cmd: ExecuteImmediateCommand =>
        executeImmediate(cmd)
    }
  }

  private def executeImmediate(cmd: ExecuteImmediateCommand): LogicalPlan = {
    try {
      // Extract the query string
      val queryString = extractQueryString(cmd.query)

      // Parse and validate the query
      val parsedPlan = sparkSession.sessionState.sqlParser.parsePlan(queryString)
      validateQuery(queryString, parsedPlan)

      // Execute the query recursively
      val result = if (cmd.args.isEmpty) {
        // No parameters - execute directly
        AnalysisContext.withExecuteImmediateContext {
          sparkSession.sql(queryString)
        }
      } else {
        // For parameterized queries, build parameter map from USING clause
        // The args are already resolved by outer parameter resolution
        val (positionalParams, namedParams) = separateParameters(cmd.args)

        AnalysisContext.withExecuteImmediateContext {
          if (namedParams.nonEmpty && positionalParams.isEmpty) {
            // Only named parameters: use Map overload
            sparkSession.sql(queryString, namedParams)
          } else if (positionalParams.nonEmpty && namedParams.isEmpty) {
            // Only positional parameters: use Array overload
            sparkSession.sql(queryString, positionalParams.toArray)
          } else if (namedParams.isEmpty && positionalParams.isEmpty) {
            // No parameters
            sparkSession.sql(queryString)
          } else {
            // Mixed parameters: not directly supported, need manual substitution
            val substitutedQuery = substituteNamedParameters(queryString, namedParams)
            sparkSession.sql(substitutedQuery, positionalParams.toArray)
          }
        }
      }

      // Handle target variables if specified
      if (cmd.targetVariables.nonEmpty) {
        handleTargetVariables(result, cmd.targetVariables)
        // Return empty relation for INTO queries
        LocalRelation(Nil, Nil)
      } else {
        // Return the query results as a LocalRelation
        val internalRows = result.queryExecution.executedPlan.executeCollect()
        LocalRelation(result.queryExecution.analyzed.output, internalRows.toSeq)
      }

    } catch {
      case e: AnalysisException =>
        // Re-throw AnalysisException as-is to preserve error type for tests
        throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to execute immediate query: ${e.getMessage}", e)
    }
  }

  private def extractQueryString(queryExpr: Either[String, VariableReference]): String = {
    queryExpr match {
      case Left(literal) => literal
      case Right(variable) =>
        // Evaluate the variable reference
        if (!variable.dataType.sameType(StringType)) {
          throw QueryCompilationErrors.invalidExecuteImmediateVariableType(variable.dataType)
        }

        val value = variable.eval(null)
        if (value == null) {
          throw QueryCompilationErrors.nullSQLStringExecuteImmediate(variable.identifier.name())
        }

        value.toString
    }
  }

  private def validateQuery(queryString: String, parsedPlan: LogicalPlan): Unit = {
    // Check for compound bodies (SQL scripting)
    if (parsedPlan.isInstanceOf[CompoundBody]) {
      throw QueryCompilationErrors.sqlScriptInExecuteImmediate(queryString)
    }

    // Check for nested EXECUTE IMMEDIATE
    if (parsedPlan.containsPattern(EXECUTE_IMMEDIATE)) {
      throw QueryCompilationErrors.nestedExecuteImmediate(queryString)
    }
  }

  private def substituteParameters(queryString: String, paramValues: Seq[Any]): String = {
    // For now, just handle positional parameters
    // Named parameters require more complex resolution that involves the original args expressions
    var substituted = queryString
    var paramIndex = 0

    // Handle positional parameters (?)
    while (substituted.contains("?") && paramIndex < paramValues.length) {
      val value = paramValues(paramIndex)
      val sqlLiteral = formatSqlLiteral(value)
      substituted = substituted.replaceFirst("\\?", sqlLiteral)
      paramIndex += 1
    }

    substituted
  }

  private def substituteParametersWithNames(
      queryString: String,
      args: Seq[Expression]): String = {
    try {
      var substituted = queryString
      val paramMap = scala.collection.mutable.Map[String, Any]()
      var positionalIndex = 0

      // Build parameter map from args
      args.foreach {
        case alias: Alias =>
          // Named parameter: "value AS paramName"
          val paramName = alias.name
          val paramValue = alias.child.eval(null)
          paramMap(paramName) = paramValue
        case expr =>
          // Positional parameter: just a value
          val paramValue = expr.eval(null)
          // Handle positional parameters first
          if (substituted.contains("?")) {
            val sqlLiteral = formatSqlLiteral(paramValue)
            substituted = substituted.replaceFirst("\\?", sqlLiteral)
          }
          positionalIndex += 1
      }

      // Substitute named parameters (:paramName)
      paramMap.foreach { case (paramName, paramValue) =>
        val sqlLiteral = formatSqlLiteral(paramValue)
        val pattern = s":$paramName\\b" // Use word boundary to avoid partial matches
        substituted = substituted.replaceAll(pattern, sqlLiteral)
      }

      substituted
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Error in parameter substitution for query '$queryString'", e)
    }
  }

  private def separateParameters(args: Seq[Expression]): (Seq[Any], Map[String, Any]) = {
    val positionalParams = scala.collection.mutable.ListBuffer[Any]()
    val namedParams = scala.collection.mutable.Map[String, Any]()

    args.foreach {
      case alias: Alias =>
        // Named parameter: "value AS paramName"
        val paramName = alias.name
        val paramValue = alias.child.eval(null)
        namedParams(paramName) = paramValue
      case expr =>
        // Positional parameter: just a value
        val paramValue = expr.eval(null)
        positionalParams += paramValue
    }

    (positionalParams.toSeq, namedParams.toMap)
  }

  private def substituteNamedParameters(
      queryString: String, namedParams: Map[String, Any]): String = {
    var substituted = queryString
    // Substitute named parameters (:paramName)
    namedParams.foreach { case (paramName, paramValue) =>
      val sqlLiteral = formatSqlLiteral(paramValue)
      val pattern = s":$paramName\\b" // Use word boundary to avoid partial matches
      substituted = substituted.replaceAll(pattern, sqlLiteral)
    }

    substituted
  }

  private def formatSqlLiteral(value: Any): String = {
    if (value == null) {
      "NULL"
    } else {
      value match {
        case s: String => s"'$s'"
        case n: Number => n.toString
        case b: Boolean => b.toString
        case _ => s"'$value'"
      }
    }
  }

  private def handleTargetVariables(
      result: org.apache.spark.sql.DataFrame,
      targetVariables: Seq[VariableReference]): Unit = {
    // Collect the results from the query
    val values = result.queryExecution.executedPlan.executeCollect()

    if (values.length == 0) {
      // No rows: Set all variables to null
      targetVariables.foreach { variable =>
        setVariable(variable, null)
      }
    } else if (values.length > 1) {
      // Multiple rows: Error
      throw new RuntimeException(
        "EXECUTE IMMEDIATE ... INTO query returned more than one row")
    } else {
      // Exactly one row: Set each variable to the corresponding column value
      val row = values(0)
      targetVariables.zipWithIndex.foreach { case (variable, index) =>
        val value = row.get(index, variable.dataType)
        setVariable(variable, value)
      }
    }
  }

  private def setVariable(variable: VariableReference, value: Any): Unit = {
    import java.util.Locale
    import org.apache.spark.sql.catalyst.{SqlScriptingContextManager}
    import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, FakeSystemCatalog}
    import org.apache.spark.sql.catalyst.catalog.VariableDefinition
    import org.apache.spark.sql.catalyst.expressions.Literal
    import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError

    val namePartsCaseAdjusted = if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
      variable.originalNameParts
    } else {
      variable.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val tempVariableManager = sparkSession.sessionState.catalogManager.tempVariableManager
    val scriptingVariableManager = SqlScriptingContextManager.get().map(_.getVariableManager)

    val variableManager = variable.catalog match {
      case FakeLocalCatalog if scriptingVariableManager.isEmpty =>
        throw new RuntimeException("SetVariable: Variable has FakeLocalCatalog, " +
          "but ScriptingVariableManager is None.")

      case FakeLocalCatalog if scriptingVariableManager.get.get(namePartsCaseAdjusted).isEmpty =>
        throw new RuntimeException("Local variable should be present in SetVariable" +
          "because ResolveSetVariable has already determined it exists.")

      case FakeLocalCatalog => scriptingVariableManager.get

      case FakeSystemCatalog if tempVariableManager.get(namePartsCaseAdjusted).isEmpty =>
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))

      case FakeSystemCatalog => tempVariableManager

      case c => throw new RuntimeException("Unexpected catalog in SetVariable: " + c)
    }

    val varDef = VariableDefinition(
      variable.identifier, variable.varDef.defaultValueSQL, Literal(value, variable.dataType))

    variableManager.set(namePartsCaseAdjusted, varDef)
  }
}
