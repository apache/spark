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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.VariableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, Literal, Nondeterministic}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.{PLAN_EXPRESSION, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.DefaultColumnAnalyzer
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * The DDL command that creates a function.
 * To create a temporary function, the syntax of using this command in SQL is:
 * {{{
 *    CREATE [OR REPLACE] TEMPORARY VARIABLE variableName
 *     [dataType] [defaultExpression]
 * }}}
 *
 * @param replace: When true, alter the function with the specified name
 */

case class CreateVariableCommand(
    identifier: VariableIdentifier,
    dataTypeStr: Option[String],
    defaultExprStr: String,
    replace: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val parser = sparkSession.sessionState.sqlParser
    val analyzer = DefaultColumnAnalyzer
    val catalog = sparkSession.sessionState.catalog

    val dataType = if (dataTypeStr.nonEmpty) {
      Some(parser.parseDataType(dataTypeStr.get))
    } else {
      None
    }

    val defaultExprPlan: LogicalPlan = {
      val parsed: Expression = parser.parseExpression(defaultExprStr)
      if (parsed.containsPattern(UNRESOLVED_ATTRIBUTE)) {
        parsed.collect {
          case a: UnresolvedAttribute =>
            throw QueryCompilationErrors.unresolvedAttributeError(
              "UNRESOLVED_COLUMN", a.sql, Seq.empty, a.origin)
        }
        if (parsed.containsPattern(PLAN_EXPRESSION)) {
          throw QueryCompilationErrors.defaultValuesMayNotContainSubQueryExpressions(
            "CREATE VARIABLE", "", defaultExprStr)
        }
      }

      val withCast = if (dataType.isDefined) {
        Cast(parsed, dataType.get)
      } else {
        parsed
      }

      val analyzed: LogicalPlan = analyzer.execute(Project(Seq(Alias(withCast,
        identifier.variableName)()),
        OneRowRelation()))
      analyzer.checkAnalysis(analyzed)
      analyzed
    }

    val defaultExpr = defaultExprPlan.collectFirst {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }.get

    defaultExpr.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    val initialValue = defaultExpr.eval()

    val initialLiteral = Literal.create(initialValue, defaultExpr.dataType)

    catalog.createTempVariable(identifier.variableName,
      initialLiteral, defaultExprStr,
      overrideIfExists = replace)
    Seq.empty[Row]
  }
}

/**
 * The DDL command that drops a variable.
 * ifExists: returns an error if the variable doesn't exist, unless this is true.
 */
case class DropVariableCommand(
    identifier: VariableIdentifier,
    ifExists: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.dropTempVariable(catalog.qualifyIdentifier(identifier), ifExists)
    Seq.empty[Row]
  }
}
