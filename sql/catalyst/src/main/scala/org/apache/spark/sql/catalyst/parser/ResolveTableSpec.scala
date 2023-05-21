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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.DefaultColumnAnalyzer

/**
 * This rule is responsible for analyzing expressions passed in as values for OPTIONS lists for
 * commands such as CREATE TABLE. These expressions may be constant but non-literal, in which case
 * we perform constant folding here.
 */
object ResolveTableSpec extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsPattern(COMMAND), ruleId) {
      case c: CreateTable if !c.tableSpec.optionsResolved =>
        val newOptions: Map[String, String] =
          c.tableSpec.options.map { case (key, value) =>
            (key, resolveOption(key, value)) }
        c.copy(tableSpec = c.tableSpec.copy(options = newOptions))
    }
  }

  private def resolveOption(key: String, value: String): String = {
    val parsed: Expression = CatalystSqlParser.parseExpression(value)
    val plan = try {
      val analyzer: Analyzer = DefaultColumnAnalyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(parsed, "col")()), OneRowRelation()))
      analyzer.checkAnalysis(analyzed)
      ConstantFolding(analyzed)
    } catch {
      case _: AnalysisException =>
        throw optionNotConstantError(key)
    }
    val result: Expression = plan.collectFirst {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }.get
    result match {
      case expr if expr.isInstanceOf[Literal] =>
        // Note: we use 'toString' here instead of using a Cast expression to support some types
        // of literals where casting to string is not supported.
        expr.toString
      case _ =>
        throw optionNotConstantError(key)
    }
  }

  private def optionNotConstantError(key: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          s"option or property key $key is invalid; only constant expressions are supported"))
  }
}
