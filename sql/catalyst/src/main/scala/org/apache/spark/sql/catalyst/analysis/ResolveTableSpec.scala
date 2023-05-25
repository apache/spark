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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * This rule is responsible for matching against unresolved table specifications in commands with
 * OPTIONS lists. The parser produces such lists as maps from strings to unresolved expressions.
 * After otherwise resolving such expressions in the analyzer, this rule converts them to resolved
 * table specifications wherein these OPTIONS list values are represented as strings instead, for
 * convenience. The 'resolveOption' is for resolving function calls within each table option.
 */
case class ResolveTableSpec(resolveOption: Expression => Expression) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsAnyPattern(COMMAND), ruleId) {
      case t @ CreateTable(_, _, _, u: UnresolvedTableSpec, _) =>
        t.copy(tableSpec = resolveTableSpec(u))
      case t @ CreateTableAsSelect(_, _, _, u: UnresolvedTableSpec, _, _, _) =>
        t.copy(tableSpec = resolveTableSpec(u))
      case t @ ReplaceTable(_, _, _, u: UnresolvedTableSpec, _) =>
        t.copy(tableSpec = resolveTableSpec(u))
      case t @ ReplaceTableAsSelect(_, _, _, u: UnresolvedTableSpec, _, _, _) =>
        t.copy(tableSpec = resolveTableSpec(u))
    }
  }

  private def resolveTableSpec(u: UnresolvedTableSpec): TableSpec = {
    val newOptions: Map[String, String] = u.optionsExpressions.map {
      case (key: String, null) =>
        (key, null)
      case (key: String, value: Expression) =>
        val newValue: String = try {
          resolveOption(value) match {
            case lit: Literal =>
              lit.toString
          }
        } catch {
          case _: SparkThrowable | _: MatchError | _: java.lang.RuntimeException =>
            throw QueryCompilationErrors.optionMustBeConstant(key)
        }
        (key, newValue)
    }
    ResolvedTableSpec(
      properties = u.properties,
      provider = u.provider,
      options = newOptions,
      location = u.location,
      comment = u.comment,
      serde = u.serde,
      external = u.external)
  }
}
