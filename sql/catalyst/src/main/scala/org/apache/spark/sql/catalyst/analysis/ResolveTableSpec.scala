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
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * This object is responsible for processing unresolved table specifications in commands with
 * OPTIONS lists. The parser produces such lists as maps from strings to unresolved expressions.
 * After otherwise resolving such expressions in the analyzer, here we convert them to resolved
 * table specifications wherein these OPTIONS list values are represented as strings instead, for
 * convenience.
 */
object ResolveTableSpec {
  def apply(u: UnresolvedTableSpec): ResolvedTableSpec = {
    val newOptions: Map[String, String] = u.optionsExpressions.map {
      case (key: String, null) =>
        (key, null)
      case (key: String, value: Expression) =>
        val newValue: String = try {
          constantFold(value) match {
            case lit: Literal =>
              lit.toString
            case _ =>
              throw QueryCompilationErrors.optionMustBeConstant(key)
          }
        } catch {
          case _: SparkThrowable | _: java.lang.RuntimeException =>
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

  /** Helper method to constant-fold expressions of TableSpec options. */
  private def constantFold(expression: Expression): Expression = {
    val logical = Project(Seq(Alias(expression, "col")()), OneRowRelation())
    val folded = ConstantFolding(logical)
    folded match {
      case Project(Seq(Alias(expression, _)), _) => expression
      // Note that we do not need to check if the pattern does not match because the constant
      // folding we have invoked will not change the operators and number of projection expressions
      // in the query plan.
    }
  }
}
