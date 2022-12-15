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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETER, TreePattern}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.DataType

/**
 * The expression represents a named parameter that should be replaced by a literal.
 *
 * @param name The identifier of the parameter without the marker.
 */
case class Parameter(name: String) extends LeafExpression with Unevaluable {
  override lazy val resolved: Boolean = false

  private def unboundError(methodName: String): Nothing = {
    throw SparkException.internalError(
      s"Cannot call `$methodName()` of the unbound parameter `$name`.")
  }
  override def dataType: DataType = unboundError("dataType")
  override def nullable: Boolean = unboundError("nullable")

  final override val nodePatterns: Seq[TreePattern] = Seq(PARAMETER)
}


/**
 * Finds all named parameters in the given plan and substitutes them by literals of `args` values.
 */
object Parameter extends QueryErrorsBase {
  def bind(plan: LogicalPlan, args: Map[String, Expression]): LogicalPlan = {
    if (!args.isEmpty) {
      args.filter(!_._2.isInstanceOf[Literal]).headOption.foreach { case (name, expr) =>
        expr.failAnalysis(
          errorClass = "INVALID_SQL_ARG",
          messageParameters = Map("name" -> toSQLId(name)))
      }
      plan.transformAllExpressionsWithPruning(_.containsPattern(PARAMETER)) {
        case Parameter(name) if args.contains(name) => args(name)
      }
    } else {
      plan
    }
  }
}
