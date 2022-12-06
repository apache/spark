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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETER, TreePattern}
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * The expression represents a named parameter that should be replaces by a foldable expression.
 *
 * @param name The identifier of the parameter without the marker.
 */
case class Parameter(name: String) extends LeafExpression {
  override def dataType: DataType = NullType
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(PARAMETER)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw QueryExecutionErrors.unboundParameterError(name)
  }

  def eval(input: InternalRow): Any = {
    throw QueryExecutionErrors.unboundParameterError(name)
  }
}


/**
 * Finds all named parameters in the given plan and substitutes them by
 * foldable expressions of `args` values.
 */
object Parameter extends QueryErrorsBase {
  def bind(plan: LogicalPlan, args: Map[String, Expression]): LogicalPlan = {
    if (!args.isEmpty && SQLConf.get.parametersEnabled) {
      args.filter(!_._2.foldable).headOption.foreach { case (name, expr) =>
        expr.failAnalysis(
          errorClass = "NON_FOLDABLE_SQL_ARG",
          messageParameters = Map("name" -> name))
      }
      plan.transformAllExpressionsWithPruning(_.containsPattern(PARAMETER)) {
        case param @ Parameter(name) =>
          if (args.contains(name)) {
            args(name)
          } else {
            param.failAnalysis(
              errorClass = "UNBOUND_PARAMETER",
              messageParameters = Map("name" -> name))
          }
      }
    } else {
      plan
    }
  }
}
