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

package org.apache.spark.sql.execution.externalUDF

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExprId,
  ExternalUserDefinedFunction, NamedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_UDF, WINDOW}

/**
 * Extracts external UDFs that are parents of window expressions from a [[Window]] operator.
 * The window expressions are evaluated by the [[Window]], and [[PlanExternalUDFs]] subsequently
 * converts the external UDFs in the new [[Project]] into evaluation nodes above it.
 */
private[sql] object ExtractExternalUDFFromWindow extends Rule[LogicalPlan] {

  private def containsExternalUDFOverWindowExpression(expression: Expression): Boolean = {
    expression.exists {
      case udf: ExternalUserDefinedFunction =>
        udf.exists(_.isInstanceOf[WindowExpression])
      case _ => false
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(
      _.containsAllPatterns(EXTERNAL_UDF, WINDOW)) {
      case window: Window
          if window.windowExpressions.exists(containsExternalUDFOverWindowExpression) =>
        val windowProjectExprIds = mutable.Set.empty[ExprId]
        val windowProjectList = mutable.ArrayBuffer.empty[NamedExpression]
        val externalUdfProjectList = window.windowExpressions.map { expression =>
          if (containsExternalUDFOverWindowExpression(expression)) {
            expression.transformDown {
              case windowExpression: WindowExpression =>
                val alias = Alias(windowExpression, s"w_${windowProjectList.size}")()
                windowProjectList += alias
                alias.toAttribute
            }.asInstanceOf[NamedExpression]
          } else {
            if (!windowProjectExprIds.contains(expression.exprId)) {
              windowProjectList += expression
              windowProjectExprIds += expression.exprId
            }
            expression.toAttribute
          }
        }
        Project(
          window.child.output ++ externalUdfProjectList,
          window.copy(windowExpressions = windowProjectList.toSeq))
    }
  }
}
