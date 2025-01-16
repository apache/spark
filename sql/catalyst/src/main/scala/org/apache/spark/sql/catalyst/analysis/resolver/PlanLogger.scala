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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys.{MESSAGE, QUERY_PLAN}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.internal.SQLConf

/**
 * [[PlanLogger]] is used by the [[Resolver]] to log intermediate resolution results.
 */
class PlanLogger extends Logging {
  private val planChangeLogLevel = SQLConf.get.planChangeLogLevel
  private val expressionTreeChangeLogLevel = SQLConf.get.expressionTreeChangeLogLevel

  def logPlanResolutionEvent(plan: LogicalPlan, event: String): Unit = {
    log(() => log"""
       |=== Plan resolution: ${MDC(MESSAGE, event)} ===
       |${MDC(QUERY_PLAN, plan.treeString)}
     """.stripMargin, planChangeLogLevel)
  }

  def logPlanResolution(unresolvedPlan: LogicalPlan, resolvedPlan: LogicalPlan): Unit = {
    log(
      () =>
        log"""
       |=== Unresolved plan -> Resolved plan ===
       |${MDC(
               QUERY_PLAN,
               sideBySide(
                 unresolvedPlan.treeString,
                 resolvedPlan.treeString
               ).mkString("\n")
             )}
     """.stripMargin,
      planChangeLogLevel
    )
  }

  def logExpressionTreeResolutionEvent(expressionTree: Expression, event: String): Unit = {
    log(
      () => log"""
       |=== Expression tree resolution: ${MDC(MESSAGE, event)} ===
       |${MDC(QUERY_PLAN, expressionTree.treeString)}
     """.stripMargin,
      expressionTreeChangeLogLevel
    )
  }

  def logExpressionTreeResolution(
      unresolvedExpressionTree: Expression,
      resolvedExpressionTree: Expression): Unit = {
    log(
      () =>
        log"""
       |=== Unresolved expression tree -> Resolved expression tree ===
       |${MDC(
               QUERY_PLAN,
               sideBySide(
                 unresolvedExpressionTree
                   .withNewChildren(resolvedExpressionTree.children)
                   .treeString,
                 resolvedExpressionTree.treeString
               ).mkString("\n")
             )}
     """.stripMargin,
      expressionTreeChangeLogLevel
    )
  }

  private def log(createMessage: () => MessageWithContext, logLevel: String): Unit =
    logLevel match {
      case "TRACE" => logTrace(createMessage().message)
      case "DEBUG" => logDebug(createMessage().message)
      case "INFO" => logInfo(createMessage())
      case "WARN" => logWarning(createMessage())
      case "ERROR" => logError(createMessage())
      case _ => logTrace(createMessage().message)
    }
}
