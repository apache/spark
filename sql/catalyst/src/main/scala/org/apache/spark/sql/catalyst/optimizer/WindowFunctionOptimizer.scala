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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentRow, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.{SpecifiedWindowFrame, UnboundedFollowing}
import org.apache.spark.sql.catalyst.expressions.{UnboundedPreceding, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.{WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, First, Last}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule

// Replace UnboundedFollowing with UnboundedPreceding for window function first and last.

object ReplaceUnboundedFollowingWithUnboundedPreceding extends Rule[LogicalPlan] {

  private def replacedFrameSpec(frameSpec: WindowFrame): WindowFrame = {
    frameSpec match {
      case SpecifiedWindowFrame(frame, CurrentRow, UnboundedFollowing) =>
        SpecifiedWindowFrame(frame, UnboundedPreceding, CurrentRow)
      case x => x
    }
  }

  private def reverseOrderSpec(orderSpec: Seq[SortOrder]): Seq[SortOrder] = {
    orderSpec.map(_.reverse)
  }

  private def reverseAggFunc(aggFunc: AggregateFunction): AggregateFunction = {
    aggFunc match {
      case First(fChild, ignoreNulls) => Last(fChild, ignoreNulls)
      case Last(lChild, ignoreNulls) => First(lChild, ignoreNulls)
      case others => others
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {

    def isApplicable(aggFunc: AggregateFunction, frameSpec: WindowFrame): Boolean = {
      val isAggFuncApplicable = aggFunc match {
        case _: First => true
        case _: Last => true
        case _ => false
      }

      val isFrameSpecApplicable = frameSpec match {
        case SpecifiedWindowFrame(_, CurrentRow, UnboundedFollowing) => true
        case _ => false
      }

      isAggFuncApplicable && isFrameSpecApplicable
    }

    plan.transform {
      case win: logical.Window =>

        val winExprs = win.windowExpressions
        val (candidates, others) = winExprs.partition {
          case Alias(w@ WindowExpression(aggExpr: AggregateExpression,
            wsd: WindowSpecDefinition), _) =>
            isApplicable(aggExpr.aggregateFunction, wsd.frameSpecification)
          case _ => false
        }

        val nWinExprs: Seq[NamedExpression] = candidates.map {
          case a@ Alias(w@ WindowExpression(aggExpr: AggregateExpression,
            wsd: WindowSpecDefinition), _) =>

            val nAggExpr = aggExpr.copy(
              aggregateFunction = reverseAggFunc(aggExpr.aggregateFunction))
            val reversedWsd = wsd.copy(
              orderSpec = reverseOrderSpec(wsd.orderSpec),
              frameSpecification = replacedFrameSpec(wsd.frameSpecification))
            val nWinExpr = w.copy(windowFunction = nAggExpr, windowSpec = reversedWsd)
            val nAlias = a.copy(child = nWinExpr)(exprId = a.exprId,
              qualifier = a.qualifier, explicitMetadata = a.explicitMetadata,
              nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)

            nAlias
        }

        if (nWinExprs.nonEmpty) {
          if (others.isEmpty) {
            win.copy(windowExpressions = nWinExprs, orderSpec = reverseOrderSpec(win.orderSpec))
          } else {
            val nWindow = win.copy(windowExpressions = others,
              child = win.copy(windowExpressions = nWinExprs,
                orderSpec = reverseOrderSpec(win.orderSpec), child = win.child))

            // Restore the original output column order
            val nOutput = win.output.flatMap { att =>
              val nAttr = nWindow.output.find(x => att.name == x.name)
              if (nAttr.nonEmpty) nAttr
              else Some(att)
            }
            Project(nOutput, child = nWindow)
          }
        } else {
          win
        }
    }
  }
}
